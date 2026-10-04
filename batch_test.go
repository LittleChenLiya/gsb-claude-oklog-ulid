// Copyright 2026 The Oklog Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ulid_test

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// testEntropy is a shared monotonic entropy source so successive calls with
// the same millisecond produce distinct ULIDs.
var (
	testEntropyMu sync.Mutex
	testEntropy   = ulid.Monotonic(rand.New(rand.NewSource(12345)), 0)
)

// newID returns a ULID with the given millisecond timestamp. Successive
// calls with the same millisecond produce distinct ULIDs.
func newID(t *testing.T, ms uint64) ulid.ULID {
	t.Helper()
	testEntropyMu.Lock()
	defer testEntropyMu.Unlock()
	id, err := ulid.New(ms, testEntropy)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// validate feeds records to a fresh validator and returns the results.
func validate(records []string, window int) []ulid.BatchResult {
	v := ulid.NewBatchValidator(window)
	results := make([]ulid.BatchResult, len(records))
	for i, record := range records {
		results[i] = v.Validate(record)
	}
	return results
}

// TestBatchValidatorAcceptance covers the core import scenario: two
// distinct ULIDs in the same millisecond pass, an earlier record reports a
// regression, a repeat reports a duplicate, and a syntactically invalid
// record sandwiched in the middle leaves the time baseline unchanged.
func TestBatchValidatorAcceptance(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	first := newID(t, ms)
	second := newID(t, ms) // same millisecond, different entropy
	earlier := newID(t, ms-1)

	records := []string{
		first.String(),   // 0: ok
		second.String(),  // 1: ok, same millisecond
		"not-a-ulid",     // 2: syntax error (wrong size)
		earlier.String(), // 3: regression; baseline unchanged by record 2
		first.String(),   // 4: duplicate
	}

	want := []error{nil, nil, ulid.ErrDataSize, ulid.ErrBatchRegression, ulid.ErrBatchDuplicate}
	results := validate(records, 10)

	for i, res := range results {
		if res.Index != i {
			t.Errorf("record %d: got index %d", i, res.Index)
		}
		if !errors.Is(res.Err, want[i]) {
			t.Errorf("record %d: got err %v, want %v", i, res.Err, want[i])
		}
	}
}

// TestBatchValidatorInvalidCharacters ensures a 26-character record with an
// invalid base32 character is reported as a syntax error and does not
// advance the time baseline.
func TestBatchValidatorInvalidCharacters(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	first := newID(t, ms)
	sameMS := newID(t, ms)

	v := ulid.NewBatchValidator(10)
	v.Validate(first.String())

	res := v.Validate("0000000000000000000000000!")
	if !errors.Is(res.Err, ulid.ErrInvalidCharacters) {
		t.Fatalf("got err %v, want ErrInvalidCharacters", res.Err)
	}
	if res.ID != (ulid.ULID{}) {
		t.Errorf("syntax error produced non-zero ID: %v", res.ID)
	}

	// The invalid record must not advance the baseline: a record in the
	// same millisecond as the first is still valid.
	res = v.Validate(sameMS.String())
	if res.Err != nil {
		t.Fatalf("same-ms record after invalid record got err %v", res.Err)
	}
}

// TestBatchValidatorOverflow ensures a record whose first character exceeds
// the valid base32 range is reported as ErrOverflow and does not advance
// the time baseline.
func TestBatchValidatorOverflow(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	first := newID(t, ms)
	sameMS := newID(t, ms)

	v := ulid.NewBatchValidator(10)
	v.Validate(first.String())

	res := v.Validate("81ARZ3NDEKTSV4RRFFQ69G5FAV")
	if !errors.Is(res.Err, ulid.ErrOverflow) {
		t.Fatalf("got err %v, want ErrOverflow", res.Err)
	}

	// The overflow record must not advance the baseline.
	res = v.Validate(sameMS.String())
	if res.Err != nil {
		t.Fatalf("same-ms record after overflow got err %v", res.Err)
	}
}

// TestBatchValidatorWindowBoundary verifies the finite window semantics.
// Records that age out of the window are no longer remembered, so
// re-importing them is not reported as a duplicate — which must not be read
// as a global uniqueness guarantee. Records still inside the window are
// reported as duplicates.
func TestBatchValidatorWindowBoundary(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	id1 := newID(t, ms)
	id2 := newID(t, ms)
	id3 := newID(t, ms)

	v := ulid.NewBatchValidator(2) // remember only the 2 most recent valid records

	check := func(rec string, want error) {
		t.Helper()
		res := v.Validate(rec)
		if !errors.Is(res.Err, want) {
			t.Errorf("got err %v, want %v", res.Err, want)
		}
	}

	check(id1.String(), nil)
	check(id2.String(), nil)
	check(id3.String(), nil) // evicts id1; window is now {id2, id3}

	// id1 has aged out of the window: it is not reported as a duplicate.
	// This is only a within-window guarantee, not global uniqueness.
	check(id1.String(), nil) // evicts id2; window is now {id3, id1}

	// id3 is still retained: duplicate.
	check(id3.String(), ulid.ErrBatchDuplicate)
}

// TestBatchValidatorCaseNormalization verifies that upper- and lower-case
// encodings of the same ULID compare equal for duplicate detection, per the
// existing parsing rules.
func TestBatchValidatorCaseNormalization(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	id := newID(t, ms)

	v := ulid.NewBatchValidator(10)
	v.Validate(id.String())

	res := v.Validate(strings.ToLower(id.String()))
	if !errors.Is(res.Err, ulid.ErrBatchDuplicate) {
		t.Fatalf("lowercase duplicate got err %v, want ErrBatchDuplicate", res.Err)
	}
}

// TestBatchValidatorInvalidRecordsDoNotAdvance verifies that a record which
// fails validation does not advance the time baseline or enter the
// duplicate-detection window.
func TestBatchValidatorInvalidRecordsDoNotAdvance(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	first := newID(t, ms)
	earlier := newID(t, ms-1)
	sameMS := newID(t, ms)
	later := newID(t, ms+1)

	v := ulid.NewBatchValidator(10)
	v.Validate(first.String())

	// A regression does not advance the baseline.
	if res := v.Validate(earlier.String()); !errors.Is(res.Err, ulid.ErrBatchRegression) {
		t.Fatalf("got err %v, want regression", res.Err)
	}

	// A record in the same millisecond as the first is still valid.
	if res := v.Validate(sameMS.String()); res.Err != nil {
		t.Fatalf("same-ms record got err %v", res.Err)
	}

	// A record one millisecond later is valid.
	if res := v.Validate(later.String()); res.Err != nil {
		t.Fatalf("later record got err %v", res.Err)
	}
}

// TestBatchValidatorUnlimitedWindow verifies that a window of 0 remembers
// every valid record (exact duplicate detection).
func TestBatchValidatorUnlimitedWindow(t *testing.T) {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	first := newID(t, ms)
	second := newID(t, ms)

	v := ulid.NewBatchValidator(0)
	v.Validate(first.String())
	v.Validate(second.String())

	if res := v.Validate(first.String()); !errors.Is(res.Err, ulid.ErrBatchDuplicate) {
		t.Fatalf("got err %v, want duplicate", res.Err)
	}
}

// ExampleBatchValidator demonstrates streaming validation of a small input
// stream. Records are read one at a time and validated immediately; the
// batch is never buffered.
func ExampleBatchValidator() {
	ms := ulid.Timestamp(time.Unix(1600000000, 0))
	entropy := ulid.Monotonic(rand.New(rand.NewSource(42)), 0)
	first := ulid.MustNew(ms, entropy)
	second := ulid.MustNew(ms, entropy)

	stream := strings.NewReader(strings.Join([]string{
		first.String(),
		second.String(),
		first.String(), // duplicate of the first
		"not-a-ulid",   // syntax error
	}, "\n") + "\n")

	v := ulid.NewBatchValidator(10)
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		result := v.Validate(scanner.Text())
		switch {
		case result.Err == nil:
			fmt.Printf("record %d: ok\n", result.Index)
		case errors.Is(result.Err, ulid.ErrBatchDuplicate):
			fmt.Printf("record %d: duplicate (within window)\n", result.Index)
		case errors.Is(result.Err, ulid.ErrBatchRegression):
			fmt.Printf("record %d: time regression\n", result.Index)
		default:
			fmt.Printf("record %d: syntax error: %v\n", result.Index, result.Err)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("stream error:", err)
	}
	// Output:
	// record 0: ok
	// record 1: ok
	// record 2: duplicate (within window)
	// record 3: syntax error: ulid: bad data size when unmarshaling
}
