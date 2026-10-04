// Copyright 2016 The Oklog Authors
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
	"errors"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
)

// mustMakeID builds a deterministic ULID from a millisecond timestamp and a
// single entropy byte placed at the start of the random component.
func mustMakeID(ms uint64, entropy byte) ulid.ULID {
	var id ulid.ULID
	if err := id.SetTime(ms); err != nil {
		panic(err)
	}
	id[6] = entropy
	return id
}

// TestBatchValidatorAcceptance exercises the required acceptance scenario:
// two different ULIDs in the same millisecond both pass, the next older
// record is reported as time-reversed, repeating an earlier record is
// reported as duplicate, and an invalid record in between does not change the
// time baseline.
func TestBatchValidatorAcceptance(t *testing.T) {
	var (
		v          = ulid.NewBatchValidator(0)
		first      = mustMakeID(100, 1) // 00000000XEG...
		second     = mustMakeID(100, 2)
		older      = mustMakeID(99, 3)
		duplicate  = first
		invalid    = "not-a-ulid!!"
		afterwards = mustMakeID(100, 4)
	)

	records := []string{
		first.String(),
		second.String(),
		older.String(),
		duplicate.String(),
		invalid,
		afterwards.String(),
	}
	want := []ulid.ErrorCategory{
		ulid.CategoryUnknown, // accepted
		ulid.CategoryUnknown, // accepted (same ms, different randomness)
		ulid.CategoryTimeReversed,
		ulid.CategoryDuplicate,
		ulid.CategoryInvalid,
		ulid.CategoryUnknown, // still baseline 100: same ms is not a reversal
	}

	for i, rec := range records {
		_, rerr := v.Validate(i, rec)
		if want[i] == ulid.CategoryUnknown {
			if rerr != nil {
				t.Fatalf("record %d unexpectedly rejected: %v", i, rerr)
			}
			continue
		}
		if rerr == nil {
			t.Fatalf("record %d unexpectedly accepted", i)
		}
		if rerr.Index != i {
			t.Errorf("record %d error has index %d", i, rerr.Index)
		}
		if rerr.Record != rec {
			t.Errorf("record %d error lost original text: %q", i, rerr.Record)
		}
		if rerr.Category != want[i] {
			t.Errorf("record %d category = %s, want %s", i, rerr.Category, want[i])
		}
	}

	// The invalid record must not have advanced the time baseline.
	last, ok := v.LastTime()
	if !ok || last != 100 {
		t.Fatalf("last accepted time = %d (ok=%v), want 100", last, ok)
	}
}

// TestBatchValidatorCaseNormalization verifies that case follows the
// existing parse rules: lower-case input equals its upper-case form, and a
// repeat in a different case is still a duplicate.
func TestBatchValidatorCaseNormalization(t *testing.T) {
	v := ulid.NewBatchValidator(0)
	id := mustMakeID(1000, 0xAA)

	if _, rerr := v.Validate(0, strings.ToLower(id.String())); rerr != nil {
		t.Fatalf("lowercase record rejected: %v", rerr)
	}
	if _, rerr := v.Validate(1, id.String()); rerr == nil || rerr.Category != ulid.CategoryDuplicate {
		t.Fatalf("upper/lower case repeat not detected as duplicate: %v", rerr)
	}
}

// TestBatchValidatorInvalidDoesNotAdvanceBaseline inserts invalid,
// time-reversed and duplicate records between valid ones and confirms none of
// them move the baseline or enter the window.
func TestBatchValidatorInvalidDoesNotAdvanceBaseline(t *testing.T) {
	v := ulid.NewBatchValidator(0)

	if _, rerr := v.Validate(0, mustMakeID(50, 1).String()); rerr != nil {
		t.Fatal(rerr)
	}

	// Bad syntax ('I' is not part of the base32 alphabet, 26 chars).
	if _, rerr := v.Validate(1, "00000000I00000000000000000000"); rerr == nil || rerr.Category != ulid.CategoryInvalid {
		t.Fatalf("want invalid, got %v", rerr)
	}
	// A record at the same baseline timestamp still passes; a reversal would
	// have meant the baseline moved.
	if _, rerr := v.Validate(2, mustMakeID(50, 2).String()); rerr != nil {
		t.Fatalf("baseline changed after invalid record: %v", rerr)
	}
	// Reversed record is rejected and does not lower the baseline.
	if _, rerr := v.Validate(3, mustMakeID(10, 9).String()); rerr == nil || rerr.Category != ulid.CategoryTimeReversed {
		t.Fatalf("want time-reversed, got %v", rerr)
	}
	if _, rerr := v.Validate(4, mustMakeID(50, 3).String()); rerr != nil {
		t.Fatalf("baseline changed after reversed record: %v", rerr)
	}
	// Duplicate record is rejected and does not enter the window.
	if _, rerr := v.Validate(5, mustMakeID(50, 1).String()); rerr == nil || rerr.Category != ulid.CategoryDuplicate {
		t.Fatalf("want duplicate, got %v", rerr)
	}
	if _, rerr := v.Validate(6, mustMakeID(50, 4).String()); rerr != nil {
		t.Fatalf("baseline changed after duplicate record: %v", rerr)
	}

	if last, ok := v.LastTime(); !ok || last != 50 {
		t.Fatalf("last accepted time = %d (ok=%v), want 50", last, ok)
	}
}

// TestBatchValidatorWindowBoundary verifies that exactly windowSize accepted
// records are tracked: the oldest record is forgotten once the window fills,
// and repeating it is accepted (not falsely claimed as a duplicate), while a
// repeat still inside the window is reported.
func TestBatchValidatorWindowBoundary(t *testing.T) {
	const w = 2
	v := ulid.NewBatchValidator(w)
	if cap, bounded := v.WindowSize(); !bounded || cap != w {
		t.Fatalf("window = (%d,%v), want (%d,true)", cap, bounded, w)
	}
	if !strings.Contains(v.UniquenessScope(), "only") {
		t.Fatalf("finite window scope must be partial: %q", v.UniquenessScope())
	}

	// Same millisecond keeps ordering non-reversed; entropy distinguishes IDs.
	a := mustMakeID(10, 1)
	b := mustMakeID(10, 2)
	c := mustMakeID(10, 3)

	if _, rerr := v.Validate(0, a.String()); rerr != nil {
		t.Fatal(rerr)
	}
	if _, rerr := v.Validate(1, b.String()); rerr != nil {
		t.Fatal(rerr)
	}
	// c pushes a out of the size-2 window, which is now [b, c]. Repeating a
	// must pass: the validator must not claim global uniqueness for a finite
	// window.
	if _, rerr := v.Validate(2, c.String()); rerr != nil {
		t.Fatal(rerr)
	}
	if _, rerr := v.Validate(3, a.String()); rerr != nil {
		t.Fatalf("expired window record wrongly rejected: %v", rerr)
	}
	// The window is now [c, a]; c is still inside it and must be detected as
	// a duplicate.
	if _, rerr := v.Validate(4, c.String()); rerr == nil || rerr.Category != ulid.CategoryDuplicate {
		t.Fatalf("in-window repeat not detected: %v", rerr)
	}
	// After accepting c again the window is [a, c]; b has expired and a
	// repeat of b must pass.
	if _, rerr := v.Validate(5, b.String()); rerr != nil {
		t.Fatalf("expired window record wrongly rejected: %v", rerr)
	}
}

// TestBatchValidatorWindowSizeOne checks the smallest finite window.
func TestBatchValidatorWindowSizeOne(t *testing.T) {
	v := ulid.NewBatchValidator(1)
	a := mustMakeID(1, 1)
	b := mustMakeID(1, 2)

	if _, rerr := v.Validate(0, a.String()); rerr != nil {
		t.Fatal(rerr)
	}
	if _, rerr := v.Validate(1, a.String()); rerr == nil || rerr.Category != ulid.CategoryDuplicate {
		t.Fatalf("size-1 window failed to catch repeat: %v", rerr)
	}
	if _, rerr := v.Validate(2, b.String()); rerr != nil {
		t.Fatal(rerr)
	}
	// a has left the size-1 window.
	if _, rerr := v.Validate(3, a.String()); rerr != nil {
		t.Fatalf("expired size-1 record rejected: %v", rerr)
	}
}

// TestBatchValidatorInvalidKinds covers wrong length, invalid characters and
// overflow strings, including the first character > '7' overflow case.
func TestBatchValidatorInvalidKinds(t *testing.T) {
	valid := mustMakeID(1, 1).String()
	cases := []struct {
		rec      string
		parseErr error
	}{
		{"", ulid.ErrDataSize},                                             // empty / wrong length
		{"short", ulid.ErrDataSize},                                        // wrong length
		{strings.Repeat("0", 25) + "I", ulid.ErrInvalidCharacters},         // 'I' is not in base32
		{strings.Repeat("0", 25) + "U", ulid.ErrInvalidCharacters},         // 'U' is not in base32
		{strings.Repeat("!", ulid.EncodedSize), ulid.ErrInvalidCharacters}, // non-alphabet characters
		{strings.Repeat("Z", ulid.EncodedSize), ulid.ErrOverflow},          // first char 'Z' overflows
		{strings.Repeat("8", ulid.EncodedSize), ulid.ErrOverflow},          // first char '8' overflows 128 bits
		{"7" + strings.Repeat("Z", ulid.EncodedSize-1), nil},               // max valid ULID boundary
	}

	for i, tc := range cases {
		v := ulid.NewBatchValidator(0)
		_, rerr := v.Validate(0, tc.rec)
		if tc.parseErr == nil {
			if rerr != nil {
				t.Errorf("case %d %q unexpectedly rejected: %v", i, tc.rec, rerr)
			}
			continue
		}
		if rerr == nil {
			t.Errorf("case %d %q unexpectedly accepted", i, tc.rec)
			continue
		}
		if rerr.Category != ulid.CategoryInvalid {
			t.Errorf("case %d %q category = %s, want invalid", i, tc.rec, rerr.Category)
		}
		if !errors.Is(rerr, ulid.ErrInvalidRecord) {
			t.Errorf("case %d %q does not unwrap to ErrInvalidRecord", i, tc.rec)
		}
		if !errors.Is(rerr, tc.parseErr) {
			t.Errorf("case %d %q ParseErr = %v, want %v", i, tc.rec, rerr.ParseErr, tc.parseErr)
		}
	}

	// Sanity: a normal valid record still parses on the same path.
	if _, rerr := ulid.NewBatchValidator(0).Validate(0, valid); rerr != nil {
		t.Fatalf("valid record rejected: %v", rerr)
	}
}

// TestRecordErrorUnwrap verifies errors.Is classification per category.
func TestRecordErrorUnwrap(t *testing.T) {
	cases := []struct {
		rec  string
		want error
	}{
		{"bad", ulid.ErrInvalidRecord},
		{mustMakeID(1, 1).String(), ulid.ErrTimeReversed}, // after baseline: reversed
	}
	v := ulid.NewBatchValidator(0)
	if _, rerr := v.Validate(0, mustMakeID(10, 1).String()); rerr != nil {
		t.Fatal(rerr)
	}
	// The first case is at index 1; the valid baseline is index 0.
	if _, rerr := v.Validate(1, cases[0].rec); !errors.Is(rerr, ulid.ErrInvalidRecord) {
		t.Fatalf("invalid errors.Is failed: %v", rerr)
	}
	if _, rerr := v.Validate(2, cases[1].rec); !errors.Is(rerr, ulid.ErrTimeReversed) {
		t.Fatalf("reversed errors.Is failed: %v", rerr)
	}
	if _, rerr := v.Validate(3, mustMakeID(10, 1).String()); !errors.Is(rerr, ulid.ErrDuplicate) {
		t.Fatalf("duplicate errors.Is failed: %v", rerr)
	}

	if got := ulid.CategoryInvalid.String(); got != "invalid" {
		t.Errorf("category string = %q", got)
	}
}

// TestBatchValidatorNegativeWindowPanics ensures the constructor contract.
func TestBatchValidatorNegativeWindowPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("negative window did not panic")
		}
	}()
	_ = ulid.NewBatchValidator(-1)
}

// TestBatchValidatorStream drives the reader-based entry point and checks the
// report preserves original indices and states a finite-window scope.
func TestBatchValidatorStream(t *testing.T) {
	var b strings.Builder
	b.WriteString(mustMakeID(100, 1).String() + "\r\n") // ms 100 (valid)
	b.WriteString(mustMakeID(100, 2).String() + "\r\n") // same ms, different entropy (valid)
	b.WriteString(mustMakeID(99, 3).String() + "\r\n")  // ms 99 (reversed)
	b.WriteString(mustMakeID(100, 1).String() + "\r\n") // duplicate of first
	b.WriteString("not-an-ulid\r\n")                    // invalid; baseline unchanged
	input := b.String()

	// Window 4 keeps every accepted record, so the duplicate is still caught,
	// while the report stays explicitly bounded.
	v := ulid.NewBatchValidator(4)
	report, err := v.ValidateReader(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 5 || report.Accepted != 2 || report.Rejected != 3 {
		t.Fatalf("counts total=%d accepted=%d rejected=%d", report.Total, report.Accepted, report.Rejected)
	}
	wantCats := []ulid.ErrorCategory{
		ulid.CategoryTimeReversed,
		ulid.CategoryDuplicate,
		ulid.CategoryInvalid,
	}
	for i, rerr := range report.Errors {
		if rerr.Category != wantCats[i] {
			t.Errorf("error %d category = %s, want %s", i, rerr.Category, wantCats[i])
		}
	}
	if !report.Bounded() || report.WindowCapacity != 4 {
		t.Fatalf("report window = (%d,%v)", report.WindowCapacity, report.Bounded())
	}
	if !strings.Contains(report.UniquenessScope(), "only") {
		t.Fatalf("bounded report must not claim global uniqueness: %q", report.UniquenessScope())
	}

	// Unbounded report states full uniqueness.
	unbounded := (&ulid.StreamReport{}).UniquenessScope()
	if strings.Contains(unbounded, "only") {
		t.Fatalf("unbounded scope must not read as partial: %q", unbounded)
	}
}

// TestBatchValidatorFiniteWindowRepeatsAccepted proves an expired record is
// never falsely reported as a duplicate (i.e. globally unique claim).
func TestBatchValidatorFiniteWindowRepeatsAccepted(t *testing.T) {
	v := ulid.NewBatchValidator(2)
	recs := []ulid.ULID{
		mustMakeID(1, 1), mustMakeID(1, 2), mustMakeID(1, 3),
		mustMakeID(1, 1), mustMakeID(1, 2), mustMakeID(1, 3),
	}
	for i, id := range recs {
		if _, rerr := v.Validate(i, id.String()); rerr != nil {
			t.Fatalf("record %d (%s) rejected with window 2: %v", i, id, rerr)
		}
	}
}
