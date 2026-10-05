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
	"bytes"
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/oklog/ulid/v2"
)

// countingReader wraps an io.Reader and counts the number of bytes read.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// flakyReader returns a short read (5 bytes + EOF) on its first Read and a
// full read (10 bytes) on every subsequent Read.
type flakyReader struct {
	short []byte
	full  []byte
	done  bool
}

func (r *flakyReader) Read(p []byte) (int, error) {
	if !r.done {
		r.done = true
		n := copy(p, r.short)
		return n, io.EOF
	}
	n := copy(p, r.full)
	return n, nil
}

func TestSessionGenerate(t *testing.T) {
	t.Parallel()

	r := bytes.NewReader(bytes.Repeat([]byte{0x11}, 40))
	s := ulid.NewSession(r)

	first, firstRcpt, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if firstRcpt.Stage != ulid.StageEntropy {
		t.Errorf("first receipt stage: got %v, want %v", firstRcpt.Stage, ulid.StageEntropy)
	}
	if firstRcpt.Count != 10 {
		t.Errorf("first receipt count: got %d, want 10", firstRcpt.Count)
	}
	if got, want := first.Entropy(), bytes.Repeat([]byte{0x11}, 10); !bytes.Equal(got, want) {
		t.Errorf("first entropy: got %x, want %x", got, want)
	}

	second, secondRcpt, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if secondRcpt.Stage != ulid.StageMonotonic {
		t.Errorf("second receipt stage: got %v, want %v", secondRcpt.Stage, ulid.StageMonotonic)
	}
	if second.Compare(first) <= 0 {
		t.Errorf("same-millisecond ULIDs not increasing: %s >= %s", first, second)
	}

	third, thirdRcpt, err := s.Generate(1001)
	if err != nil {
		t.Fatal(err)
	}
	if thirdRcpt.Stage != ulid.StageEntropy {
		t.Errorf("third receipt stage: got %v, want %v", thirdRcpt.Stage, ulid.StageEntropy)
	}
	if third.Time() != 1001 {
		t.Errorf("third timestamp: got %d, want 1001", third.Time())
	}
}

func TestSessionShortReadDoesNotAdvanceState(t *testing.T) {
	t.Parallel()

	// The first read yields only 5 bytes before EOF: a short read.
	// Subsequent reads yield the full 10 entropy bytes.
	r := &countingReader{r: &flakyReader{
		short: bytes.Repeat([]byte{0xAA}, 5),
		full:  bytes.Repeat([]byte{0xBB}, 10),
	}}
	s := ulid.NewSessionWithInc(r, 1)

	first, firstRcpt, err := s.Generate(1000)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("first generate: got err %v, want %v", err, io.ErrUnexpectedEOF)
	}
	if firstRcpt.Stage != ulid.StageEntropy {
		t.Errorf("first receipt stage: got %v, want %v", firstRcpt.Stage, ulid.StageEntropy)
	}
	if firstRcpt.Count != 5 {
		t.Errorf("first receipt count: got %d, want 5", firstRcpt.Count)
	}
	if r.n != 5 {
		t.Errorf("counting reader: got %d bytes, want 5", r.n)
	}

	// A failed generate must not advance the session state: the next
	// generate reads fresh entropy instead of incrementing.
	second, secondRcpt, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if secondRcpt.Stage != ulid.StageEntropy {
		t.Errorf("second receipt stage: got %v, want %v", secondRcpt.Stage, ulid.StageEntropy)
	}
	if secondRcpt.Count != 10 {
		t.Errorf("second receipt count: got %d, want 10", secondRcpt.Count)
	}
	if r.n != 15 {
		t.Errorf("counting reader: got %d bytes, want 15", r.n)
	}
	if got, want := second.Entropy(), bytes.Repeat([]byte{0xBB}, 10); !bytes.Equal(got, want) {
		t.Errorf("second entropy: got %x, want %x (state was advanced by the short read)", got, want)
	}

	// The fresh state then increments monotonically within the millisecond.
	third, _, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if third.Compare(second) <= 0 {
		t.Errorf("same-millisecond ULIDs not increasing: %s >= %s", second, third)
	}
	if r.n != 15 {
		t.Errorf("increment must not read with inc=1: got %d bytes, want 15", r.n)
	}
	_ = first
}

func TestSessionMonotonicOverflow(t *testing.T) {
	t.Parallel()

	r := io.MultiReader(
		bytes.NewReader(bytes.Repeat([]byte{0xFF}, 10)), // max entropy for first ULID
		crand.Reader, // random bytes for the increment
	)
	s := ulid.NewSession(r)

	_, firstRcpt, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if firstRcpt.Stage != ulid.StageEntropy || firstRcpt.Count != 10 {
		t.Fatalf("first receipt: stage=%v count=%d", firstRcpt.Stage, firstRcpt.Count)
	}

	second, secondRcpt, err := s.Generate(1000)
	if err != ulid.ErrMonotonicOverflow {
		t.Fatalf("second generate: got err %v, want %v", err, ulid.ErrMonotonicOverflow)
	}
	if secondRcpt.Stage != ulid.StageMonotonic {
		t.Errorf("second receipt stage: got %v, want %v", secondRcpt.Stage, ulid.StageMonotonic)
	}

	// Overflow must not advance state: retrying rejects again instead of
	// silently wrapping.
	third, _, err := s.Generate(1000)
	if err != ulid.ErrMonotonicOverflow {
		t.Fatalf("third generate after overflow: got err %v, want %v", err, ulid.ErrMonotonicOverflow)
	}
	if third.Compare(second) != 0 {
		t.Errorf("overflow advanced state: %s != %s", second, third)
	}
}

func TestSessionClockFailure(t *testing.T) {
	t.Parallel()

	r := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte{0xCC}, 20))}
	s := ulid.NewSession(r)

	_, rcpt, err := s.Generate(ulid.MaxTime() + 1)
	if err != ulid.ErrBigTime {
		t.Fatalf("got err %v, want %v", err, ulid.ErrBigTime)
	}
	if rcpt.Stage != ulid.StageClock {
		t.Errorf("receipt stage: got %v, want %v", rcpt.Stage, ulid.StageClock)
	}
	if rcpt.Count != 0 {
		t.Errorf("receipt count: got %d, want 0", rcpt.Count)
	}
	if r.n != 0 {
		t.Errorf("counting reader: got %d bytes, want 0 (clock failure must not read)", r.n)
	}

	// State is untouched: a valid generate still reads fresh entropy.
	id, rcpt, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if rcpt.Stage != ulid.StageEntropy || rcpt.Count != 10 {
		t.Errorf("receipt: stage=%v count=%d", rcpt.Stage, rcpt.Count)
	}
	if id.Time() != 1000 {
		t.Errorf("timestamp: got %d, want 1000", id.Time())
	}
}

func TestSessionClosed(t *testing.T) {
	t.Parallel()

	s := ulid.NewSession(bytes.NewReader(bytes.Repeat([]byte{0xDD}, 20)))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil { // closing twice is harmless
		t.Fatal(err)
	}

	_, rcpt, err := s.Generate(1000)
	if err != ulid.ErrSessionClosed {
		t.Fatalf("got err %v, want %v", err, ulid.ErrSessionClosed)
	}
	if rcpt != (ulid.Receipt{}) {
		t.Errorf("receipt after close: got %+v, want zero", rcpt)
	}
}

func TestSessionReadCount(t *testing.T) {
	t.Parallel()

	r := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte{0xEE}, 64))}
	s := ulid.NewSession(r)

	check := func(ms uint64, wantStage ulid.Stage) ulid.ULID {
		t.Helper()
		before := r.n
		id, rcpt, err := s.Generate(ms)
		if err != nil {
			t.Fatalf("generate(%d): %v", ms, err)
		}
		if rcpt.Stage != wantStage {
			t.Errorf("generate(%d) stage: got %v, want %v", ms, rcpt.Stage, wantStage)
		}
		if got, want := rcpt.Count, r.n-before; got != want {
			t.Errorf("generate(%d) count: receipt=%d, reader delta=%d", ms, got, want)
		}
		return id
	}

	first := check(1000, ulid.StageEntropy)
	second := check(1000, ulid.StageMonotonic)
	if second.Compare(first) <= 0 {
		t.Errorf("same-millisecond ULIDs not increasing: %s >= %s", first, second)
	}

	// A new millisecond reads exactly the 10 needed bytes, no prefetching.
	before := r.n
	third := check(1001, ulid.StageEntropy)
	if got, want := r.n-before, 10; got != want {
		t.Errorf("new-millisecond read: got %d bytes, want %d", got, want)
	}
	if third.Time() != 1001 {
		t.Errorf("timestamp: got %d, want 1001", third.Time())
	}
}

func TestSessionIncOneReadsNothing(t *testing.T) {
	t.Parallel()

	r := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte{0x77}, 20))}
	s := ulid.NewSessionWithInc(r, 1)

	first, _, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if r.n != 10 {
		t.Fatalf("first read: got %d bytes, want 10", r.n)
	}

	second, rcpt, err := s.Generate(1000)
	if err != nil {
		t.Fatal(err)
	}
	if rcpt.Stage != ulid.StageMonotonic || rcpt.Count != 0 {
		t.Errorf("receipt: stage=%v count=%d, want monotonic/0", rcpt.Stage, rcpt.Count)
	}
	if r.n != 10 {
		t.Errorf("increment must not read with inc=1: got %d bytes, want 10", r.n)
	}
	if second.Compare(first) <= 0 {
		t.Errorf("same-millisecond ULIDs not increasing: %s >= %s", first, second)
	}
}

func ExampleSession_Generate() {
	// A session is an injectable, monotonic generation scope.
	entropy := rand.New(rand.NewSource(20261005))
	session := ulid.NewSessionWithInc(entropy, 1)

	// Step 1: generate the first ULID in a millisecond (fresh entropy read).
	first, firstRcpt, err := session.Generate(1469918176385)
	if err != nil {
		panic(err)
	}

	// Step 2: generate again in the same millisecond (monotonic increment).
	second, secondRcpt, err := session.Generate(1469918176385)
	if err != nil {
		panic(err)
	}

	// Step 3: generate in the next millisecond (fresh entropy read).
	third, thirdRcpt, err := session.Generate(1469918176386)
	if err != nil {
		panic(err)
	}

	fmt.Println(first, firstRcpt.Stage, firstRcpt.Count)
	fmt.Println(second, secondRcpt.Stage, secondRcpt.Count)
	fmt.Println(third, thirdRcpt.Stage, thirdRcpt.Count)
	// Output:
	// 01ARYZ6S41AQ3DB0DEJAFX7YDG entropy 10
	// 01ARYZ6S41AQ3DB0DEJAFX7YDH monotonic 0
	// 01ARYZ6S42HBS80KTW2QY38Y1N entropy 10
}
