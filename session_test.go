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
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/oklog/ulid/v2"
)

// countingReader wraps an io.Reader and counts read calls and bytes, proving
// exactly when and how much entropy a Session consumes.
type countingReader struct {
	r     io.Reader
	reads int
	bytes int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.reads++
	c.bytes += n
	return n, err
}

// shortThenFullReader short-reads once (failing the first io.ReadFull) and
// then serves bytes from the underlying reader normally.
type shortThenFullReader struct {
	r       io.Reader
	failed  bool
	shortN  int
	readCnt int
}

func (s *shortThenFullReader) Read(p []byte) (int, error) {
	s.readCnt++
	if !s.failed {
		s.failed = true
		n := s.shortN
		if n > len(p) {
			n = len(p)
		}
		for i := 0; i < n; i++ {
			p[i] = byte(i + 1)
		}
		return n, io.EOF // short read: io.ReadFull sees ErrUnexpectedEOF
	}
	return s.r.Read(p)
}

func fixedClock(ms uint64) ulid.ClockFunc {
	return func() (uint64, error) { return ms, nil }
}

// TestSessionNoPrefetch proves NewSession itself performs no entropy reads.
func TestSessionNoPrefetch(t *testing.T) {
	t.Parallel()

	cr := &countingReader{r: bytes.NewReader(make([]byte, 64))}
	_ = ulid.NewSession(fixedClock(123), cr, 0)

	if cr.reads != 0 || cr.bytes != 0 {
		t.Fatalf("NewSession prefetched entropy: reads=%d bytes=%d, want 0/0", cr.reads, cr.bytes)
	}
}

// TestSessionShortReadThenSuccess verifies that a first entropy short read
// fails with ErrEntropyRead without advancing state, and that the second
// generation succeeds as a fresh read with count 1 (no state jump).
func TestSessionShortReadThenSuccess(t *testing.T) {
	t.Parallel()

	const ms = 1_700_000_000_000
	src := &shortThenFullReader{
		r:      bytes.NewReader(bytes.Repeat([]byte{0xAB}, 64)),
		shortN: 3,
	}
	cr := &countingReader{r: src}
	s := ulid.NewSession(fixedClock(ms), cr, 0)

	// First generation: entropy short read must fail.
	id, receipt, err := s.Generate()
	if !errors.Is(err, ulid.ErrEntropyRead) {
		t.Fatalf("first Generate err = %v, want ErrEntropyRead", err)
	}
	if id != ulid.Zero {
		t.Fatalf("first Generate id = %v, want zero ULID", id)
	}
	if receipt != (ulid.Receipt{}) {
		t.Fatalf("first Generate receipt = %v, want zero Receipt", receipt)
	}

	// Second generation: succeeds without any state jump.
	id, receipt, err = s.Generate()
	if err != nil {
		t.Fatalf("second Generate err = %v, want nil", err)
	}
	if receipt.Stage != ulid.StageFresh {
		t.Fatalf("second Generate stage = %v, want StageFresh (failed attempt must not commit state)", receipt.Stage)
	}
	if receipt.Count != 1 {
		t.Fatalf("second Generate count = %d, want 1 (failed attempt must not be counted)", receipt.Count)
	}
	if id.Time() != ms {
		t.Fatalf("second Generate time = %d, want %d", id.Time(), ms)
	}
	if !bytes.Equal(id.Entropy(), bytes.Repeat([]byte{0xAB}, 10)) {
		t.Fatalf("second Generate entropy = %x, want 10 x 0xAB", id.Entropy())
	}
}

// TestSessionMonotonicSameMillisecond verifies strictly increasing ULIDs
// within one millisecond and on-demand entropy reads only.
func TestSessionMonotonicSameMillisecond(t *testing.T) {
	t.Parallel()

	const ms = 1_700_000_000_001
	cr := &countingReader{r: bytes.NewReader(make([]byte, 64))}
	s := ulid.NewSession(fixedClock(ms), cr, 0) // inc=1: deterministic +1

	id1, r1, err := s.Generate()
	if err != nil {
		t.Fatalf("first Generate err = %v", err)
	}
	if r1.Stage != ulid.StageFresh || r1.Count != 1 {
		t.Fatalf("first receipt = %v, want stage=fresh count=1", r1)
	}
	if cr.bytes != 10 {
		t.Fatalf("entropy bytes after first Generate = %d, want exactly 10", cr.bytes)
	}

	id2, r2, err := s.Generate()
	if err != nil {
		t.Fatalf("second Generate err = %v", err)
	}
	if r2.Stage != ulid.StageIncrement || r2.Count != 2 {
		t.Fatalf("second receipt = %v, want stage=increment count=2", r2)
	}
	if id1.Compare(id2) != -1 {
		t.Fatalf("same-ms ULIDs not strictly increasing: %v vs %v", id1, id2)
	}
	if cr.bytes != 10 {
		t.Fatalf("same-ms increment read extra entropy: bytes = %d, want 10", cr.bytes)
	}

	// A third generation in the same millisecond keeps incrementing.
	id3, r3, err := s.Generate()
	if err != nil {
		t.Fatalf("third Generate err = %v", err)
	}
	if r3.Stage != ulid.StageIncrement || r3.Count != 3 {
		t.Fatalf("third receipt = %v, want stage=increment count=3", r3)
	}
	if id2.Compare(id3) != -1 {
		t.Fatalf("same-ms ULIDs not strictly increasing: %v vs %v", id2, id3)
	}
	if cr.bytes != 10 {
		t.Fatalf("same-ms increments read extra entropy: bytes = %d, want 10", cr.bytes)
	}
}

// TestSessionFreshReadOnNewMillisecond verifies that a new millisecond reads
// fresh entropy instead of incrementing.
func TestSessionFreshReadOnNewMillisecond(t *testing.T) {
	t.Parallel()

	ms := uint64(1_700_000_000_000)
	clock := ulid.ClockFunc(func() (uint64, error) { return ms, nil })
	cr := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte{0x01}, 64))}
	s := ulid.NewSession(clock, cr, 0)

	if _, _, err := s.Generate(); err != nil {
		t.Fatalf("first Generate err = %v", err)
	}
	ms++ // advance the clock: next generation must read fresh entropy
	_, r, err := s.Generate()
	if err != nil {
		t.Fatalf("second Generate err = %v", err)
	}
	if r.Stage != ulid.StageFresh || r.Count != 2 {
		t.Fatalf("receipt = %v, want stage=fresh count=2", r)
	}
	if cr.bytes != 20 {
		t.Fatalf("entropy bytes = %d, want 20 (two fresh reads of 10)", cr.bytes)
	}
}

// TestSessionMonotonicOverflow verifies that a monotonic sequence overflow is
// explicitly rejected with ErrMonotonicOverflow and does not advance the
// committed state: every subsequent attempt in the same millisecond keeps
// failing, the count stays put, and no entropy is consumed.
func TestSessionMonotonicOverflow(t *testing.T) {
	t.Parallel()

	const ms = uint64(1_700_000_000_002)
	clock := ms
	cr := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte{0xFF}, 64))}
	s := ulid.NewSession(func() (uint64, error) { return clock, nil }, cr, 0) // inc=1

	id1, r1, err := s.Generate()
	if err != nil {
		t.Fatalf("first Generate err = %v", err)
	}
	if r1.Stage != ulid.StageFresh || r1.Count != 1 {
		t.Fatalf("first receipt = %v, want stage=fresh count=1", r1)
	}

	// Entropy is all 0xFF: incrementing by 1 overflows the 80-bit space.
	for i := 0; i < 3; i++ {
		id, receipt, err := s.Generate()
		if !errors.Is(err, ulid.ErrMonotonicOverflow) {
			t.Fatalf("overflow Generate #%d err = %v, want ErrMonotonicOverflow", i, err)
		}
		if id != ulid.Zero || receipt != (ulid.Receipt{}) {
			t.Fatalf("overflow Generate #%d returned id=%v receipt=%v, want zero values", i, id, receipt)
		}
	}

	// State was not advanced: the overflow consumed no entropy, and once the
	// clock advances the session recovers with a fresh read continuing from
	// the last successful count.
	if cr.bytes != 10 {
		t.Fatalf("entropy bytes = %d, want 10 (overflow must not consume entropy)", cr.bytes)
	}
	clock++
	id2, r2, err := s.Generate()
	if err != nil {
		t.Fatalf("Generate after clock advance err = %v", err)
	}
	if r2.Stage != ulid.StageFresh || r2.Count != 2 {
		t.Fatalf("receipt after overflow = %v, want stage=fresh count=2", r2)
	}
	if id1.Compare(id2) != -1 {
		t.Fatalf("ULIDs not strictly increasing: %v vs %v", id1, id2)
	}
	if got := id1.Entropy(); !bytes.Equal(got, bytes.Repeat([]byte{0xFF}, 10)) {
		t.Fatalf("first entropy = %x, want 10 x 0xFF", got)
	}
}

// TestSessionClockFailure verifies that clock failures are distinguishable
// and do not advance the committed state.
func TestSessionClockFailure(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	calls := 0
	clock := ulid.ClockFunc(func() (uint64, error) {
		calls++
		if calls == 2 {
			return 0, boom
		}
		return 1_700_000_000_003, nil
	})
	cr := &countingReader{r: bytes.NewReader(make([]byte, 64))}
	s := ulid.NewSession(clock, cr, 0)

	id1, r1, err := s.Generate()
	if err != nil {
		t.Fatalf("first Generate err = %v", err)
	}
	if r1.Count != 1 {
		t.Fatalf("first receipt count = %d, want 1", r1.Count)
	}

	// Clock failure: distinguishable via errors.Is, no state advance.
	id, receipt, err := s.Generate()
	if !errors.Is(err, ulid.ErrClockFailure) {
		t.Fatalf("second Generate err = %v, want ErrClockFailure", err)
	}
	if errors.Is(err, ulid.ErrEntropyRead) || errors.Is(err, ulid.ErrMonotonicOverflow) {
		t.Fatalf("clock failure misclassified: %v", err)
	}
	if id != ulid.Zero || receipt != (ulid.Receipt{}) {
		t.Fatalf("clock failure returned id=%v receipt=%v, want zero values", id, receipt)
	}

	// Next success continues from the last committed state (same ms =>
	// increment of id1, count 2): the failed call did not jump state.
	id3, r3, err := s.Generate()
	if err != nil {
		t.Fatalf("third Generate err = %v", err)
	}
	if r3.Stage != ulid.StageIncrement || r3.Count != 2 {
		t.Fatalf("third receipt = %v, want stage=increment count=2", r3)
	}
	if id1.Compare(id3) != -1 {
		t.Fatalf("ULIDs not strictly increasing: %v vs %v", id1, id3)
	}
}

// TestSessionClockOverflow verifies ErrBigTime for out-of-range timestamps.
func TestSessionClockOverflow(t *testing.T) {
	t.Parallel()

	s := ulid.NewSession(fixedClock(ulid.MaxTime()+1), bytes.NewReader(make([]byte, 64)), 0)
	if _, _, err := s.Generate(); !errors.Is(err, ulid.ErrBigTime) {
		t.Fatalf("Generate err = %v, want ErrBigTime", err)
	}
}

// TestSessionDestroy verifies that a destroyed session refuses to generate.
func TestSessionDestroy(t *testing.T) {
	t.Parallel()

	cr := &countingReader{r: bytes.NewReader(make([]byte, 64))}
	s := ulid.NewSession(fixedClock(123), cr, 0)

	if _, _, err := s.Generate(); err != nil {
		t.Fatalf("Generate before Destroy err = %v", err)
	}

	s.Destroy()

	id, receipt, err := s.Generate()
	if !errors.Is(err, ulid.ErrSessionClosed) {
		t.Fatalf("Generate after Destroy err = %v, want ErrSessionClosed", err)
	}
	if id != ulid.Zero || receipt != (ulid.Receipt{}) {
		t.Fatalf("Generate after Destroy returned id=%v receipt=%v, want zero values", id, receipt)
	}
	if cr.bytes != 10 {
		t.Fatalf("entropy bytes = %d, want 10 (destroyed session must not read)", cr.bytes)
	}
}

// ExampleSession demonstrates a three-step generation with an injectable
// session: a fresh read, a same-millisecond monotonic increment, and a fresh
// read after the clock advances. The receipt of each step carries only the
// generation stage and the count of successful generations.
func ExampleSession() {
	ms := uint64(1_700_000_000_000)
	clock := ulid.ClockFunc(func() (uint64, error) { return ms, nil })
	entropy := &countingReader{r: bytes.NewReader(bytes.Repeat([]byte{0x07}, 64))}

	s := ulid.NewSession(clock, entropy, 0)

	// Step 1: first generation reads fresh entropy.
	id1, r1, _ := s.Generate()

	// Step 2: same millisecond, entropy is monotonically incremented.
	id2, r2, _ := s.Generate()

	// Step 3: the clock advances, entropy is read fresh again.
	ms++
	id3, r3, _ := s.Generate()

	fmt.Println(id1.Time() < id3.Time() || id1.Compare(id2) == -1)
	fmt.Println(r1, r2, r3, entropy.bytes)
	// Output:
	// true
	// stage=fresh count=1 stage=increment count=2 stage=fresh count=3 20
}
