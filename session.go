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

package ulid

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
)

var (
	// ErrSessionClosed is returned by Session.Generate after the session has
	// been destroyed with Session.Destroy.
	ErrSessionClosed = errors.New("ulid: generation session destroyed")

	// ErrClockFailure is returned by Session.Generate when the session
	// clock fails to yield a usable timestamp.
	ErrClockFailure = errors.New("ulid: clock failure")

	// ErrEntropyRead is returned by Session.Generate when the session
	// entropy source cannot provide enough bytes (a short read).
	ErrEntropyRead = errors.New("ulid: entropy short read")
)

// Stage identifies which generation step produced a ULID within a Session.
type Stage uint8

const (
	// StageFresh indicates that entropy was freshly read from the entropy
	// source because a new millisecond (or the first generation) was seen.
	StageFresh Stage = iota + 1

	// StageIncrement indicates that the previously committed entropy was
	// monotonically incremented within the same millisecond.
	StageIncrement
)

// String returns a human readable name of the Stage.
func (s Stage) String() string {
	switch s {
	case StageFresh:
		return "fresh"
	case StageIncrement:
		return "increment"
	default:
		return "unknown"
	}
}

// Receipt acknowledges a single successful Session.Generate call. It carries
// only the generation stage and the count of successful generations so far.
type Receipt struct {
	Stage Stage
	Count uint64
}

// String returns a human readable form of the Receipt.
func (r Receipt) String() string {
	return fmt.Sprintf("stage=%s count=%d", r.Stage, r.Count)
}

// ClockFunc yields the current Unix time in milliseconds. It is injectable
// so tests and specialized runtimes can control the clock observed by a
// Session.
type ClockFunc func() (uint64, error)

// Session is an explicit, injectable ULID generation session. Unlike the
// package level New and Make helpers, a Session owns its monotonic state,
// distinguishes clock failures, entropy short reads and monotonic sequence
// overflows, and never advances its previously committed state when a
// generation fails.
//
// The entropy source is read strictly on demand: no bytes are consumed when
// the session is created, exactly 10 bytes are consumed when a fresh
// millisecond is observed, and same-millisecond increments only consume
// entropy when a random increment is required (inc > 1).
//
// A Session is not safe for concurrent use.
type Session struct {
	clock   ClockFunc
	entropy io.Reader
	inc     uint64

	closed bool
	ok     bool // whether ms/last hold a committed, previously successful state
	ms     uint64
	last   uint80
	count  uint64
}

// NewSession returns a Session that generates ULIDs using the given clock
// and entropy source. A nil clock defaults to the current time via Now, and
// a nil entropy source defaults to DefaultEntropy.
//
// The inc parameter bounds the random increment applied to the entropy of
// consecutive ULIDs within the same millisecond, mirroring Monotonic. The
// zero value defaults to 1, meaning deterministic +1 increments that consume
// no additional entropy.
//
// NewSession performs no I/O: the entropy source is only read when Generate
// actually needs bytes.
func NewSession(clock ClockFunc, entropy io.Reader, inc uint64) *Session {
	if clock == nil {
		clock = func() (uint64, error) { return Now(), nil }
	}
	if entropy == nil {
		entropy = DefaultEntropy()
	}
	if inc == 0 {
		inc = 1
	}
	return &Session{
		clock:   clock,
		entropy: entropy,
		inc:     inc,
	}
}

// Generate produces the next ULID of the session along with a Receipt
// describing the generation. On any failure the returned ULID and Receipt
// are zero and the previously committed session state is left untouched, so
// a subsequent successful Generate continues exactly where the last success
// left off.
//
// Errors are distinguishable with errors.Is:
//
//   - ErrClockFailure: the clock did not yield a timestamp.
//   - ErrBigTime: the clock yielded a timestamp larger than MaxTime.
//   - ErrEntropyRead: the entropy source short-read.
//   - ErrMonotonicOverflow: incrementing the previous entropy would overflow.
//   - ErrSessionClosed: the session has been destroyed.
func (s *Session) Generate() (id ULID, r Receipt, err error) {
	if s.closed {
		return id, r, ErrSessionClosed
	}

	ms, err := s.clock()
	if err != nil {
		return id, r, fmt.Errorf("%w: %v", ErrClockFailure, err)
	}
	if ms > maxTime {
		return id, r, ErrBigTime
	}

	var (
		next  uint80
		stage = StageFresh
	)

	if s.ok && s.ms == ms {
		// Same millisecond as the last committed state: monotonic
		// increment. Compute the candidate without mutating s.last so a
		// failure cannot advance the committed state.
		inc, ierr := s.increment()
		if ierr != nil {
			return id, r, fmt.Errorf("%w: %v", ErrEntropyRead, ierr)
		}
		var overflow bool
		if next, overflow = addUint80(s.last, inc); overflow {
			return id, r, ErrMonotonicOverflow
		}
		stage = StageIncrement
	} else {
		// New millisecond (or first generation): read fresh entropy.
		var buf [10]byte
		if _, rerr := io.ReadFull(s.entropy, buf[:]); rerr != nil {
			return id, r, fmt.Errorf("%w: %v", ErrEntropyRead, rerr)
		}
		next.SetBytes(buf[:])
	}

	// Commit only on success.
	s.ok = true
	s.ms = ms
	s.last = next
	s.count++

	if err = id.SetTime(ms); err != nil {
		return id, r, err // unreachable: ms <= maxTime checked above
	}
	next.AppendTo(id[6:])

	return id, Receipt{Stage: stage, Count: s.count}, nil
}

// MustGenerate is a convenience method equivalent to Generate that panics on
// failure instead of returning an error.
func (s *Session) MustGenerate() ULID {
	id, _, err := s.Generate()
	if err != nil {
		panic(err)
	}
	return id
}

// Destroy closes the session and clears its committed state. Any subsequent
// Generate call fails with ErrSessionClosed.
func (s *Session) Destroy() {
	s.closed = true
	s.ok = false
	s.ms = 0
	s.last = uint80{}
}

// increment returns a uniform random increment in [1, s.inc], reading entropy
// from the session entropy source only when s.inc > 1. When s.inc == 1 the
// increment is deterministically 1 and no entropy is consumed.
func (s *Session) increment() (uint64, error) {
	if s.inc <= 1 {
		return 1, nil
	}

	// bitLen is the maximum bit length needed to encode a value < s.inc.
	bitLen := bits.Len64(s.inc)

	// byteLen is the maximum byte length needed to encode a value < s.inc.
	byteLen := uint(bitLen+7) / 8

	// msbitLen is the number of bits in the most significant byte of s.inc-1.
	msbitLen := uint(bitLen % 8)
	if msbitLen == 0 {
		msbitLen = 8
	}

	var (
		rand [8]byte
		inc  uint64
	)
	for inc == 0 || inc >= s.inc {
		if _, err := io.ReadFull(s.entropy, rand[:byteLen]); err != nil {
			return 0, err
		}

		// Clear bits in the first byte to increase the probability
		// that the candidate is < s.inc.
		rand[0] &= uint8(int(1<<msbitLen) - 1)

		inc = binary.LittleEndian.Uint64(rand[:])
	}

	// Range: [1, s.inc]
	return 1 + inc, nil
}

// addUint80 returns u+n and whether the addition overflowed 80 bits. The
// input value is not mutated, so callers can discard the result on overflow
// without corrupting previously committed state.
func addUint80(u uint80, n uint64) (next uint80, overflow bool) {
	next = u
	next.Lo += n
	if next.Lo < u.Lo {
		next.Hi++
	}
	return next, next.Hi < u.Hi
}
