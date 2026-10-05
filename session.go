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

package ulid

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/bits"
)

// ErrSessionClosed is returned by Generate after a Session has been
// destroyed by Close.
var ErrSessionClosed = errors.New("ulid: session closed")

// Stage identifies the stage of ULID generation recorded by a Receipt.
type Stage uint8

const (
	// StageClock is the timestamp validation stage. A failure at this stage
	// means the timestamp was invalid (ErrBigTime); no entropy was read.
	StageClock Stage = iota

	// StageEntropy is the initial entropy read stage. It is reached when a
	// session has no usable state for the requested millisecond.
	StageEntropy

	// StageMonotonic is the monotonic entropy increment stage. It is reached
	// when generating another ULID within the same millisecond as the last
	// successful generation.
	StageMonotonic
)

// String returns the name of the stage.
func (s Stage) String() string {
	switch s {
	case StageClock:
		return "clock"
	case StageEntropy:
		return "entropy"
	case StageMonotonic:
		return "monotonic"
	default:
		return "unknown"
	}
}

// Receipt is a minimal receipt for a Generate call: the stage that produced
// the entropy and the number of entropy bytes consumed from the injected
// source.
type Receipt struct {
	Stage Stage
	Count int
}

// Session is an explicit, injectable ULID generation session.
//
// A Session owns an entropy source and keeps the monotonic state for the
// current millisecond. All reads go directly to the injected source, only
// when they are actually needed: sessions never prefetch.
//
// A failed Generate never advances the session's monotonic state, so a later
// successful generation does not skip any entropy.
//
// A Session is not safe for concurrent use.
type Session struct {
	rng     io.Reader
	inc     uint64
	have    bool
	ms      uint64
	entropy uint80
	closed  bool
}

// NewSession returns a new generation session that reads entropy from r.
// Monotonic increments use the default increment range of [1, MaxUint32).
func NewSession(r io.Reader) *Session {
	return NewSessionWithInc(r, 0)
}

// NewSessionWithInc returns a new generation session like NewSession but
// with the given monotonic increment bound. See Monotonic for the meaning
// of the inc parameter.
func NewSessionWithInc(r io.Reader, inc uint64) *Session {
	if r == nil {
		r = eofReader{}
	}
	if inc == 0 {
		inc = math.MaxUint32
	}
	return &Session{rng: r, inc: inc}
}

// Close destroys the session. Any further call to Generate returns
// ErrSessionClosed.
func (s *Session) Close() error {
	s.closed = true
	return nil
}

// Generate returns a ULID for the given Unix millisecond timestamp, a receipt
// describing the stage that produced the entropy and the number of entropy
// bytes consumed, and an error.
//
// Errors are distinguished by stage:
//   - clock: ErrBigTime
//   - entropy: io.EOF / io.ErrUnexpectedEOF from the entropy source
//   - monotonic: ErrMonotonicOverflow
//
// A failed Generate never advances the session's monotonic state.
func (s *Session) Generate(ms uint64) (id ULID, rcpt Receipt, err error) {
	if s.closed {
		return id, Receipt{}, ErrSessionClosed
	}

	// Clock stage: validate and set the timestamp.
	if err = id.SetTime(ms); err != nil {
		return id, Receipt{Stage: StageClock}, err
	}

	if !s.have || s.ms != ms {
		// Entropy stage: read exactly the 10 entropy bytes, only now.
		var n int
		n, err = io.ReadFull(s.rng, id[6:])
		rcpt = Receipt{Stage: StageEntropy, Count: n}
		if err != nil {
			return id, rcpt, err
		}
		// Commit state only after a fully successful read.
		s.ms = ms
		s.entropy.SetBytes(id[6:])
		s.have = true
		return id, rcpt, nil
	}

	// Monotonic stage: increment the previous entropy.
	var inc uint64
	var n int
	inc, n, err = s.nextInc()
	rcpt = Receipt{Stage: StageMonotonic, Count: n}
	if err != nil {
		return id, rcpt, err
	}
	next := s.entropy
	if next.Add(inc) {
		return id, rcpt, ErrMonotonicOverflow
	}
	// Commit state only after the increment succeeds.
	s.entropy = next
	next.AppendTo(id[6:])
	return id, rcpt, nil
}

// nextInc returns the next monotonic increment and the number of entropy
// bytes consumed computing it. Bytes are read from the injected source only
// when the configured increment requires randomness.
func (s *Session) nextInc() (inc uint64, n int, err error) {
	if s.inc <= 1 {
		return 1, 0, nil
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

	var buf [8]byte
	for inc == 0 || inc >= s.inc {
		var m int
		m, err = io.ReadFull(s.rng, buf[:byteLen])
		n += m
		if err != nil {
			return 0, n, err
		}

		// Clear bits in the first byte to increase the probability
		// that the candidate is < s.inc.
		buf[0] &= uint8(int(1<<msbitLen) - 1)

		// Convert the read bytes into an uint64 with byteLen.
		switch byteLen {
		case 1:
			inc = uint64(buf[0])
		case 2:
			inc = uint64(binary.LittleEndian.Uint16(buf[:2]))
		case 3, 4:
			inc = uint64(binary.LittleEndian.Uint32(buf[:4]))
		case 5, 6, 7, 8:
			inc = uint64(binary.LittleEndian.Uint64(buf[:8]))
		}
	}

	// Range: [1, s.inc)
	return 1 + inc, n, nil
}

// eofReader is used as a safe fallback when a Session is created with a nil
// entropy source. Reads fail immediately with io.EOF.
type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }
