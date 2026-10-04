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

import "errors"

// BatchResult is the outcome of validating a single record in a stream of
// ULID records.
type BatchResult struct {
	// Index is the zero-based ordinal of the record in the order it was
	// fed to the validator. The validator emits exactly one result per
	// record and never buffers the batch, so callers can correlate every
	// outcome with its source record, including records that failed
	// validation.
	Index int

	// ID is the parsed ULID. It is the zero value when the record is
	// syntactically invalid.
	ID ULID

	// Err is nil when the record is valid. Otherwise it is one of:
	//
	//   - ErrDataSize, ErrInvalidCharacters or ErrOverflow for a syntax
	//     error (the record could not be parsed);
	//   - ErrBatchRegression when the record's 48-bit millisecond
	//     timestamp is earlier than the most recent valid record's;
	//   - ErrBatchDuplicate when the record duplicates a valid record that
	//     is still retained in the validator's finite window.
	Err error
}

// ErrBatchRegression is returned when a syntactically valid record has a
// 48-bit millisecond timestamp earlier than the most recent valid record
// in the stream. A record that fails the monotonicity check does not
// advance the validator's time baseline.
var ErrBatchRegression = errors.New("ulid: batch record time regression")

// ErrBatchDuplicate is returned when a record's ULID duplicates a valid
// record still retained in the validator's finite window.
//
// Guarantee scope: the validator can only report duplicates among the
// records it still remembers. A record that is not reported as a duplicate
// is therefore guaranteed unique only within the configured window, not
// globally unique. Once a record has aged out of the window it is no
// longer remembered, and re-importing it will not be flagged as a
// duplicate.
var ErrBatchDuplicate = errors.New("ulid: batch record duplicate within window")

// BatchValidator validates a stream of ULID records one record at a time,
// detecting syntax errors, time regressions and duplicates within a
// bounded window.
//
// Records are fed in stream order via Validate. Each record is parsed and
// validated immediately and exactly one BatchResult is emitted per record;
// the batch as a whole is never buffered. Only valid records advance the
// time baseline and enter the duplicate-detection window, so syntactically
// invalid or out-of-order records in the middle of the stream do not shift
// later results.
//
// Time ordering uses the ULID's 48-bit millisecond timestamp (see
// ULID.Time): two distinct ULIDs sharing the same millisecond are ordered
// by their entropy and are not a regression. Case is normalized by the
// existing parsing rules, so upper- and lower-case encodings of the same
// ULID compare equal.
//
// Duplicate detection is bounded by the window given to
// NewBatchValidator. Only the window most recent valid records are
// remembered, so absence of a duplicate error is a guarantee of uniqueness
// within the window only — never a guarantee of global uniqueness. A
// window of 0 remembers every valid record (exact duplicate detection) at
// the cost of unbounded memory.
//
// BatchValidator is not safe for concurrent use.
type BatchValidator struct {
	window   int
	seen     map[ULID]struct{}
	ring     []ULID
	head     int // oldest position in ring
	count    int // number of valid records retained (<= window)
	lastMS   uint64
	haveLast bool
	index    int
}

// NewBatchValidator returns a BatchValidator that remembers at most window
// valid records for duplicate detection. A window of 0 (or any negative
// value) remembers every valid record, which gives exact global duplicate
// detection but uses memory proportional to the stream length.
func NewBatchValidator(window int) *BatchValidator {
	v := &BatchValidator{window: window}
	if window > 0 {
		v.seen = make(map[ULID]struct{}, window)
		v.ring = make([]ULID, window)
	} else {
		v.seen = make(map[ULID]struct{})
	}
	return v
}

// Validate parses and validates a single record and returns its result.
// Records must be fed in stream order. The returned BatchResult.Index is
// the record's zero-based position in the stream.
//
// Validation order is: syntax, then time regression, then duplicate. A
// record that fails any of these checks does not advance the time baseline
// and is not added to the duplicate-detection window.
func (v *BatchValidator) Validate(record string) BatchResult {
	idx := v.index
	v.index++

	id, err := ParseStrict(record)
	if err != nil {
		return BatchResult{Index: idx, Err: err}
	}

	if v.haveLast && id.Time() < v.lastMS {
		return BatchResult{Index: idx, ID: id, Err: ErrBatchRegression}
	}

	if _, ok := v.seen[id]; ok {
		return BatchResult{Index: idx, ID: id, Err: ErrBatchDuplicate}
	}

	v.lastMS = id.Time()
	v.haveLast = true
	v.insert(id)

	return BatchResult{Index: idx, ID: id}
}

// insert adds a valid record to the duplicate-detection window, evicting
// the oldest retained record when the window is full.
func (v *BatchValidator) insert(id ULID) {
	if v.window <= 0 {
		v.seen[id] = struct{}{}
		return
	}
	if v.count < v.window {
		v.ring[(v.head+v.count)%v.window] = id
		v.count++
	} else {
		evicted := v.ring[v.head]
		delete(v.seen, evicted)
		v.ring[v.head] = id
		v.head = (v.head + 1) % v.window
	}
	v.seen[id] = struct{}{}
}
