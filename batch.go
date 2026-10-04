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
	"bufio"
	"errors"
	"io"
)

var (
	// ErrInvalidRecord is returned by a BatchValidator for a record whose text
	// is not a syntactically valid ULID (wrong length, invalid base32
	// characters, or an overflowing first character).
	ErrInvalidRecord = errors.New("ulid: record is not a valid ULID")

	// ErrTimeReversed is returned by a BatchValidator for a record whose
	// 48-bit millisecond timestamp is older than the most recent accepted
	// record's timestamp. Records with the same timestamp but different
	// randomness are allowed.
	ErrTimeReversed = errors.New("ulid: record timestamp precedes the most recent accepted record")

	// ErrDuplicate is returned by a BatchValidator for a record identical to
	// one that is still within the uniqueness window. With a finite window,
	// this error only certifies uniqueness within that window; records that
	// have expired from the window are not claimed to be globally unique.
	ErrDuplicate = errors.New("ulid: record duplicates an accepted record")
)

// ErrorCategory classifies a rejected record.
type ErrorCategory int

const (
	// CategoryUnknown is the zero value and is never reported.
	CategoryUnknown ErrorCategory = iota

	// CategoryInvalid means the record text could not be parsed as a ULID.
	CategoryInvalid

	// CategoryTimeReversed means the record's timestamp is older than the
	// most recent accepted timestamp.
	CategoryTimeReversed

	// CategoryDuplicate means the record duplicates one still inside the
	// uniqueness window.
	CategoryDuplicate
)

// String returns a stable, lowercase name for the category.
func (c ErrorCategory) String() string {
	switch c {
	case CategoryInvalid:
		return "invalid"
	case CategoryTimeReversed:
		return "time-reversed"
	case CategoryDuplicate:
		return "duplicate"
	default:
		return "unknown"
	}
}

// RecordError describes a single rejected record. The Index is the record's
// original 0-based position in the input stream, so callers can correlate
// errors with their input without the records being buffered.
type RecordError struct {
	// Index is the 0-based position of the record in the validated stream.
	Index int

	// Record is the original, unmodified record text.
	Record string

	// Category is the classification of the rejection.
	Category ErrorCategory

	// ParseErr is the original parse error (ErrDataSize, ErrInvalidCharacters
	// or ErrOverflow) when Category is CategoryInvalid, and nil otherwise.
	ParseErr error

	categoryErr error
}

// invalidRecordErr is the category error for syntactically invalid records.
// Its Is method lets errors.Is reach both ErrInvalidRecord and the original
// parse error (ErrDataSize, ErrInvalidCharacters or ErrOverflow).
type invalidRecordErr struct{ cause error }

func (e *invalidRecordErr) Error() string { return ErrInvalidRecord.Error() }

func (e *invalidRecordErr) Is(target error) bool {
	return target == ErrInvalidRecord || errors.Is(e.cause, target)
}

func newRecordError(index int, record string, category ErrorCategory, categoryErr, parseErr error) *RecordError {
	return &RecordError{
		Index:       index,
		Record:      record,
		Category:    category,
		ParseErr:    parseErr,
		categoryErr: categoryErr,
	}
}

// Error implements the error interface.
func (e *RecordError) Error() string {
	switch e.Category {
	case CategoryInvalid:
		if e.ParseErr != nil {
			return "ulid: record " + itoa(e.Index) + " is invalid: " + e.ParseErr.Error()
		}
		return "ulid: record " + itoa(e.Index) + " is invalid"
	case CategoryTimeReversed:
		return "ulid: record " + itoa(e.Index) + " timestamp precedes the most recent accepted record"
	case CategoryDuplicate:
		return "ulid: record " + itoa(e.Index) + " duplicates an accepted record"
	default:
		return "ulid: record " + itoa(e.Index) + " is rejected"
	}
}

// Unwrap exposes the category sentinel error (ErrInvalidRecord,
// ErrTimeReversed or ErrDuplicate) for use with errors.Is and errors.As.
func (e *RecordError) Unwrap() error { return e.categoryErr }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// BatchValidator validates a stream of text-encoded ULIDs one record at a
// time. It never buffers the whole input batch: at most windowSize accepted
// records are retained for duplicate detection.
//
// Records are checked in order:
//
//  1. Syntax: the record is parsed with ParseStrict, so length, base32
//     character set, and 128-bit overflow are all enforced. Upper and lower
//     case are accepted and normalized by the existing parse rules.
//  2. Time order: a valid record must not be older (by the 48-bit millisecond
//     timestamp) than the most recent accepted record. Equal timestamps with
//     different random components are allowed.
//  3. Uniqueness: the parsed ULID must not equal a record still inside the
//     configured window.
//
// Invalid, time-reversed and duplicate records are rejected and do not
// advance the most recent accepted timestamp and do not enter the window.
//
// A BatchValidator is not safe for concurrent use.
type BatchValidator struct {
	window   int // 0 means unbounded
	ring     []ULID
	pos      int
	size     int
	seen     map[ULID]struct{}
	lastTime uint64
	haveLast bool
}

// NewBatchValidator returns a BatchValidator that detects duplicates among
// the most recent windowSize accepted records. A windowSize of 0 disables
// window expiry and checks uniqueness against every accepted record.
// windowSize must not be negative; a negative value panics.
//
// When windowSize is positive, duplicate detection is explicitly limited to
// that window: records older than the window are forgotten and may repeat
// without being reported. Nothing in the validator claims global uniqueness
// when a finite window is in effect.
func NewBatchValidator(windowSize int) *BatchValidator {
	if windowSize < 0 {
		panic("ulid: BatchValidator window size must be non-negative")
	}
	v := &BatchValidator{
		window: windowSize,
		seen:   make(map[ULID]struct{}),
	}
	if windowSize > 0 {
		v.ring = make([]ULID, windowSize)
	}
	return v
}

// Validate parses and checks one record, returning the normalized ULID on
// success or a *RecordError describing the rejection. The index is the
// record's 0-based position in the caller's stream and is echoed unchanged
// in the error.
//
// A rejected record never advances the most recent accepted timestamp and
// never enters the uniqueness window.
func (v *BatchValidator) Validate(index int, record string) (ULID, *RecordError) {
	id, err := ParseStrict(record)
	if err != nil {
		catErr := &invalidRecordErr{cause: err}
		return id, newRecordError(index, record, CategoryInvalid, catErr, err)
	}

	if v.haveLast && id.Time() < v.lastTime {
		return id, newRecordError(index, record, CategoryTimeReversed, ErrTimeReversed, nil)
	}

	if _, ok := v.seen[id]; ok {
		return id, newRecordError(index, record, CategoryDuplicate, ErrDuplicate, nil)
	}

	v.accept(id)
	return id, nil
}

// accept records a validated ULID as the new most recent record. Only
// Validate, after every check succeeds, calls it.
func (v *BatchValidator) accept(id ULID) {
	v.lastTime = id.Time()
	v.haveLast = true
	v.seen[id] = struct{}{}

	if v.window == 0 {
		return
	}

	if v.size < v.window {
		v.ring[v.pos] = id
		v.pos = (v.pos + 1) % v.window
		v.size++
		return
	}

	// The window is full: evict the oldest record so that it is no longer
	// part of the uniqueness guarantee.
	oldest := v.ring[v.pos]
	delete(v.seen, oldest)
	v.ring[v.pos] = id
	v.pos = (v.pos + 1) % v.window
}

// LastTime returns the timestamp of the most recent accepted record. The
// boolean is false before the first record is accepted. Rejected records do
// not affect this value.
func (v *BatchValidator) LastTime() (uint64, bool) { return v.lastTime, v.haveLast }

// WindowSize returns the configured duplicate window: the result is false
// when the validator checks against every accepted record, and true together
// with the capacity when a finite window is in effect.
func (v *BatchValidator) WindowSize() (capacity int, bounded bool) {
	return v.window, v.window > 0
}

// UniquenessScope returns a human-readable statement of what uniqueness the
// validator certifies. A finite window is explicitly described as partial so
// that callers cannot mistake it for a global guarantee.
func (v *BatchValidator) UniquenessScope() string {
	if v.window == 0 {
		return "uniqueness is guaranteed among all accepted records"
	}
	return "uniqueness is guaranteed only among the most recent " + itoa(v.window) +
		" accepted records; records that left the window are not checked and may repeat"
}

// StreamReport summarizes a streamed validation pass.
type StreamReport struct {
	// Total is the number of records read.
	Total int

	// Accepted and Rejected count records by outcome.
	Accepted int
	Rejected int

	// Errors lists rejected records in encounter order, each retaining its
	// original index and error category.
	Errors []*RecordError

	// WindowCapacity is the configured window; 0 means an unbounded window.
	WindowCapacity int
}

// Bounded reports whether duplicate detection used a finite sliding window.
func (r *StreamReport) Bounded() bool { return r.WindowCapacity > 0 }

// UniquenessScope states the exact scope of the uniqueness guarantee for the
// pass that produced the report. With a finite window it makes clear that
// expired records are not covered.
func (r *StreamReport) UniquenessScope() string {
	if r.WindowCapacity == 0 {
		return "uniqueness is guaranteed among all accepted records"
	}
	return "uniqueness is guaranteed only among the most recent " + itoa(r.WindowCapacity) +
		" accepted records; records that left the window are not checked and may repeat"
}

// ValidateReader reads records from r one line at a time and validates each
// line with Validate. Lines are split by bufio.ScanLines, so CRLF line
// endings are handled and a final empty line is ignored. The reader is
// consumed incrementally; the input batch is never held in memory.
//
// Rejected records are collected in the returned StreamReport. For callers
// that also need to avoid buffering the (typically small) error list, drive
// Validate directly over a bufio.Scanner.
func (v *BatchValidator) ValidateReader(r io.Reader) (*StreamReport, error) {
	report := &StreamReport{WindowCapacity: v.window}
	s := bufio.NewScanner(r)
	for s.Scan() {
		record := s.Text()
		if _, rerr := v.Validate(report.Total, record); rerr != nil {
			report.Rejected++
			report.Errors = append(report.Errors, rerr)
		} else {
			report.Accepted++
		}
		report.Total++
	}
	return report, s.Err()
}
