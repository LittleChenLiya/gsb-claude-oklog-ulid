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

import "errors"

// ErrEmptyRange is returned when a time range's start is greater than or
// equal to its end, i.e. the half-open interval [start, end) contains no
// timestamps.
var ErrEmptyRange = errors.New("ulid: empty time range")

// Range returns the inclusive lower and upper ULID bounds that cover every
// possible ULID with a timestamp in the half-open millisecond interval
// [start, end).
//
// The lower bound has timestamp start and zero entropy (0x00..0x00); the
// upper bound has timestamp end-1 and full entropy (0xFF..0xFF). The returned
// values are the 16-byte binary representations and may be used directly as
// index key bounds without enumerating every ULID in the range.
//
// Bounds are computed with SetTime and ordered with Compare, so the result is
// consistent with the library's own lexicographic ordering. In particular,
// string case does not affect ordering: upper and lower case encodings
// decode to the same ULID and therefore compare equal.
//
// An error is returned in the following cases:
//   - start >= end: the interval is empty (ErrEmptyRange).
//   - end > MaxTime()+1: the upper bound timestamp would overflow the
//     48-bit timestamp field (ErrBigTime).
func Range(start, end uint64) (min, max ULID, err error) {
	if start >= end {
		return min, max, ErrEmptyRange
	}
	if end > maxTime+1 {
		return min, max, ErrBigTime
	}

	// Lower bound: timestamp start, entropy all zeros.
	// min is the zero ULID, so its entropy bytes are already 0x00.
	if err = min.SetTime(start); err != nil {
		return min, max, err
	}

	// Upper bound: timestamp end-1, entropy all ones.
	// end-1 is guaranteed <= maxTime because end <= maxTime+1.
	if err = max.SetTime(end - 1); err != nil {
		return min, max, err
	}
	for i := 6; i < 16; i++ {
		max[i] = 0xFF
	}

	return min, max, nil
}
