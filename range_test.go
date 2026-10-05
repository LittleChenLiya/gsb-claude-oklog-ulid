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
	"fmt"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
)

// ExampleRange demonstrates the four ULIDs that bound the half-open
// millisecond range [10, 11): the lower bound (time 10, zero entropy), the
// upper bound (time 10, full entropy), and the two excluded neighbors
// (time 9 and time 11).
func ExampleRange() {
	min, max, err := ulid.Range(10, 11)
	if err != nil {
		panic(err)
	}

	// Four ULID examples:
	// 1. Lower bound: timestamp 10, entropy 0x00..0x00.
	// 2. Upper bound: timestamp 10, entropy 0xFF..0xFF.
	// 3. Time 9: excluded (below the lower bound).
	// 4. Time 11: excluded (above the upper bound).
	below := ulid.MustNew(9, nil)
	above := ulid.MustNew(11, nil)

	fmt.Println("min:", min)
	fmt.Println("max:", max)
	fmt.Println("below:", below)
	fmt.Println("above:", above)
	fmt.Println("below < min:", below.Compare(min) < 0)
	fmt.Println("above > max:", above.Compare(max) > 0)
	// Output:
	// min: 000000000A0000000000000000
	// max: 000000000AZZZZZZZZZZZZZZZZ
	// below: 00000000090000000000000000
	// above: 000000000B0000000000000000
	// below < min: true
	// above > max: true
}

func TestRange(t *testing.T) {
	t.Parallel()

	t.Run("half-open [10,11) covers only time 10", func(t *testing.T) {
		min, max, err := ulid.Range(10, 11)
		if err != nil {
			t.Fatal(err)
		}

		// Lower bound: timestamp 10, zero entropy.
		wantMin := ulid.MustNew(10, bytes.NewReader(make([]byte, 16)))
		if min != wantMin {
			t.Errorf("min: got %v, want %v", min, wantMin)
		}

		// Upper bound: timestamp 10, full entropy.
		wantMax := ulid.MustNew(10, bytes.NewReader(bytes.Repeat([]byte{0xFF}, 16)))
		if max != wantMax {
			t.Errorf("max: got %v, want %v", max, wantMax)
		}

		// Every ULID with timestamp 10 is within [min, max].
		for _, e := range [][]byte{
			make([]byte, 10),
			bytes.Repeat([]byte{0xFF}, 10),
			{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23},
		} {
			id := ulid.MustNew(10, bytes.NewReader(e))
			if id.Compare(min) < 0 || id.Compare(max) > 0 {
				t.Errorf("ULID %v (time 10) outside [min, max]", id)
			}
		}

		// Time 9 is excluded: below the lower bound.
		if below := ulid.MustNew(9, nil); below.Compare(min) >= 0 {
			t.Errorf("time 9 should be below min: %v >= %v", below, min)
		}

		// Time 11 is excluded: above the upper bound.
		if above := ulid.MustNew(11, nil); above.Compare(max) <= 0 {
			t.Errorf("time 11 should be above max: %v <= %v", above, max)
		}
	})

	t.Run("max time boundary does not overflow", func(t *testing.T) {
		maxTime := ulid.MaxTime()

		// [MaxTime, MaxTime+1) is the largest valid range.
		min, max, err := ulid.Range(maxTime, maxTime+1)
		if err != nil {
			t.Fatal(err)
		}

		// Lower bound: timestamp MaxTime, zero entropy.
		if got, want := min.Time(), maxTime; got != want {
			t.Errorf("min.Time(): got %d, want %d", got, want)
		}

		// Upper bound: timestamp MaxTime, full entropy.
		if got, want := max.Time(), maxTime; got != want {
			t.Errorf("max.Time(): got %d, want %d", got, want)
		}
		for i := 6; i < 16; i++ {
			if max[i] != 0xFF {
				t.Errorf("max[%d]: got 0x%02X, want 0xFF", i, max[i])
			}
		}

		// The upper bound is the largest possible ULID.
		largest := ulid.MustNew(maxTime, bytes.NewReader(bytes.Repeat([]byte{0xFF}, 16)))
		if max != largest {
			t.Errorf("max: got %v, want %v", max, largest)
		}

		// Any range ending beyond MaxTime+1 overflows.
		if _, _, err := ulid.Range(0, maxTime+2); err != ulid.ErrBigTime {
			t.Errorf("Range(0, %d): got err %v, want %v", maxTime+2, err, ulid.ErrBigTime)
		}
	})

	t.Run("empty range is explicitly represented", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			start uint64
			end   uint64
		}{
			{"start > end", 11, 10},
			{"start == end", 10, 10},
			{"zero range", 0, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				min, max, err := ulid.Range(tc.start, tc.end)
				if err != ulid.ErrEmptyRange {
					t.Errorf("got err %v, want %v", err, ulid.ErrEmptyRange)
				}
				// Bounds must be the zero ULID on error.
				if !min.IsZero() || !max.IsZero() {
					t.Errorf("bounds not zero on error: min=%v max=%v", min, max)
				}
			})
		}
	})

	t.Run("string case does not affect ordering", func(t *testing.T) {
		min, max, err := ulid.Range(10, 11)
		if err != nil {
			t.Fatal(err)
		}

		// Upper and lower case encodings decode to the same ULID.
		for _, id := range []ulid.ULID{min, max} {
			upper := ulid.MustParse(strings.ToUpper(id.String()))
			lower := ulid.MustParse(strings.ToLower(id.String()))
			if upper.Compare(lower) != 0 {
				t.Errorf("case-insensitive ordering broken: %v vs %v", upper, lower)
			}
		}

		// Byte bounds are directly usable as index key bounds.
		if bytes.Compare(min[:], max[:]) > 0 {
			t.Errorf("min > max: %v > %v", min, max)
		}
	})
}
