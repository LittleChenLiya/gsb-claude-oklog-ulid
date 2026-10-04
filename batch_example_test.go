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
	"bufio"
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"
)

// ExampleBatchValidator shows how to validate a streaming import one record
// at a time with a bufio.Scanner: the batch is never collected into a slice,
// and each rejection keeps its original 0-based line index and error category.
func ExampleBatchValidator() {
	makeID := func(ms uint64, ent byte) string {
		var id ulid.ULID
		if err := id.SetTime(ms); err != nil {
			panic(err)
		}
		id[6] = ent
		return id.String()
	}

	// One ULID per line, as a small stand-in for an import file or pipe.
	stream := strings.NewReader(strings.Join([]string{
		makeID(100, 1),
		makeID(100, 2), // same millisecond, different randomness: allowed
		makeID(99, 3),  // older timestamp
		makeID(100, 1), // repeats the first record
		"not-a-ulid",   // bad syntax; baseline stays at 100
	}, "\n"))

	// window 0: check every accepted record for duplicates. Pass a positive
	// size to keep only the most recent N records in the uniqueness window.
	v := ulid.NewBatchValidator(0)

	sc := bufio.NewScanner(stream)
	for index := 0; sc.Scan(); index++ {
		if _, err := v.Validate(index, sc.Text()); err != nil {
			fmt.Printf("line %d: %s\n", err.Index, err.Category)
			continue
		}
		fmt.Printf("line %d: ok\n", index)
	}
	if err := sc.Err(); err != nil {
		fmt.Println("read error:", err)
	}
	fmt.Println(v.UniquenessScope())
	// Output:
	// line 0: ok
	// line 1: ok
	// line 2: time-reversed
	// line 3: duplicate
	// line 4: invalid
	// uniqueness is guaranteed among all accepted records
}
