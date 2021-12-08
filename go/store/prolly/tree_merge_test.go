// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prolly

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

func Test3WayMapMerge(t *testing.T) {
	scales := []int{
		10,
		100,
		1000,
		10000,
	}

	for _, s := range scales {
		name := fmt.Sprintf("test proCur map at scale %d", s)
		t.Run(name, func(t *testing.T) {
			prollyMap, tuples := makeProllyMap(t, s)

			t.Run("merge identical maps", func(t *testing.T) {
				testEmptyMapMerge(t, prollyMap.(Map))
			})
			t.Run("3way merge inserts", func(t *testing.T) {
				for k := 0; k < 10; k++ {
					testMapMergeInserts(t, prollyMap.(Map), tuples)
				}
			})
		})
	}
}

func testEmptyMapMerge(t *testing.T, m Map) {
	ctx := context.Background()
	mm, err := ThreeWayMerge(ctx, m, m, m, panicOnConflict)
	require.NoError(t, err)
	assert.NotNil(t, mm)
	assert.Equal(t, m.Count(), mm.Count())
}

func testMapMergeInserts(t *testing.T, final Map, tups [][2]val.Tuple) {
	testRand.Shuffle(len(tups), func(i, j int) {
		tups[i], tups[j] = tups[j], tups[i]
	})

	// edit 10%
	sz := final.Count() / 10

	left := deleteKeys(t, final, tups[:sz]...)
	right := deleteKeys(t, final, tups[sz:sz*2]...)
	base := deleteKeys(t, final, tups[:sz*2]...)

	ctx := context.Background()
	final2, err := ThreeWayMerge(ctx, base, left, right, panicOnConflict)
	require.NoError(t, err)

	cnt := 0
	err = DiffMaps(ctx, final, final2, func(ctx context.Context, diff Diff) error {
		cnt++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, cnt)

	compareMaps(t, final, final2)
}

func panicOnConflict(left, right Diff) (Diff, bool) {
	panic("cannot merge cells")
}

func compareMaps(t *testing.T, left, right Map) {
	ctx := context.Background()
	l, err := left.IterAll(ctx)
	require.NoError(t, err)
	r, err := right.IterAll(ctx)
	require.NoError(t, err)

	cnt := uint64(0)
	for {
		lk, lv, err := l.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		rk, rv, err := r.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		assert.Equal(t, lk, rk)
		assert.Equal(t, lv, rv)
		cnt++
	}
	assert.Equal(t, left.Count(), cnt)
	assert.Equal(t, right.Count(), cnt)
}
