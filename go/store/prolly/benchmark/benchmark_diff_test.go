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

package benchmark

import (
	"context"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func BenchmarkProllyDiff(b *testing.B) {
	benchmarkProllyMapDiff(b, 100_000, 100)
}

func BenchmarkTypesDiff(b *testing.B) {
	benchmarkTypesMapDiff(b, 100_000, 100)
}

func benchmarkProllyMapDiff(b *testing.B, size, revs uint64) {
	bench := generateProllyDiffBench(b, size, revs)
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("benchmark prolly map", func(b *testing.B) {
		ctx := context.Background()
		counter := 0
		for i, from := range bench.revisions {
			for j, to := range bench.revisions {
				if i == j {
					continue
				}

				err := prolly.DiffMaps(ctx, from, to, func(ctx context.Context, diff prolly.Diff) error {
					counter++
					if diff.Type == prolly.ModifiedDiff {
						panic("incorrect diff type")
					}
					return nil
				})
				require.NoError(b, err)
			}
		}
		assert.True(b, counter > 0)
	})
}

type prollyDiffBench struct {
	revisions []prolly.Map
	mutations [][2]val.Tuple
}

func generateProllyDiffBench(t *testing.B, size, revisions uint64) (d prollyDiffBench) {
	orig := generateProllyBench(size)

	rand.Shuffle(len(orig.tups), func(i, j int) {
		orig.tups[i], orig.tups[j] = orig.tups[j], orig.tups[i]
	})

	d.mutations = orig.tups[:revisions]
	d.revisions = make([]prolly.Map, revisions)

	ctx := context.Background()
	for i, pair := range d.mutations {
		mut := orig.m.Mutate()
		err := mut.Put(ctx, pair[0], nil)
		require.NoError(t, err)
		rev, err := mut.Map(ctx)
		d.revisions[i] = rev
	}

	return d
}


func benchmarkTypesMapDiff(b *testing.B, size, revs uint64) {
	bench := generateTypesDiffBench(b, size, revs)
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("benchmark prolly map", func(b *testing.B) {
		ctx := context.Background()
		counter := 0

		for i, from := range bench.revisions {
			for j, to := range bench.revisions {
				if i == j {
					continue
				}

				diffs := make(chan types.ValueChanged, 8)
				go func() {
					defer close(diffs)
					err := from.Diff(ctx, to, diffs)
					require.NoError(b, err)
				}()

				var d types.ValueChanged
				ok := true
				for ok {
					select {
					case d, ok = <- diffs:
						if ok {
							counter++
						}
						if d.ChangeType == types.DiffChangeModified {
							panic("incorrect diff type")
						}
					}
				}
			}
		}
		assert.True(b, counter > 0)
	})
}

type typesDiffBench struct {
	revisions []types.Map
	mutations [][2]types.Tuple
}

func generateTypesDiffBench(t *testing.B, size, revisions uint64) (d typesDiffBench) {
	orig := generateTypesBench(size)

	rand.Shuffle(len(orig.tups), func(i, j int) {
		orig.tups[i], orig.tups[j] = orig.tups[j], orig.tups[i]
	})

	d.mutations = orig.tups[:revisions]
	d.revisions = make([]types.Map, revisions)

	ctx := context.Background()
	for i, pair := range d.mutations {
		rev, err := orig.m.Edit().Remove(pair[0]).Map(ctx)
		require.NoError(t, err)
		d.revisions[i] = rev
	}

	return d
}

