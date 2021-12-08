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
	"bytes"
	"context"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/val"
)

const patchBufferSize = 1024

type ConflictFn func(left, right Diff) (Diff, bool)

type patch [2]val.Tuple

func ThreeWayMerge(ctx context.Context, base, left, right Map, cb ConflictFn) (Map, error) {
	ld, err := treeDifferFromMaps(ctx, base, left)
	if err != nil {
		return Map{}, err
	}

	rd, err := treeDifferFromMaps(ctx, base, right)
	if err != nil {
		return Map{}, err
	}

	var finished Map
	buf := make(chan patch, patchBufferSize)
	src := patchSource{src: buf}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(buf)
		return sendPatches(ctx, ld, rd, buf, cb)
	})
	eg.Go(func() (err error) {
		finished, err = materializeMutations(ctx, left, src)
		return err
	})

	if err = eg.Wait(); err != nil {
		return Map{}, err
	}

	return finished, nil
}

func sendPatches(ctx context.Context, l, r treeDiffer, sink chan<- patch, conflict ConflictFn) (err error) {
	var (
		left, right Diff
		lok, rok    = true, true
	)

	left, err = l.Next(ctx)
	if err == io.EOF {
		err = nil
		lok = false
	}
	if err != nil {
		return err
	}

	right, err = r.Next(ctx)
	if err == io.EOF {
		err = nil
		rok = false
	}
	if err != nil {
		return err
	}

	for lok && rok {
		cmp := compareDiffKeys(left, right, l.cmp)

		switch {
		case cmp < 0:
			// already in left
			left, err = l.Next(ctx)
			if err == io.EOF {
				err, lok = nil, false
			}
			if err != nil {
				return err
			}

		case cmp > 0:
			err = sendPatch(ctx, right, sink)
			if err != nil {
				return err
			}

			right, err = r.Next(ctx)
			if err == io.EOF {
				err, rok = nil, false
			}
			if err != nil {
				return err
			}

		case cmp == 0:
			if equalDiffVals(left, right) {
				// already in left
				continue
			}

			resolved, ok := conflict(left, right)
			if ok {
				err = sendPatch(ctx, resolved, sink)
			}
			if err != nil {
				return err
			}
		}
	}

	for lok {
		// already in left
		break
	}

	for rok {
		err = sendPatch(ctx, right, sink)
		if err != nil {
			return err
		}

		right, err = r.Next(ctx)
		if err == io.EOF {
			err, rok = nil, false
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func sendPatch(ctx context.Context, diff Diff, sink chan<- patch) error {
	var p patch
	switch diff.Type {
	case AddedDiff:
		p[0], p[1] = diff.Key, diff.To
	case ModifiedDiff:
		p[0], p[1] = diff.Key, diff.To
	case RemovedDiff:
		p[0], p[1] = diff.Key, nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case sink <- p:
	}

	return nil
}

func compareDiffKeys(left, right Diff, cmp compareFn) int {
	return cmp(nodeItem(left.Key), nodeItem(right.Key))
}

func equalDiffVals(left, right Diff) bool {
	// todo(andy): bytes must be comparable
	ok := left.Type == right.Type
	return ok && bytes.Equal(left.To, right.To)
}

type patchSource struct {
	src <-chan patch
}

var _ mutationIter = patchSource{}

func (ps patchSource) nextMutation(ctx context.Context) (val.Tuple, val.Tuple) {
	var p patch
	select {
	case p = <-ps.src:
		return p[0], p[1]
	case <-ctx.Done():
		return nil, nil
	}
}

func (ps patchSource) close() error {
	return nil
}
