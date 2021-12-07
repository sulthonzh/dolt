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

	"github.com/dolthub/dolt/go/store/val"
)

type DiffType byte

const (
	AddedDiff    DiffType = 0
	ModifiedDiff DiffType = 1
	RemovedDiff  DiffType = 2
)

type Diff struct {
	Type     DiffType
	Key      val.Tuple
	From, To val.Tuple
}

type DiffFn func(context.Context, Diff) error

type treeDiffer struct {
	from, to *nodeCursor
	cmp      compareFn
}

func (td treeDiffer) Next(ctx context.Context) (diff Diff, err error) {
	for td.from.valid() && td.to.valid() {

		f := td.from.currentPair()
		t := td.to.currentPair()
		cmp := td.cmp(f.key(), t.key())
		
		switch {
		case cmp < 0:
			return sendRemoved(ctx, td.from)

		case cmp > 0:
			return sendAdded(ctx, td.to)

		case cmp == 0 && !equalValues(f, t):
			return sendModified(ctx, td.from, td.to)

		case cmp == 0 && equalValues(f, t):
			// seek ahead to the next diff and loop again
			if err = skipCommon(ctx, td.from, td.to); err != nil {
				return Diff{}, err
			}
		}
	}

	if td.from.valid() {
		return sendRemoved(ctx, td.from)
	}

	if td.to.valid() {
		return sendAdded(ctx, td.to)
	}

	return Diff{}, io.EOF
}

func sendRemoved(ctx context.Context, from *nodeCursor) (diff Diff, err error) {
	pair := from.currentPair()
	diff = Diff{
		Type: RemovedDiff,
		Key:  val.Tuple(pair.key()),
		From: val.Tuple(pair.value()),
	}

	if _, err = from.advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendAdded(ctx context.Context, to *nodeCursor) (diff Diff, err error) {
	pair := to.currentPair()
	diff = Diff{
		Type: AddedDiff,
		Key:  val.Tuple(pair.key()),
		To:   val.Tuple(pair.value()),
	}

	if _, err = to.advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func sendModified(ctx context.Context, from, to *nodeCursor) (diff Diff, err error) {
	fromPair := from.currentPair()
	toPair := to.currentPair()
	diff = Diff{
		Type: ModifiedDiff,
		Key:  val.Tuple(fromPair.key()),
		From: val.Tuple(fromPair.value()),
		To:   val.Tuple(toPair.value()),
	}

	if _, err = from.advance(ctx); err != nil {
		return Diff{}, err
	}
	if _, err = to.advance(ctx); err != nil {
		return Diff{}, err
	}
	return
}

func skipCommon(ctx context.Context, from, to *nodeCursor) (err error) {
	for from.valid() && to.valid() {
		if !equalItems(from, to) {
			// found the next difference
			return nil
		}

		var equalParents bool
		if from.parent != nil && to.parent != nil {
			// todo(andy): we compare here every loop
			equalParents = equalItems(from.parent, to.parent)
		}
		if equalParents {
			// if our parents are equal, we can search for differences
			// faster at the next highest tree level.
			if err = skipCommonParents(ctx, from, to); err != nil {
				return err
			}
			continue
		}

		// todo(andy): we'd like to not load the next node
		//  here if we know they're equal (parents are equal),
		//  however we can only optimize in the case were both
		//  cursors exhaust their current node at the same time.
		if _, err = from.advance(ctx); err != nil {
			return err
		}
		if _, err = to.advance(ctx); err != nil {
			return err
		}
	}

	return err
}

func skipCommonParents(ctx context.Context, from, to *nodeCursor) (err error) {
	err = skipCommon(ctx, from.parent, to.parent)
	if err != nil {
		return err
	}

	if from.parent.valid() {
		if err = from.fetchNode(ctx); err != nil {
			return err
		}
		from.skipToNodeStart()
	} else {
		from.invalidate()
	}

	if to.parent.valid() {
		if err = to.fetchNode(ctx); err != nil {
			return err
		}
		to.skipToNodeStart()
	} else {
		to.invalidate()
	}

	return
}

func equalItems(from, to *nodeCursor) bool {
	// todo(andy): assumes equal byte representations
	f, t := from.currentPair(), to.currentPair()
	return bytes.Equal(f.key(), t.key()) &&
		bytes.Equal(f.value(), t.value())
}

func equalValues(from, to nodePair) bool {
	return bytes.Equal(from.value(), to.value())
}
