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

func (td treeDiffer) Diff(ctx context.Context, cb DiffFn) (err error) {
	for td.from.valid() && td.to.valid() {
		from := td.from.currentPair()
		to := td.to.currentPair()

		cmp := td.cmp(from.key(), to.key())
		switch {
		case cmp < 0:
			err = sendRemoved(ctx, td.from, cb)

		case cmp > 0:
			err = sendAdded(ctx, td.to, cb)

		case cmp == 0:
			if !equalValues(from, to) {
				err = sendModified(ctx, td.from, td.to, cb)
			} else {
				err = skipCommon(ctx, td.from, td.to)
			}
		}
		if err != nil {
			return err
		}
	}

	for td.from.valid() {
		if err = sendRemoved(ctx, td.from, cb); err != nil {
			return err
		}
	}

	for td.to.valid() {
		if err = sendAdded(ctx, td.to, cb); err != nil {
			return err
		}
	}

	return
}

func sendRemoved(ctx context.Context, from *nodeCursor, cb DiffFn) (err error) {
	p := from.currentPair()
	d := Diff{
		Type: RemovedDiff,
		Key:  val.Tuple(p.key()),
		From: val.Tuple(p.value()),
	}

	if err = cb(ctx, d); err != nil {
		return err
	}
	if _, err = from.advance(ctx); err != nil {
		return err
	}
	return
}

func sendAdded(ctx context.Context, to *nodeCursor, cb DiffFn) (err error) {
	p := to.currentPair()
	d := Diff{
		Type: AddedDiff,
		Key:  val.Tuple(p.key()),
		To:   val.Tuple(p.value()),
	}

	if err = cb(ctx, d); err != nil {
		return err
	}
	if _, err = to.advance(ctx); err != nil {
		return err
	}
	return
}

func sendModified(ctx context.Context, from, to *nodeCursor, cb DiffFn) (err error) {
	fp := from.currentPair()
	tp := to.currentPair()
	d := Diff{
		Type: ModifiedDiff,
		Key:  val.Tuple(fp.key()),
		From: val.Tuple(fp.value()),
		To:   val.Tuple(tp.value()),
	}

	if err = cb(ctx, d); err != nil {
		return err
	}
	if _, err = from.advance(ctx); err != nil {
		return err
	}
	if _, err = to.advance(ctx); err != nil {
		return err
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
