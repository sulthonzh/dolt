// Copyright 2019 Dolthub, Inc.
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

package merge

import (
	"context"
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdocs"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrFastForward = errors.New("fast forward")
var ErrSameTblAddedTwice = errors.New("table with same name added in 2 commits can't be merged")
var ErrTableDeletedAndModified = errors.New("conflict: table with same name deleted and modified ")

type Merger struct {
	root      *doltdb.RootValue
	mergeRoot *doltdb.RootValue
	ancRoot   *doltdb.RootValue
	vrw       types.ValueReadWriter
}

// NewMerger creates a new merger utility object.
func NewMerger(ctx context.Context, root, mergeRoot, ancRoot *doltdb.RootValue, vrw types.ValueReadWriter) *Merger {
	return &Merger{root, mergeRoot, ancRoot, vrw}
}

func MergeCommits(ctx context.Context, commit, mergeCommit *doltdb.Commit, opts editor.Options) (*doltdb.RootValue, map[string]*MergeStats, error) {
	ancCommit, err := doltdb.GetCommitAncestor(ctx, commit, mergeCommit)
	if err != nil {
		return nil, nil, err
	}

	ourRoot, err := commit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	theirRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	ancRoot, err := ancCommit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	return MergeRoots(ctx, ourRoot, theirRoot, ancRoot, opts)
}

func MergeRoots(ctx context.Context, ourRoot, theirRoot, ancRoot *doltdb.RootValue, opts editor.Options) (*doltdb.RootValue, map[string]*MergeStats, error) {
	tables, err := ourRoot.GetTableNames(ctx)
	if err != nil {
		return nil, nil, err
	}

	stats := make(map[string]*MergeStats)

	mergedRoot := ourRoot
	for _, name := range tables {
		o, t, a, err := getTables(ctx, name, ourRoot, theirRoot, ancRoot)
		if err != nil {
			return nil, nil, err
		}

		merged, err := MergeTable(ctx, o, t, a)
		if err != nil {
			return nil, nil, err
		}

		mergedRoot, err = mergedRoot.PutTable(ctx, name, merged)
		if err != nil {
			return nil, nil, err
		}
	}

	return mergedRoot, stats, nil
}

func MergeTable(ctx context.Context, ours, theirs, anc *doltdb.Table) (*doltdb.Table, error) {
	o, err := ours.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	t, err := theirs.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	a, err := anc.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	var cb prolly.TupleMergeFn
	merged, err := prolly.ThreeWayMerge(ctx, o, t, a, cb)
	if err != nil {
		return nil, err
	}

	return ours.UpdateRows(ctx, merged)
}

func getTables(ctx context.Context, name string, ourRoot, theirRoot, ancRoot *doltdb.RootValue) (ours, theirs, anc *doltdb.Table, err error) {
	var ok bool
	ours, ok, err = ourRoot.GetTable(ctx, name)
	if !ok {
		panic("table missing")
	}
	if err != nil {
		return nil, nil, nil, err
	}

	theirs, ok, err = theirRoot.GetTable(ctx, name)
	if !ok {
		panic("table missing")
	}
	if err != nil {
		return nil, nil, nil, err
	}

	anc, ok, err = ancRoot.GetTable(ctx, name)
	if !ok {
		panic("table missing")
	}
	if err != nil {
		return nil, nil, nil, err
	}

	return
}

// MayHaveConstraintViolations returns whether the given roots may have constraint violations. For example, a fast
// forward merge that does not involve any tables with foreign key constraints or check constraints will not be able
// to generate constraint violations. Unique key constraint violations would be caught during the generation of the
// merged root, therefore it is not a factor for this function.
func MayHaveConstraintViolations(ctx context.Context, ancestor, merged *doltdb.RootValue) (bool, error) {
	ancTables, err := ancestor.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	mergedTables, err := merged.MapTableHashes(ctx)
	if err != nil {
		return false, err
	}
	fkColl, err := merged.GetForeignKeyCollection(ctx)
	if err != nil {
		return false, err
	}
	tablesInFks := fkColl.Tables()
	for tblName := range tablesInFks {
		if ancHash, ok := ancTables[tblName]; !ok {
			// If a table used in a foreign key is new then it's treated as a change
			return true, nil
		} else if mergedHash, ok := mergedTables[tblName]; !ok {
			return false, fmt.Errorf("foreign key uses table '%s' but no hash can be found for this table", tblName)
		} else if !ancHash.Equal(mergedHash) {
			return true, nil
		}
	}
	return false, nil
}

func GetTablesInConflict(ctx context.Context, roots doltdb.Roots) (
	workingInConflict, stagedInConflict, headInConflict []string,
	err error,
) {
	headInConflict, err = roots.Head.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedInConflict, err = roots.Staged.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingInConflict, err = roots.Working.TablesInConflict(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return workingInConflict, stagedInConflict, headInConflict, err
}

func GetTablesWithConstraintViolations(ctx context.Context, roots doltdb.Roots) (
	workingViolations, stagedViolations, headViolations []string,
	err error,
) {
	headViolations, err = roots.Head.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	stagedViolations, err = roots.Staged.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	workingViolations, err = roots.Working.TablesWithConstraintViolations(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return workingViolations, stagedViolations, headViolations, err
}

func GetDocsInConflict(ctx context.Context, workingRoot *doltdb.RootValue, docs doltdocs.Docs) (*diff.DocDiffs, error) {
	return diff.NewDocDiffs(ctx, workingRoot, nil, docs)
}

func MergeWouldStompChanges(ctx context.Context, roots doltdb.Roots, mergeCommit *doltdb.Commit) ([]string, map[string]hash.Hash, error) {
	mergeRoot, err := mergeCommit.GetRootValue()
	if err != nil {
		return nil, nil, err
	}

	headTableHashes, err := roots.Head.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	workingTableHashes, err := roots.Working.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	mergeTableHashes, err := mergeRoot.MapTableHashes(ctx)
	if err != nil {
		return nil, nil, err
	}

	headWorkingDiffs := diffTableHashes(headTableHashes, workingTableHashes)
	mergedHeadDiffs := diffTableHashes(headTableHashes, mergeTableHashes)

	stompedTables := make([]string, 0, len(headWorkingDiffs))
	for tName, _ := range headWorkingDiffs {
		if _, ok := mergedHeadDiffs[tName]; ok {
			// even if the working changes match the merge changes, don't allow (matches git behavior).
			stompedTables = append(stompedTables, tName)
		}
	}

	return stompedTables, headWorkingDiffs, nil
}

func diffTableHashes(headTableHashes, otherTableHashes map[string]hash.Hash) map[string]hash.Hash {
	diffs := make(map[string]hash.Hash)
	for tName, hh := range headTableHashes {
		if h, ok := otherTableHashes[tName]; ok {
			if h != hh {
				// modification
				diffs[tName] = h
			}
		} else {
			// deletion
			diffs[tName] = hash.Hash{}
		}
	}

	for tName, h := range otherTableHashes {
		if _, ok := headTableHashes[tName]; !ok {
			// addition
			diffs[tName] = h
		}
	}

	return diffs
}
