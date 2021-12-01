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

package index

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/types"
)

func WritersFromTable(ctx *sql.Context, tbl *doltdb.Table) (Writer, map[string]Writer, error) {
	var primary Writer
	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return primary, nil, err
	}

	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return primary, nil, err
	}
	primary = newIndexWriter(tblSch, tblSch, rows)

	indexes, err := tbl.GetIndexData(ctx)
	if err != nil {
		return Writer{}, nil, err
	}
	secondary := make(map[string]Writer, indexes.Len())

	err = indexes.IterAll(ctx, func(key, value types.Value) error {
		vrw := tbl.ValueReadWriter()
		tv, err := value.(types.Ref).TargetValue(ctx, vrw)
		if err != nil {
			return err
		}

		idxName := string(key.(types.String))
		idxSch := tblSch.Indexes().GetByName(idxName).Schema()
		index := prolly.MapFromValue(tv, idxSch, vrw)

		secondary[idxName] = newIndexWriter(idxSch, tblSch, index)
		return nil
	})
	if err != nil {
		return primary, secondary, err
	}

	return primary, nil, nil
}

type Writer struct {
	mut  prolly.MutableMap
	conv rowConv
}

func newIndexWriter(sch, idxSch schema.Schema, rows prolly.Map) (ed Writer) {
	return Writer{
		mut:  rows.Mutate(),
		conv: newRowConverter(sch, idxSch),
	}
}

// StatementBegin implements the interface sql.TableWriter.
func (w Writer) StatementBegin(ctx *sql.Context) {
	return
}

func (w Writer) Insert(ctx *sql.Context, sqlRow sql.Row) (err error) {
	k, v := w.conv.ConvertRow(sqlRow)
	return w.mut.Put(ctx, k, v)
}

func (w Writer) Delete(ctx *sql.Context, sqlRow sql.Row) (err error) {
	k, _ := w.conv.ConvertRow(sqlRow)
	return w.mut.Put(ctx, k, nil)
}

func (w Writer) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) (err error) {
	k, v := w.conv.ConvertRow(newRow)
	return w.mut.Put(ctx, k, v)
}

// DiscardChanges implements the interface sql.TableWriter.
func (w Writer) DiscardChanges(ctx *sql.Context, errorEncountered error) (err error) {
	panic("unimplemented")
}

// StatementComplete implements the interface sql.TableWriter.
func (w Writer) StatementComplete(ctx *sql.Context) (err error) {
	return
}

func (w Writer) Flush(ctx *sql.Context) (prolly.Map, error) {
	return w.mut.Map(ctx)
}

// Close implements Closer
func (w Writer) Close(ctx *sql.Context) (err error) {
	return
}
