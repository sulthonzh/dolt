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

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/val"
)

var shimPool = pool.NewBuffPool()

func newRowConverter(tblSch, idxSch schema.Schema) (rc rowConv) {
	kd := prolly.KeyDescriptorFromSchema(idxSch)
	vd := prolly.ValueDescriptorFromSchema(idxSch)

	if !schema.ColCollsAreEqual(tblSch.GetAllCols(), idxSch.GetAllCols()) {
		panic("bad schema")
	}

	rc = rowConv{
		keyBld: val.NewTupleBuilder(kd),
		valBld: val.NewTupleBuilder(vd),
	}

	for i := range idxSch.GetPKCols().GetColumns() {
		rc.keyMap = append(rc.keyMap, i)
	}
	offset := len(rc.keyMap)
	for i := range idxSch.GetNonPKCols().GetColumns() {
		rc.valMap = append(rc.valMap, i+offset)
	}

	return rc
}

type rowConv struct {
	keyMap, valMap []int
	keyBld, valBld *val.TupleBuilder
}

func (rc rowConv) ConvertRow(row sql.Row) (key, value val.Tuple) {
	for i, j := range rc.keyMap {
		rc.keyBld.PutField(i, row[j])
	}
	key = rc.keyBld.Build(shimPool)

	for i, j := range rc.valMap {
		rc.valBld.PutField(i, row[j])
	}
	value = rc.valBld.Build(shimPool)

	return
}
