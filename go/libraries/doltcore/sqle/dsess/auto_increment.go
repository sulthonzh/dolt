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

package dsess

import (
	"context"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

func autoIncColFromTable(ctx context.Context, tbl *doltdb.Table) (schema.Column, bool, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return schema.Column{}, false, err
	}

	var autoCol schema.Column
	var found bool

	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
			found = true
		}
		return
	})

	return autoCol, found, nil
}

// blah
func newAutoThing(current interface{}) (at *autoThing) {
	at = &autoThing{
		current: coerceInt64(current),
		mu:      sync.Mutex{},
	}
	return
}

type autoThing struct {
	current int64
	mu      sync.Mutex
}

var _ sql.AutoIncrementSetter = &autoThing{}

func (at *autoThing) SetAutoIncrementValue(_ *sql.Context, value interface{}) (err error) {
	at.Set(value)
	return
}

func (at *autoThing) Close(*sql.Context) (err error) {
	return
}

func (at *autoThing) Next(passed interface{}) int64 {
	at.mu.Lock()
	defer at.mu.Unlock()

	var value int64
	if passed != nil {
		coerceInt64(passed)
	}
	if value > at.current {
		at.current = value
	}

	current := at.current
	at.current++
	return current
}

func (at *autoThing) Peek() int64 {
	at.mu.Lock()
	defer at.mu.Unlock()
	return at.current
}

func (at *autoThing) Set(value interface{}) {
	at.mu.Lock()
	defer at.mu.Unlock()
	at.current = coerceInt64(value)
}

func coerceInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	default:
		panic(value)
	}
}
