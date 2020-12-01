package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/jrhy/mast"
)

type Tree struct {
	Config       Config
	Mast         *mast.Mast
	Created      *time.Time
	Source       *string
	MergeSources []string
}

type Root struct {
	mast.Root
	Created      *time.Time `json:"cr,omitempty"`
	MergeSources []string   `json:"p,omitempty"`
}

type Value struct {
	ModEpochNanos            int64       `json:"m"`
	PreviousRoot             string      `json:"p"`
	TombstoneSinceEpochNanos int64       `json:"d"`
	Value                    interface{} `json:"v"`
}

func mergeTrees(ctx context.Context, primary *mast.Mast, grafts ...*mast.Mast) (*mast.Mast, error) {
	if len(grafts) == 0 {
		return primary, nil
	}
	newTree, err := primary.Clone(ctx)
	if err != nil {
		return nil, fmt.Errorf("clone: %w", err)
	}

	for _, graft := range grafts {
		err = newTree.DiffIter(ctx, graft, func(added bool, removed bool, key interface{}, addedValue interface{}, removedValue interface{}) (bool, error) {
			var newValue interface{}
			if !added && !removed {
				newValue = LastWriteWins(addedValue.(Value), removedValue.(Value))
			} else if added {
				newValue = addedValue
			} else if removed {
				newValue = removedValue
			} else {
				return false, fmt.Errorf("no added/removed value")
			}
			err := newTree.Insert(ctx, key, newValue)
			if err != nil {
				return false, fmt.Errorf("insert: %w", err)
			}
			return true, nil
		})
		if err != nil {
			return nil, fmt.Errorf("DiffIter: %w", err)
		}
	}
	return &newTree, nil
}

func LastWriteWins(newValue, oldValue Value) Value {
	if newValue.tombstoned() || oldValue.tombstoned() {
		return firstTombstoneWins(newValue, oldValue)
	}
	if newValue.ModEpochNanos > oldValue.ModEpochNanos {
		return newValue
	}
	return oldValue
}

func (v Value) tombstoned() bool {
	return v.TombstoneSinceEpochNanos != 0
}

func firstTombstoneWins(newValue, oldValue Value) Value {
	if !newValue.tombstoned() {
		return oldValue
	}
	if !oldValue.tombstoned() {
		return newValue
	}
	if newValue.TombstoneSinceEpochNanos < oldValue.TombstoneSinceEpochNanos {
		return newValue
	}
	return oldValue
}

type Config struct {
	KeysLike                       interface{}
	ValuesLike                     interface{}
	StoreImmutablePartsWith        mast.Persist
	NodeCache                      mast.NodeCache
	Marshal                        func(interface{}) ([]byte, error)
	Unmarshal                      func([]byte, interface{}) error
	UnmarshalerUsesRegisteredTypes bool
}

func NewRoot(when time.Time, branchFactor uint) Root {
	return Root{
		Root: *mast.NewRoot(&mast.CreateRemoteOptions{
			BranchFactor: branchFactor,
		}),
		Created: &when,
	}
}

func emptyValue(cfg Config) Value {
	if cfg.ValuesLike == nil {
		return Value{}
	}
	aType := reflect.TypeOf(cfg.ValuesLike)
	aCopy := reflect.New(aType)
	return Value{Value: aCopy}
}

func unmarshal(bytes []byte, i interface{}, cfg Config) error {
	ucb := cfg.Unmarshal
	if ucb == nil {
		ucb = json.Unmarshal
	}
	cv, ok := i.(*Value)
	if !ok {
		return ucb(bytes, i)
	}
	var jv struct {
		ModEpochNanos            int64           `json:"m"`
		TombstoneSinceEpochNanos int64           `json:"t,omitempty"`
		Value                    json.RawMessage `json:"v,omitempty"`
	}
	err := ucb(bytes, &jv)
	if err != nil {
		return fmt.Errorf("unmarshal crdtValue message: %w", err)
	}

	if jv.Value != nil {
		aType := reflect.TypeOf(cfg.ValuesLike)
		aCopy := reflect.New(aType)
		err = json.Unmarshal(jv.Value, aCopy.Interface())
		if err != nil {
			return fmt.Errorf("unmarshal crdtValue: %w", err)
		}
		cv.Value = aCopy.Elem().Interface()
	} else if jv.TombstoneSinceEpochNanos == 0 {
		return fmt.Errorf("nil value for nondeleted entry")
	}

	cv.ModEpochNanos = jv.ModEpochNanos
	cv.TombstoneSinceEpochNanos = jv.TombstoneSinceEpochNanos
	return nil
}

func Load(ctx context.Context, cfg Config, rootName *string, root Root) (*Tree, error) {
	mastCfg := mast.RemoteConfig{
		KeysLike: cfg.KeysLike,
		ValuesLike: Value{
			Value: cfg.ValuesLike,
		},
		StoreImmutablePartsWith: cfg.StoreImmutablePartsWith,
		NodeCache:               cfg.NodeCache,
		Marshal:                 cfg.Marshal,
		Unmarshal: func(bytes []byte, i interface{}) error {
			return unmarshal(bytes, i, cfg)
		},
		UnmarshalerUsesRegisteredTypes: cfg.UnmarshalerUsesRegisteredTypes,
	}
	m, err := root.Root.LoadMast(ctx, &mastCfg)
	if err != nil {
		return nil, fmt.Errorf("load new root: %w", err)
	}
	return &Tree{
		cfg,
		m,
		root.Created,
		rootName,
		root.MergeSources,
	}, nil
}

func (c *Tree) MakeRoot(ctx context.Context) (*Root, error) {
	mastRoot, err := c.Mast.MakeRoot(ctx)
	if err != nil {
		return nil, err
	}
	crdtRoot := Root{
		Root:         *mastRoot,
		Created:      c.Created,
		MergeSources: c.MergeSources,
	}
	return &crdtRoot, nil
}

func (c *Tree) Merge(ctx context.Context, other *Tree) error {
	m, err := mergeTrees(ctx, c.Mast, other.Mast)
	if err != nil {
		return fmt.Errorf("mergeTrees: %w", err)
	}
	c.Mast = m
	if other.Source != nil {
		if c.MergeSources == nil {
			c.MergeSources = []string{*other.Source}
		} else {
			c.MergeSources = append(c.MergeSources, *other.Source)
		}
	}
	return nil
}

func (c *Tree) Tombstone(ctx context.Context, when time.Time, key interface{}) error {
	n := when.UnixNano()
	return c.update(ctx, when, key,
		Value{
			ModEpochNanos:            n,
			TombstoneSinceEpochNanos: n,
		},
	)
}

func (c *Tree) IsTombstoned(ctx context.Context, key interface{}) (bool, error) {
	cv := emptyValue(c.Config)
	contains, err := c.Mast.Get(ctx, key, &cv)
	if err != nil || !contains {
		return false, err
	}
	return cv.TombstoneSinceEpochNanos != 0, nil
}

func (c *Tree) Set(ctx context.Context, when time.Time, key, value interface{}) error {
	return c.update(ctx, when, key,
		Value{
			ModEpochNanos: when.UnixNano(),
			Value:         value,
		},
	)
}

func (c *Tree) update(ctx context.Context, when time.Time, key interface{}, cv Value) error {
	existing := emptyValue(c.Config)
	contains, err := c.Mast.Get(ctx, key, &existing)
	if err != nil {
		return fmt.Errorf("get existing: %w", err)
	}
	if contains {
		winner := LastWriteWins(cv, existing)
		if winner != existing {
			if c.Source == nil {
				return fmt.Errorf("expected Source to be set")
			}
			winner.PreviousRoot = *c.Source
		}
		err = c.Mast.Insert(ctx, key, winner)
	} else {
		err = c.Mast.Insert(ctx, key, cv)
	}
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

func (c *Tree) Get(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	cv := emptyValue(c.Config)
	contains, err := c.Mast.Get(ctx, key, &cv)
	if err != nil || !contains {
		return false, err
	}
	if cv.TombstoneSinceEpochNanos > 0 {
		return false, nil
	}
	reflect.ValueOf(value).Elem().Set(reflect.ValueOf(cv.Value))
	return true, nil
}

func (c *Tree) Size() uint64 {
	return c.Mast.Size()
}

func (c Tree) Clone(ctx context.Context) (*Tree, error) {
	clone := c
	clonedMast, err := c.Mast.Clone(ctx)
	if err != nil {
		return nil, err
	}
	clone.Mast = &clonedMast
	return &clone, nil
}

func (c Tree) IsDirty() bool {
	return c.Mast.IsDirty()
}
