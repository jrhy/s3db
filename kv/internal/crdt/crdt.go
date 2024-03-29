package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jrhy/mast"
	"github.com/jrhy/s3db/kv/crdt"
)

type Tree struct {
	Config       Config
	Mast         *mast.Mast
	Created      *time.Time
	Source       *string
	MergeSources []string
	MergeMode    int
}

// Root identifies a tree, a persisted form, that links the mast and
// ancestors.
type Root struct {
	mast.Root
	Created      *time.Time `json:"cr,omitempty"`
	MergeSources []string   `json:"p,omitempty"`
	MergeMode    int        `json:"mm,omitempty"`
	KVVersion    int        `json:"kv_version,omitempty"`
}

const (
	MergeModeLWW = iota
	MergeModeCustom
	MergeModeCustomLWW
)

func mergeTrees(ctx context.Context, mergeFunc MergeFunc, conflictCB OnConflictMerged, primary *mast.Mast, grafts ...*mast.Mast) (*mast.Mast, error) {
	if len(grafts) == 0 {
		return primary, nil
	}
	newTree, err := primary.Clone(ctx)
	if err != nil {
		return nil, fmt.Errorf("clone: %w", err)
	}

	for _, graft := range grafts {
		err = newTree.DiffIter(ctx, graft, mergeFunc.ToDiffFunc(ctx, &newTree, conflictCB))
		if err != nil {
			return nil, err
		}
	}
	return &newTree, nil
}

type MergeFunc func(context.Context, *mast.Mast, bool, bool, interface{}, interface{}, interface{},
	OnConflictMerged) (bool, error)

type MergeError error

func (mf MergeFunc) ToDiffFunc(ctx context.Context, m *mast.Mast, conflictCB OnConflictMerged) func(added, removed bool,
	key, addedValue, removedValue interface{},
) (bool, error) {
	return func(added, removed bool, key, addedValue, removedValue interface{}) (bool, error) {
		ok, err := mf(ctx, m, added, removed, key, addedValue, removedValue, conflictCB)
		if err != nil {
			err = MergeError(err)
		}
		return ok, err
	}
}

var LWW MergeFunc = MergeFunc(
	func(ctx context.Context, newTree *mast.Mast, /*, conflicts *uint64*/
		added, removed bool, key, addedValue, removedValue interface{},
		onConflictMerged OnConflictMerged) (bool, error) {
		var newValue crdt.Value
		if !added && !removed { // changed
			av := addedValue.(crdt.Value)
			rv := removedValue.(crdt.Value)
			newValue = *crdt.LastWriteWins(&av, &rv)
			if onConflictMerged != nil && !av.Tombstoned() && !rv.Tombstoned() &&
				!reflect.DeepEqual(av.Value, rv.Value) {
				err := onConflictMerged(key, av.Value, rv.Value)
				if err != nil {
					return false, fmt.Errorf("OnConflictMerged: %w", err)
				}
			}
		} else if added {
			// already present
			return true, nil
		} else if removed {
			newValue = removedValue.(crdt.Value)
		} else {
			return false, fmt.Errorf("no added/removed value")
		}
		err := newTree.Insert(ctx, key, newValue)
		if err != nil {
			return false, fmt.Errorf("insert: %w", err)
		}
		return true, nil
	})

type Config struct {
	KeysLike                       interface{}
	ValuesLike                     interface{}
	StoreImmutablePartsWith        mast.Persist
	NodeCache                      mast.NodeCache
	Marshal                        func(interface{}) ([]byte, error)
	Unmarshal                      func([]byte, interface{}) error
	UnmarshalerUsesRegisteredTypes bool
	CustomMerge                    func(key interface{}, v1, v2 crdt.Value) crdt.Value
	OnConflictMerged
	MastNodeFormat string
}

type OnConflictMerged func(key, v1, v2 interface{}) error

func NewRoot(when time.Time, branchFactor uint) Root {
	return Root{
		Root: *mast.NewRoot(&mast.CreateRemoteOptions{
			BranchFactor: branchFactor,
		}),
		Created: &when,
	}
}

func emptyValue(cfg Config) crdt.Value {
	if cfg.ValuesLike == nil {
		return crdt.Value{}
	}
	aType := reflect.TypeOf(cfg.ValuesLike)
	aCopy := reflect.New(aType)
	return crdt.Value{Value: aCopy}
}

func unmarshal(bytes []byte, i interface{}, cfg Config) error {
	ucb := cfg.Unmarshal
	if ucb == nil {
		ucb = json.Unmarshal
	}
	if cfg.UnmarshalerUsesRegisteredTypes {
		return cfg.Unmarshal(bytes, i)
	}
	cv, ok := i.(*crdt.Value)
	if !ok {
		return ucb(bytes, i)
	}
	var jv struct {
		ModEpochNanos            int64           `json:"m"`
		TombstoneSinceEpochNanos int64           `json:"d,omitempty"`
		Value                    json.RawMessage `json:"v,omitempty"`
		PreviousRoot             string          `json:"p,omitempty"`
	}
	err := ucb(bytes, &jv)
	if err != nil {
		return fmt.Errorf("unmarshal crdtValue message: %w", err)
	}

	if len(jv.Value) != 0 {
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
	cv.PreviousRoot = jv.PreviousRoot
	return nil
}

func Load(ctx context.Context, cfg Config, rootName *string, root Root) (*Tree, error) {
	if !cfg.UnmarshalerUsesRegisteredTypes && cfg.ValuesLike == nil {
		return nil, errors.New("must set cfg.{Keys,Values}Like or use a marshaler which registers types")
	}
	mastCfg := mast.RemoteConfig{
		KeysLike: cfg.KeysLike,
		ValuesLike: crdt.Value{
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
	switch root.MergeMode {
	case MergeModeLWW:
		if cfg.CustomMerge != nil {
			return nil, errors.New("config.CustomMergeValueOnly conflicts with MergeModeLWW")
		}
		if cfg.OnConflictMerged != nil {
			return nil, errors.New("config.OnConflictMerged handler conflicts with MergeModeLWW")
		}
	case MergeModeCustom:
		if cfg.CustomMerge == nil {
			return nil, errors.New("MergeModeCustom requires config.CustomMergeValueOnly")
		}
		if cfg.OnConflictMerged != nil {
			return nil, errors.New("config.OnConflictMerged handler conflicts with MergeModeCustom")
		}
	case MergeModeCustomLWW:
		if cfg.OnConflictMerged == nil {
			return nil, errors.New("MergeModeCustomLWW requires config.OnConflictMerged")
		}
		if cfg.CustomMerge != nil {
			return nil, errors.New("config.CustomMergeValueOnly handler conflicts with MergeModeCustomLWW")
		}
	}
	return &Tree{
		cfg,
		m,
		root.Created,
		rootName,
		root.MergeSources,
		root.MergeMode,
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
		MergeMode:    c.MergeMode,
	}
	return &crdtRoot, nil
}

func convertMergeFunc(cb func(key interface{}, v1, v2 crdt.Value) crdt.Value) MergeFunc {
	return MergeFunc(func(ctx context.Context, newTree *mast.Mast,
		added, removed bool, key, addedValue, removedValue interface{},
		onConflictMerged OnConflictMerged) (bool, error) {
		var newValue crdt.Value
		if !added && !removed { // changed
			av := addedValue.(crdt.Value)
			rv := removedValue.(crdt.Value)
			newValue = av
			newValue = cb(key, av, rv)
			if onConflictMerged != nil && !av.Tombstoned() && !rv.Tombstoned() &&
				!reflect.DeepEqual(av.Value, rv.Value) {
				err := onConflictMerged(key, av.Value, rv.Value)
				if err != nil {
					return false, fmt.Errorf("OnConflictMerged: %w", err)
				}
			}
		} else if added {
			// already present
			return true, nil
		} else if removed {
			newValue = removedValue.(crdt.Value)
		} else {
			return false, fmt.Errorf("no added/removed value")
		}
		err := newTree.Insert(ctx, key, newValue)
		if err != nil {
			return false, fmt.Errorf("insert: %w", err)
		}
		return true, nil
	})

}

func (c *Tree) Merge(ctx context.Context, other *Tree) error {
	if c.MergeMode != other.MergeMode {
		return fmt.Errorf("incoming graft has different MergeMode %d than local %d", other.MergeMode, c.MergeMode)
	}
	var mergeFunc MergeFunc
	if c.MergeMode == MergeModeCustom {
		mergeFunc = convertMergeFunc(c.Config.CustomMerge)
	} else {
		mergeFunc = LWW
	}
	m, err := mergeTrees(ctx, mergeFunc, c.Config.OnConflictMerged, c.Mast, other.Mast)
	if err != nil {
		return err
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
		crdt.Value{
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
		crdt.Value{
			ModEpochNanos: when.UnixNano(),
			Value:         value,
		},
	)
}

func (c *Tree) update(ctx context.Context, when time.Time, key interface{}, cv crdt.Value) error {
	existing := emptyValue(c.Config)
	contains, err := c.Mast.Get(ctx, key, &existing)
	if err != nil {
		return fmt.Errorf("get existing: %w", err)
	}
	if contains {
		wa := crdt.LastWriteWins(&cv, &existing)
		winner := *wa
		if wa != &existing {
			if c.Source != nil {
				winner.PreviousRoot = *c.Source
			} else {
				winner.PreviousRoot = ""
			}
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
	if cvp, ok := value.(*crdt.Value); ok {
		*cvp = cv
		return true, nil
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
