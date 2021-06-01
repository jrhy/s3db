package crdt

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jrhy/mast"
	"github.com/jrhy/mast/persist/file"
)

var ctx = context.Background()

func TestHappyCase(t *testing.T) {
	t.Parallel()
	forEachMarshaler(t, func(t *testing.T, cfg Config) {
		store := mast.NewInMemoryStore()
		emptyRoot := NewRoot(time.Now(), mast.DefaultBranchFactor)
		cfg.KeysLike = 1234
		cfg.ValuesLike = "hi"
		cfg.StoreImmutablePartsWith = store

		c1, err := Load(ctx, cfg, nil, emptyRoot)
		require.NoError(t, err)
		err = c1.Set(ctx, time.Now(), 0, "tree 1 key")
		require.NoError(t, err)
		err = c1.Set(ctx, time.Now(), 1, "first write loses")
		require.NoError(t, err)
		c1Root, err := c1.MakeRoot(ctx)
		require.NoError(t, err)

		c2, err := Load(ctx, cfg, nil, emptyRoot)
		require.NoError(t, err)
		err = c2.Set(ctx, time.Now(), 1, "last write wins")
		require.NoError(t, err)
		err = c2.Set(ctx, time.Now(), 2, "tree 2 key")
		require.NoError(t, err)
		c2Root, err := c2.MakeRoot(ctx)
		require.NoError(t, err)

		c3, err := Load(ctx, cfg, nil, emptyRoot)
		require.NoError(t, err)
		err = c3.Merge(ctx, c1)
		require.NoError(t, err)
		err = c3.Merge(ctx, c2)
		require.NoError(t, err)
		c3Root, err := c3.MakeRoot(ctx)
		require.NoError(t, err)

		c4, err := Load(ctx, cfg, nil, emptyRoot)
		require.NoError(t, err)
		c2, err = Load(ctx, cfg, c2Root.Link, *c2Root)
		require.NoError(t, err)
		err = c4.Merge(ctx, c2)
		require.NoError(t, err)
		c1, err = Load(ctx, cfg, c1Root.Link, *c1Root)
		require.NoError(t, err)
		err = c4.Merge(ctx, c1)
		require.NoError(t, err)
		c4Root, err := c4.MakeRoot(ctx)
		require.NoError(t, err)

		require.Equal(t, *c3Root.Link, *c4Root.Link)

		var v string
		contains, err := c4.Get(ctx, 1, &v)
		require.NoError(t, err)
		require.True(t, contains)
		require.Equal(t, "last write wins", v)

		contains, err = c4.Get(ctx, 999, &v)
		require.NoError(t, err)
		require.False(t, contains)

		contains, err = c4.Get(ctx, 0, &v)
		require.NoError(t, err)
		require.True(t, contains)
		require.Equal(t, "tree 1 key", v)

		contains, err = c4.Get(ctx, 2, &v)
		require.NoError(t, err)
		require.True(t, contains)
		require.Equal(t, "tree 2 key", v)
	})
}

func forEachMarshaler(t *testing.T, f func(*testing.T, Config)) {
	t.Run("json", func(t *testing.T) {
		f(t, Config{})
	})
	t.Run("gob", func(t *testing.T) {
		f(t, Config{
			Marshal:                        marshalGob,
			Unmarshal:                      unmarshalGob,
			UnmarshalerUsesRegisteredTypes: true,
		})
	})
}

func TestStructValues(t *testing.T) {
	t.Parallel()
	type asdf struct {
		I int64
		F float64
	}
	type foo struct {
		Asdf asdf
		Jk   bool
	}
	gob.Register(foo{})
	forEachMarshaler(t, func(t *testing.T, cfg Config) {
		store := mast.NewInMemoryStore()
		emptyRoot := NewRoot(time.Now(), mast.DefaultBranchFactor)
		cfg.KeysLike = 1234
		cfg.ValuesLike = foo{asdf{1, 3.14}, true}
		cfg.StoreImmutablePartsWith = store

		c1, err := Load(ctx, cfg, nil, emptyRoot)
		require.NoError(t, err)
		err = c1.Set(ctx, time.Now(), 0, foo{asdf{1, 3.14}, true})
		require.NoError(t, err)
		c1Root, err := c1.MakeRoot(ctx)
		require.NoError(t, err)
		c1, err = Load(ctx, cfg, c1Root.Link, *c1Root)
		require.NoError(t, err)
		var f foo
		contains, err := c1.Get(ctx, 0, &f)
		require.NoError(t, err)
		require.True(t, contains)
		require.Equal(t, foo{asdf{1, 3.14}, true}, f)
	})
}

func TestFile(t *testing.T) {
	t.Parallel()

	type MyObject struct {
		A string
	}
	gob.Register(MyObject{})

	forEachMarshaler(t, func(t *testing.T, cfg Config) {
		dir, err := ioutil.TempDir("", "s3dbtest")
		require.NoError(t, err)

		persist := file.NewPersistForPath(dir)

		empty := NewRoot(time.Now(), mast.DefaultBranchFactor)
		cfg.KeysLike = "hi"
		cfg.ValuesLike = MyObject{}
		cfg.StoreImmutablePartsWith = persist

		s1, err := Load(ctx, cfg, nil, empty)
		require.NoError(t, err)
		err = s1.Set(ctx, time.Now(), "user1", MyObject{A: "a"})
		require.NoError(t, err)
		_, err = s1.MakeRoot(ctx)
		require.NoError(t, err)
		var v MyObject
		found, err := s1.Get(ctx, "user1", &v)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, MyObject{"a"}, v)

		s2, err := Load(ctx, cfg, nil, empty)
		require.NoError(t, err)
		err = s2.Set(ctx, time.Now(), "user1", MyObject{A: "b"})
		require.NoError(t, err)
		_, err = s2.MakeRoot(ctx)
		require.NoError(t, err)

		err = s1.Merge(ctx, s2)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), s1.Size())
		assert.Equal(t, uint64(1), s2.Size())
		found, err = s1.Get(ctx, "user1", &v)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, MyObject{"b"}, v)
	})
}

func TestCustomMergeFunc(t *testing.T) {
	t.Parallel()
	var CountConflicts MergeFunc = MergeFunc(
		func(ctx context.Context, newTree *mast.Mast,
			added, removed bool, key, addedValue, removedValue interface{},
			conflictCB OnConflictMerged) (bool, error) {
			var newValue Value
			if !added && !removed { // changed
				av := addedValue.(Value)
				rv := removedValue.(Value)
				newValue = *LastWriteWins(&av, &rv)
				var cv Value
				ok, err := newTree.Get(ctx, "conflicts", &cv)
				if err != nil {
					return false, fmt.Errorf("get conflict counter value: %w", err)
				}
				if ok && cv.Value != nil {
					cv.Value = cv.Value.(int) + 1
				} else {
					cv.Value = 1
				}
				err = newTree.Insert(ctx, "conflicts", cv)
				if err != nil {
					return false, fmt.Errorf("set conflict counter value: %w", err)
				}
				return true, nil
			} else if added {
				// already present
				return true, nil
			} else if removed {
				newValue = removedValue.(Value)
			} else {
				return false, fmt.Errorf("no added/removed value")
			}
			err := newTree.Insert(ctx, key, newValue)
			if err != nil {
				return false, fmt.Errorf("insert: %w", err)
			}
			return true, nil
		})

	forEachMarshaler(t, func(t *testing.T, cfg Config) {
		dir, err := ioutil.TempDir("", "s3dbtest")
		require.NoError(t, err)

		persist := file.NewPersistForPath(dir)

		empty := NewRoot(time.Now(), mast.DefaultBranchFactor)
		empty.MergeMode = MergeModeCustom
		gob.Register(Value{})
		cfg.KeysLike = "hi"
		cfg.ValuesLike = 1234
		cfg.StoreImmutablePartsWith = persist
		cfg.CustomMergeFunc = CountConflicts

		s1, err := Load(ctx, cfg, nil, empty)
		require.NoError(t, err)
		err = s1.Set(ctx, time.Now(), "user1", 1)
		require.NoError(t, err)
		_, err = s1.MakeRoot(ctx)
		require.NoError(t, err)
		var v int
		found, err := s1.Get(ctx, "user1", &v)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 1, v)

		s2, err := Load(ctx, cfg, nil, empty)
		require.NoError(t, err)
		err = s2.Set(ctx, time.Now(), "user1", 2)
		require.NoError(t, err)
		s2Root, err := s2.MakeRoot(ctx)
		require.NoError(t, err)

		err = s1.Merge(ctx, s2)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), s1.Size())
		assert.Equal(t, uint64(1), s2.Size())
		found, err = s1.Get(ctx, "user1", &v)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, 1, v)

		s1Root, err := s1.MakeRoot(ctx)
		require.NoError(t, err)
		var s1Conflicts, s2Conflicts int
		_, err = s1.Get(ctx, "conflicts", &s1Conflicts)
		require.NoError(t, err)
		_, err = s2.Get(ctx, "conflicts", &s2Conflicts)
		assert.Equal(t, 1, s1Conflicts)
		assert.Equal(t, 0, s2Conflicts)
		assert.Equal(t, uint64(2), s1Root.Size)
		assert.Equal(t, uint64(1), s2Root.Size)
	})
}

func marshalGob(thing interface{}) ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(thing)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return network.Bytes(), nil
}

func unmarshalGob(input []byte, thing interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(input))
	err := dec.Decode(thing)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}
