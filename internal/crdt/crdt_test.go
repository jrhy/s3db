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
	store := mast.NewInMemoryStore()
	emptyRoot := NewRoot(time.Now(), mast.DefaultBranchFactor)
	cfg := Config{
		KeysLike:                1234,
		ValuesLike:              "hi",
		StoreImmutablePartsWith: store,
	}

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
}

func TestStructValues(t *testing.T) {
	type asdf struct {
		I int64
		F float64
	}
	type foo struct {
		Asdf asdf
		Jk   bool
	}
	store := mast.NewInMemoryStore()
	emptyRoot := NewRoot(time.Now(), mast.DefaultBranchFactor)
	cfg := Config{
		KeysLike:                1234,
		ValuesLike:              foo{asdf{1, 3.14}, true},
		StoreImmutablePartsWith: store,
	}

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
}

func TestFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "s3dbtest")
	require.NoError(t, err)

	persist := file.NewPersistForPath(dir)

	type MyObject struct {
		A string
	}

	empty := NewRoot(time.Now(), mast.DefaultBranchFactor)
	cfg := Config{
		KeysLike:                "hi",
		ValuesLike:              MyObject{},
		StoreImmutablePartsWith: persist,
	}

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
}

func TestCustomMergeFunc(t *testing.T) {
	dir, err := ioutil.TempDir("", "s3dbtest")
	require.NoError(t, err)

	persist := file.NewPersistForPath(dir)

	type MyObject struct {
		A string
	}
	var CountConflicts MergeFunc = MergeFunc(
		func(ctx context.Context, newTree *mast.Mast, /*, conflicts *uint64*/
			added, removed bool, key, addedValue, removedValue interface{},
			conflictCB OnConflictMerged) (bool, error) {
			var newValue interface{}
			if !added && !removed { // changed
				av := addedValue.(Value)
				rv := removedValue.(Value)
				newValue = LastWriteWins(av, rv)
				var cv Value
				ok, err := newTree.Get(ctx, "conflicts", &cv)
				if err != nil {
					return false, fmt.Errorf("get conflict counter value: %w", err)
				}
				if ok && cv.Value != nil {
					cv.Value = cv.Value.(uint64) + 1
				} else {
					cv.Value = uint64(1)
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

	empty := NewRoot(time.Now(), mast.DefaultBranchFactor)
	empty.MergeMode = MergeModeCustom
	gob.Register(Value{})
	gob.Register(MyObject{})
	cfg := Config{
		KeysLike:                       "hi",
		StoreImmutablePartsWith:        persist,
		Marshal:                        marshalGob,
		Unmarshal:                      unmarshalGob,
		UnmarshalerUsesRegisteredTypes: true,
		CustomMergeFunc:                CountConflicts,
	}

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
	s2Root, err := s2.MakeRoot(ctx)
	require.NoError(t, err)

	err = s1.Merge(ctx, s2)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), s1.Size())
	assert.Equal(t, uint64(1), s2.Size())
	found, err = s1.Get(ctx, "user1", &v)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, MyObject{"a"}, v)

	s1Root, err := s1.MakeRoot(ctx)
	require.NoError(t, err)
	var s1Conflicts, s2Conflicts uint64
	_, err = s1.Get(ctx, "conflicts", &s1Conflicts)
	require.NoError(t, err)
	_, err = s2.Get(ctx, "conflicts", &s2Conflicts)
	assert.Equal(t, uint64(1), s1Conflicts)
	assert.Equal(t, uint64(0), s2Conflicts)
	assert.Equal(t, uint64(2), s1Root.Size)
	assert.Equal(t, uint64(1), s2Root.Size)
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

func TestGob(t *testing.T) {
	store := mast.NewInMemoryStore()
	emptyRoot := NewRoot(time.Now(), mast.DefaultBranchFactor)
	type value struct {
		Foofee string
		Qwerty bool
	}
	gob.Register(Value{})
	gob.Register(value{})
	cfg := Config{
		KeysLike:                       1234,
		ValuesLike:                     value{},
		StoreImmutablePartsWith:        store,
		Marshal:                        marshalGob,
		Unmarshal:                      unmarshalGob,
		UnmarshalerUsesRegisteredTypes: true,
	}

	c1, err := Load(ctx, cfg, nil, emptyRoot)
	require.NoError(t, err)
	err = c1.Set(ctx, time.Now(), 0, value{Foofee: "tree 1 key", Qwerty: true})
	require.NoError(t, err)
	err = c1.Set(ctx, time.Now(), 1, value{Foofee: "first write loses"})
	require.NoError(t, err)
	c1Root, err := c1.MakeRoot(ctx)
	require.NoError(t, err)

	c2, err := Load(ctx, cfg, nil, emptyRoot)
	require.NoError(t, err)
	err = c2.Set(ctx, time.Now(), 1, value{"last write wins", false})
	require.NoError(t, err)
	err = c2.Set(ctx, time.Now(), 2, value{"tree 2 key", false})
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

	require.Equal(t, c3Root.Link, c4Root.Link)

	var v value
	contains, err := c4.Get(ctx, 1, &v)
	require.NoError(t, err)
	require.True(t, contains)
	require.Equal(t, value{Foofee: "last write wins"}, v)

	contains, err = c4.Get(ctx, 999, &v)
	require.NoError(t, err)
	require.False(t, contains)

	contains, err = c4.Get(ctx, 0, &v)
	require.NoError(t, err)
	require.True(t, contains)
	require.Equal(t, value{Foofee: "tree 1 key", Qwerty: true}, v)

	contains, err = c4.Get(ctx, 2, &v)
	require.NoError(t, err)
	require.True(t, contains)
	require.Equal(t, value{Foofee: "tree 2 key"}, v)
}
