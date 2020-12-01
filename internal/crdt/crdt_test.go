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
	err = s1.Set(ctx, time.Now(), "user1", MyObject{A: "a"})
	require.NoError(t, err)
	_, err = s1.MakeRoot(ctx)
	require.NoError(t, err)
	var v MyObject
	found, err := s1.Get(ctx, "user1", &v)
	assert.True(t, found)
	assert.Equal(t, MyObject{"a"}, v)

	s2, err := Load(ctx, cfg, nil, empty)
	err = s2.Set(ctx, time.Now(), "user1", MyObject{A: "b"})
	require.NoError(t, err)
	_, err = s2.MakeRoot(ctx)
	require.NoError(t, err)

	err = s1.Merge(ctx, s2)
	assert.Equal(t, uint64(1), s1.Size())
	assert.Equal(t, uint64(1), s2.Size())
	found, err = s1.Get(ctx, "user1", &v)
	assert.True(t, found)
	assert.Equal(t, MyObject{"b"}, v)
}

func TestImmediateDeletion(t *testing.T) {
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

	var cleanup []*Root

	s1, err := Load(ctx, cfg, nil, empty)
	err = s1.Set(ctx, time.Now(), "user1", MyObject{A: "a"})
	require.NoError(t, err)
	newRoot, err := s1.MakeRoot(ctx)
	require.NoError(t, err)
	cleanup = append(cleanup, newRoot)
	var v MyObject
	found, err := s1.Get(ctx, "user1", &v)
	assert.True(t, found)
	assert.Equal(t, MyObject{"a"}, v)

	s2, err := Load(ctx, cfg, nil, empty)
	err = s2.Set(ctx, time.Now(), "user1", MyObject{A: "b"})
	require.NoError(t, err)
	s2Root, err := s2.MakeRoot(ctx)
	require.NoError(t, err)
	cleanup = append(cleanup, s2Root)

	err = s1.Merge(ctx, s2)
	assert.Equal(t, uint64(1), s1.Size())
	assert.Equal(t, uint64(1), s2.Size())
	found, err = s1.Get(ctx, "user1", &v)
	assert.True(t, found)
	assert.Equal(t, MyObject{"b"}, v)

	s1Root, err := s1.MakeRoot(ctx)
	require.NoError(t, err)
	assert.Equal(t, s1Root, s2Root)
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
