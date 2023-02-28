package kv

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jrhy/mast"
	s3Persist "github.com/jrhy/mast/persist/s3"
	"github.com/jrhy/mast/persist/s3test"
	"github.com/jrhy/s3db/kv/internal/crdt"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestS3Copy(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)

	const (
		prefix = "prefix/"
		src    = "foo/123/456"
		dst    = "bar/456/789"
	)
	p := s3Persist.NewPersist(c, c.Endpoint, bucketName, prefix)
	err := p.Store(ctx, src, []byte("hello"))
	require.NoError(t, err)
	loaded, err := p.Load(ctx, src)
	require.NoError(t, err)
	require.Equal(t, loaded, []byte("hello"))

	_, err = c.CopyObject(&s3.CopyObjectInput{
		Bucket:     &bucketName,
		CopySource: aws.String(bucketName + "/" + /*url.PathEscape(*/ prefix + src /*)*/),
		Key:        aws.String(prefix + dst),
		Expires:    aws.Time(time.Now().AddDate(10, 0, 0)),
	})
	require.NoError(t, err)

	_, err = c.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    aws.String(prefix + src),
	})
	require.NoError(t, err)

	loaded, err = p.Load(ctx, dst)
	require.NoError(t, err)
	require.Equal(t, loaded, []byte("hello"))

	loaded, err = p.Load(ctx, src)
	require.Error(t, err)
	ae := err.(awserr.Error)
	require.NotNil(t, ae)
	require.Equal(t, s3.ErrCodeNoSuchKey, ae.Code())
	require.Nil(t, loaded)
}

func TestListRoots_Empty(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage: &S3BucketInfo{
			EndpointURL: c.Endpoint,
			BucketName:  bucketName,
			Prefix:      "prefix",
		},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	s, err := Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Empty(t, s.crdt.MergeSources)
	roots, err := s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{}, roots)
}

func TestS3DBHappyCase(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:    &S3BucketInfo{c.Endpoint, bucketName, "happyDB"},
		KeysLike:   1234,
		ValuesLike: "hi",
	}
	s, err := Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(0), s.Size())
	require.Nil(t, s.crdt.Source)
	require.Empty(t, s.crdt.MergeSources)
	s2, err := Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(0), s2.Size())
	require.Nil(t, s2.crdt.Source)
	require.Empty(t, s2.crdt.MergeSources)

	err = s.Set(ctx, time.Now(), 1, "yo")
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.Size())
	root, err := s.Commit(ctx)
	require.NoError(t, err)
	roots, err := s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{*root}, roots)

	err = s2.Set(ctx, time.Now(), 2, "yoyo")
	require.NoError(t, err)
	require.Equal(t, uint64(1), s2.Size())
	root2, err := s2.Commit(ctx)
	require.NoError(t, err)
	roots, err = s.listRoots(ctx)
	require.NoError(t, err)
	sort.Strings(roots)
	expected := []string{*root, *root2}
	sort.Strings(expected)
	require.EqualValues(t, expected, roots)

	sMerged, err := Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(2), sMerged.Size())
	require.False(t, sMerged.IsDirty())
	var v1, v2 string
	ok, err := sMerged.Get(ctx, 1, &v1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "yo", v1)
	ok, err = sMerged.Get(ctx, 2, &v2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "yoyo", v2)
	roots, err = s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(roots))

	err = sMerged.Set(ctx, time.Now(), 5, "foo")
	require.NoError(t, err)
	require.True(t, sMerged.IsDirty())
	sMerged.Cancel()
}

type screwyS3 struct {
	s3.S3
	hideBucketPath map[string]string
	hidden         map[string]map[string][]byte
}

func (s *screwyS3) StartHiding(bucketName, prefix string) {
	if _, ok := s.hideBucketPath[bucketName]; ok {
		panic("already hiding")
	}
	if s.hideBucketPath == nil {
		s.hideBucketPath = map[string]string{}
	}
	s.hideBucketPath[bucketName] = prefix
	if s.hidden == nil {
		s.hidden = map[string]map[string][]byte{
			bucketName: {},
		}
	}
}

func (s *screwyS3) Unhide() {
	for bucketName, m := range s.hidden {
		for key, data := range m {
			_, err := s.S3.PutObject(&s3.PutObjectInput{
				Bucket: &bucketName,
				Key:    &key,
				Body:   bytes.NewReader(data),
			})
			if err != nil {
				panic(err)
			}
		}
	}
	s.hideBucketPath = nil
}

func (s *screwyS3) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if path, ok := s.hideBucketPath[*input.Bucket]; ok {
		if strings.HasPrefix(*input.Key, path) {
			data, err := ioutil.ReadAll(input.Body)
			if err != nil {
				panic(err)
			}
			s.hidden[*input.Bucket][*input.Key] = data
			return nil, nil
		}
	}
	return s.S3.PutObjectWithContext(ctx, input, opts...)
}

func TestDelayedNode(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	realC, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:    &S3BucketInfo{realC.Endpoint, bucketName, "delayedNodeDB"},
		KeysLike:   1234,
		ValuesLike: "hi",
	}
	c := &screwyS3{S3: *realC}
	s, err := Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(0), s.Size())
	require.Empty(t, s.crdt.MergeSources)

	err = s.Set(ctx, time.Now(), 1, "yo")
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.Size())
	sp := s.persist.(*persistEncryptor)
	c.StartHiding(sp.BucketName, sp.Prefix)
	v1, err := s.Commit(ctx)
	require.NoError(t, err)
	roots, err := s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(roots), "saved root should be visible")

	// opening, with supposedly-unreadable node that should be silently skipped
	s, err = Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(0), s.Size())
	require.Equal(t, 1, s.unmergeableRoots)
	require.Empty(t, s.crdt.MergeSources)
	c.Unhide()

	// opening, can now read all blocks
	s, err = Open(ctx, c, cfg, OpenOptions{}, time.Now())
	require.NoError(t, err)
	require.Equal(t, uint64(1), s.Size())
	require.Equal(t, 0, s.unmergeableRoots)
	require.EqualValues(t, []string{*v1}, s.crdt.MergeSources)
}

func dbgprintf(f string, args ...interface{}) {
	if false {
		fmt.Printf(f, args...)
	}
}

type TestTime time.Time

func newTestTime() TestTime {
	t1, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Mon Jan 2 15:04:05 -0700 MST 2006")
	if err != nil {
		panic(err)
	}
	return TestTime(t1)
}

func (t *TestTime) next() time.Time {
	*t = TestTime(time.Time(*t).Add(1 * time.Second))
	return time.Time(*t)
}

func TestVersionGraph(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{c.Endpoint, bucketName, "versionGraph"},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	cfg.CustomRootPrefix = "s~"
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	branchFactor := int(s.crdt.Mast.BranchFactor())
	require.Equal(t, branchFactor, int(cfg.BranchFactor))
	for i := 0; i < branchFactor+1; i++ {
		err := s.Set(ctx, tm.next(), i, "")
		require.NoError(t, err)
	}
	require.Equal(t, uint64(branchFactor+1), s.Size())
	require.Equal(t, 1, s.Height())
	sRoot, err := s.Commit(ctx)
	require.NoError(t, err)

	cfg.CustomRootPrefix = "sModifyLeaf~"
	s2ModifyLeaf, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	s2ModifyRoot, err := s2ModifyLeaf.Clone(ctx)
	s2ModifyRoot.cfg.CustomRootPrefix = "s2ModifyRoot~"
	require.NoError(t, err)

	err = s2ModifyLeaf.Set(ctx, tm.next(), 1, "goo")
	require.NoError(t, err)
	s2ModifyLeafRoot, err := s2ModifyLeaf.Commit(ctx)
	require.NoError(t, err)

	err = s2ModifyRoot.Set(ctx, tm.next(), branchFactor, "goo")
	require.NoError(t, err)
	s2ModifyRootRoot, err := s2ModifyRoot.Commit(ctx)
	require.NoError(t, err)

	cfg.CustomRootPrefix = "sMerged~"
	sMerged, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	sMergedRoot, err := sMerged.Commit(ctx)
	require.NoError(t, err)
	var actual string
	_, err = sMerged.Get(ctx, 1, &actual)
	require.NoError(t, err)
	require.Equal(t, "goo", actual)
	_, err = sMerged.Get(ctx, branchFactor, &actual)
	require.Equal(t, "goo", actual)
	require.NoError(t, err)

	cfg.CustomRootPrefix = "s4~"
	s4, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	s4.Set(ctx, tm.next(), 999, "dummy")
	s4.Commit(ctx)
	// cause merge
	_, err = Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)

	roots, err := s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(roots))
	active := roots[0]
	_, err = s.listMergedRoots(ctx)
	require.NoError(t, err)

	dbgprintf("s:\t\t%s\n", *sRoot)
	dbgprintf("s2ModifyLeaf:\t\t%s\n", *s2ModifyLeafRoot)
	dbgprintf("s2ModifyRoot:\t\t%s\n", *s2ModifyRootRoot)
	dbgprintf("sMerged:\t%s\n", *sMergedRoot)

	nodes, _, err := sMerged.getHistoricRootsAndNodes(ctx, *sMerged.crdt.Created, cfg.LogFunc)
	require.NoError(t, err)
	dbgprintf("\nsuperseded nodes:\n")
	for i := range nodes {
		dbgprintf("  %+v\n", nodes[i])
	}

	allNodes, err := s.s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: &s.persist.(*persistEncryptor).BucketName,
		Prefix: &s.persist.(*persistEncryptor).Prefix,
	})
	require.NoError(t, err)
	nodesThatWouldBePreserved := map[string]interface{}{}
	for _, o := range allNodes.Contents {
		nodesThatWouldBePreserved[*o.Key] = nil
	}
	for _, node := range getNodesForRoot(ctx, s, active, s.root) {
		delete(nodesThatWouldBePreserved, node)
	}
	currentNodes := map[string]interface{}{}
	for node := range nodesThatWouldBePreserved {
		currentNodes[node] = nil
	}
	require.EqualValues(t, nodesThatWouldBePreserved, currentNodes)

	/*
		fmt.Printf("version graph:\n")
		for k, v := range graph {
			fmt.Printf("%s:  %+v\n", k, *v)
		}
		fmt.Println()*/
}

func getNodesForRoot(ctx context.Context, s *DB, rootName string, persist mast.Persist) []string {
	root, _, err := loadRoot(ctx, persist, rootName)
	if err != nil {
		panic("load root")
	}
	tree, err := crdt.Load(ctx, s.crdt.Config, &rootName, *root)
	if err != nil {
		panic(fmt.Errorf("new root: %w", err))
	}
	empty := crdt.NewRoot(time.Time{}, 0)
	emptyTree, err := crdt.Load(ctx, s.crdt.Config, nil, empty)
	if err != nil {
		panic(fmt.Errorf("empty root: %w", err))
	}
	res := []string{}
	err = tree.Mast.DiffLinks(ctx, emptyTree.Mast,
		func(removed bool, link interface{}) (bool, error) {
			if !removed {
				res = append(res, link.(string))
			}
			return true, nil
		})
	if err != nil {
		panic(fmt.Errorf("diffLinks: %w", err))
	}
	return res
}

func dumpRootsAndNodes(ctx context.Context, roots []string, s *DB, persist mast.Persist) {
	for i := range roots {
		fmt.Printf("  %s nodes:\n", roots[i])
		nodes := getNodesForRoot(ctx, s, roots[i], persist)
		for i := range nodes {
			fmt.Printf("    %+v\n", nodes[i])
		}
	}
}

func dumpBucket(s *DB) {
	fmt.Printf("bucket contents:\n")
	list, err := s.s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: &s.root.BucketName,
	})
	if err != nil {
		panic(err)
	}
	if *list.IsTruncated {
		panic("too much crap")
	}
	for _, o := range list.Contents {
		fmt.Printf("  %s\n", *o.Key)
	}
}

func contentHash(s *DB) string {
	return bucketContentHashForPrefix(s.s3Client, s.cfg.Storage.BucketName, s.cfg.Storage.Prefix)
}

func bucketContentHashForPrefix(s S3Interface, bucketName, prefix string) string {
	list, err := s.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	})
	if err != nil {
		panic(err)
	}
	if *list.IsTruncated {
		panic("too much crap")
	}
	digest := blake2b.New256()
	for _, o := range list.Contents {
		digest.Write([]byte(*o.Key))
		digest.Write([]byte(o.LastModified.String()))
		digest.Write([]byte(*o.ETag))
	}
	return base64.RawURLEncoding.EncodeToString(digest.Sum(nil))
}

func bucketUsage(s S3Interface, bucketName, prefix string) int {
	list, err := s.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: &prefix,
	})
	if err != nil {
		panic(err)
	}
	if *list.IsTruncated {
		panic("too much crap")
	}
	res := 0
	for _, o := range list.Contents {
		res += int(*o.Size)
	}
	return res
}

func TestAggregation(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	primaryConfig := Config{
		Storage:    &S3BucketInfo{c.Endpoint, bucketName, "primary"},
		KeysLike:   1234,
		ValuesLike: "hi",
	}

	primary, err := Open(ctx, c, primaryConfig, OpenOptions{}, tm.next())
	require.NoError(t, err)
	err = primary.Set(ctx, tm.next(), 1, "a")
	require.NoError(t, err)
	_, err = primary.Commit(ctx)
	require.NoError(t, err)

	type DerivedData struct {
		SourceVersion string
		Count         int
	}
	var lastDD DerivedData

	updateSecondary := func() error {
		secondary, err := Open(ctx, c, Config{
			Storage:    &S3BucketInfo{c.Endpoint, bucketName, "secondary"},
			KeysLike:   "hi",
			ValuesLike: DerivedData{},
		}, OpenOptions{}, tm.next())
		if err != nil {
			return fmt.Errorf("open secondary: %w", err)
		}
		var dd DerivedData
		ok, err := secondary.Get(ctx, "latest", &dd)
		if err != nil {
			return fmt.Errorf("get state: %w", err)
		}
		var lastReadPrimary *DB
		if ok {
			lastReadPrimary, err = Open(ctx, c, primaryConfig, OpenOptions{ReadOnly: true, SingleVersion: dd.SourceVersion}, time.Time{})
			if err != nil {
				// log that we couldn't read the old version, but that's OK, we can regenerate everything from scratch
			}
		}
		latestPrimary, err := Open(ctx, c, primaryConfig, OpenOptions{ReadOnly: true}, time.Time{})
		if err != nil {
			return fmt.Errorf("open latest primary")
		}
		source, err := latestPrimary.Commit(ctx)
		if err != nil {
			return fmt.Errorf("get single root for latest primary: %w", err)
		}
		if source == nil {
			// no data added yet
			return nil
		}
		updateFunc := func(key, addedValue, removedValue interface{}) (keepGoing bool, err error) {
			if addedValue != nil && removedValue == nil {
				dd.Count++
			} else if addedValue == nil && removedValue != nil {
				dd.Count--
			}
			return true, nil
		}
		err = latestPrimary.Diff(ctx, lastReadPrimary, updateFunc)
		if err != nil {
			return fmt.Errorf("update derived data: %w", err)
		}
		dd.SourceVersion = *source
		err = secondary.Set(ctx, tm.next(), "latest", &dd)
		if err != nil {
			return fmt.Errorf("set: %w", err)
		}
		lastDD = dd
		_, err = secondary.Commit(ctx)
		if err != nil {
			return fmt.Errorf("save secondary: %w", err)
		}
		return nil
	}

	err = updateSecondary()
	if err != nil {
		panic(err)
	}
	require.Equal(t, 1, lastDD.Count)

	primary, err = Open(ctx, c, primaryConfig, OpenOptions{}, tm.next())
	require.NoError(t, err)
	err = primary.Set(ctx, tm.next(), 1, "b")
	require.NoError(t, err)
	err = primary.Set(ctx, tm.next(), 2, "b")
	require.NoError(t, err)
	_, err = primary.Commit(ctx)
	require.NoError(t, err)

	err = updateSecondary()
	if err != nil {
		panic(err)
	}
	require.Equal(t, 2, lastDD.Count)
}

type countyS3 struct {
	s3.S3
	l                    sync.Mutex
	getAttemptCountByKey map[string]int
	putAttemptCount      int
}

func (s *countyS3) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if s.getAttemptCountByKey == nil {
		s.getAttemptCountByKey = map[string]int{
			*input.Key: 1,
		}
	} else {
		s.getAttemptCountByKey[*input.Key]++
	}
	return s.S3.GetObjectWithContext(ctx, input, opts...)
}

func (s *countyS3) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	s.l.Lock()
	s.putAttemptCount++
	defer s.l.Unlock()
	//fmt.Printf("PUT %s\n", *input.Key)
	return s.S3.PutObjectWithContext(ctx, input, opts...)
}

func TestDefaultNodeCacheOff(t *testing.T) {
	tm := newTestTime()
	realC, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{realC.Endpoint, bucketName, "nodeCacheOff"},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	c := &countyS3{S3: *realC}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Set(ctx, tm.next(), i, "")
		require.NoError(t, err)
	}
	require.Equal(t, uint64(cfg.BranchFactor+1), s.Size())
	require.Equal(t, 1, s.Height())
	_, err = s.Commit(ctx)
	require.NoError(t, err)

	s, err = Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	nop := func(_, _, _ interface{}) (bool, error) { return true, nil }
	err = s.Diff(ctx, nil, nop)
	require.NoError(t, err)
	err = s.Diff(ctx, nil, nop)
	require.NoError(t, err)
	nodes := 0
	for key, count := range c.getAttemptCountByKey {
		if !strings.HasPrefix(key, "nodeCacheOff/node") {
			continue
		}
		nodes++
		require.Greater(t, count, 1)
	}
	require.Equal(t, 2, nodes)
}

func TestNodeCache(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	realC, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{realC.Endpoint, bucketName, "nodeCacheOn"},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	c := &countyS3{S3: *realC}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Set(ctx, tm.next(), i, "")
		require.NoError(t, err)
	}
	require.Equal(t, uint64(cfg.BranchFactor+1), s.Size())
	require.Equal(t, 1, s.Height())
	_, err = s.Commit(ctx)
	require.NoError(t, err)

	cfg.NodeCache = mast.NewNodeCache(10)
	s, err = Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	nop := func(_, _, _ interface{}) (bool, error) { return true, nil }
	err = s.Diff(ctx, nil, nop)
	require.NoError(t, err)
	err = s.Diff(ctx, nil, nop)
	require.NoError(t, err)
	s, err = Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	err = s.Diff(ctx, nil, nop)
	require.NoError(t, err)
	err = s.Diff(ctx, nil, nop)
	require.NoError(t, err)

	nodes := 0
	for key, count := range c.getAttemptCountByKey {
		if !strings.HasPrefix(key, "nodeCacheOn/node") {
			continue
		}
		nodes++
		require.Equal(t, 1, count)
	}
	require.Equal(t, 2, nodes)
}

func TestRedundantCommitDoesNotWriteToBucket(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	realC, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{realC.Endpoint, bucketName, "redundantCommit"},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	c := &countyS3{S3: *realC}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Set(ctx, tm.next(), i, "")
		require.NoError(t, err)
	}
	require.Equal(t, uint64(cfg.BranchFactor+1), s.Size())
	require.Equal(t, 1, s.Height())
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	assert.Greater(t, c.putAttemptCount, 0)

	startCount := c.putAttemptCount
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	assert.Equal(t, startCount, c.putAttemptCount, "redundant commit should not write anything")
}

func TestSetWithAndWithoutLaterModificationTime(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{c.Endpoint, bucketName, "setSameValueWithLaterModTime"},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	origTime := tm.next()
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Set(ctx, origTime, i, "")
		require.NoError(t, err)
	}
	require.Equal(t, uint64(cfg.BranchFactor+1), s.Size())
	require.Equal(t, 1, s.Height())
	_, err = s.Commit(ctx)
	require.NoError(t, err)

	origHash := contentHash(s)

	newTime := tm.next()
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Set(ctx, newTime, i, "")
		require.NoError(t, err)
	}
	assert.True(t, s.IsDirty())
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	newHash := contentHash(s)
	assert.NotEqual(t, origHash, newHash, "set entry with later modification time should result in new content even with unchanged value")
}

func TestTombstoneVersusModTime(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{c.Endpoint, bucketName, "setSameValueWithLaterModTime"},
		KeysLike:     1234,
		ValuesLike:   "hi",
		BranchFactor: 4,
	}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	earlyTime := tm.next()
	origTime := tm.next()
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Tombstone(ctx, origTime, i)
		require.NoError(t, err)
	}
	require.Equal(t, uint64(cfg.BranchFactor+1), s.Size())
	require.Equal(t, 1, s.Height())
	_, err = s.Commit(ctx)
	require.NoError(t, err)

	origHash := contentHash(s)

	reopen(&s, c, cfg, tm.next())
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Tombstone(ctx, origTime, i)
		require.NoError(t, err)
		err = s.RemoveTombstones(ctx, earlyTime)
		require.NoError(t, err)
	}
	assert.False(t, s.IsDirty())
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	newHash := contentHash(s)
	assert.Equal(t, origHash, newHash, "re-tombstone values with same modification time shouldn't change the bucket")

	reopen(&s, c, cfg, tm.next())
	newTime := tm.next()
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Tombstone(ctx, newTime, i)
		require.NoError(t, err)
		err = s.RemoveTombstones(ctx, earlyTime)
		require.NoError(t, err)
	}
	assert.False(t, s.IsDirty())
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	newHash = contentHash(s)
	assert.Equal(t, origHash, newHash, "tombstone with later modification time should not result in new content")

	reopen(&s, c, cfg, tm.next())
	for i := 0; uint(i) < cfg.BranchFactor+1; i++ {
		err := s.Tombstone(ctx, earlyTime, i)
		require.NoError(t, err)
		err = s.RemoveTombstones(ctx, earlyTime)
		require.NoError(t, err)
	}
	assert.True(t, s.IsDirty())
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	newHash = contentHash(s)
	assert.NotEqual(t, origHash, newHash, "tombstone with earlier modification time should result in new content")
}

type dbgS3 struct {
	S3Interface
}

func (s *dbgS3) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	fmt.Printf("PUT %s...\n", *input.Key)
	return s.S3Interface.PutObjectWithContext(ctx, input, opts...)
}

func TestUpdateVsDeleteConflict(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	updateThenDelete := "updateThenDelete"
	deleteThenUpdate := "deleteThenUpdate"
	updateOnly := "updateOnly"
	origValue := 123
	s, c, cfg, tm, closer := createTestTree("updateVsDelete", updateThenDelete, origValue,
		deleteThenUpdate, origValue,
		updateOnly, origValue)
	defer closer()
	_, err := s.Commit(ctx)
	require.NoError(t, err)
	origHash := contentHash(s)
	left, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	right, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	require.Equal(t, origHash, contentHash(s), "open single root should not result in a merge")
	require.NoError(t, left.Set(ctx, tm.next(), updateThenDelete, origValue+1))
	require.NoError(t, right.Tombstone(ctx, tm.next(), updateThenDelete))
	require.NoError(t, left.Tombstone(ctx, tm.next(), deleteThenUpdate))
	require.NoError(t, right.Set(ctx, tm.next(), deleteThenUpdate, origValue+1))
	require.NoError(t, left.Set(ctx, tm.next(), updateOnly, origValue+1))
	require.NoError(t, right.Set(ctx, tm.next(), updateOnly, origValue+2))
	_, err = left.Commit(ctx)
	require.NoError(t, err)
	_, err = right.Commit(ctx)
	require.NoError(t, err)
	merged, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	var retrieved int
	ok, err := merged.Get(ctx, updateThenDelete, &retrieved)
	require.NoError(t, err)
	require.False(t, ok, "tombstone always wins")
	ok, err = merged.Get(ctx, deleteThenUpdate, &retrieved)
	require.NoError(t, err)
	require.False(t, ok, "tombstone always wins")
	ok, err = merged.Get(ctx, updateOnly, &retrieved)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, origValue+2, retrieved, "later value wins")
}

func createTestTree(name string, key, value interface{}, keyVals ...interface{}) (*DB, S3Interface, Config, TestTime, func()) {
	return createTestTreeWithConfig(name, nil, key, value, keyVals...)
}
func createTestTreeWithConfig(name string, baseCfg *Config, key, value interface{}, keyVals ...interface{}) (*DB, S3Interface, Config, TestTime, func()) {
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	// dbgC:=&dbgS3{c}
	var cfg Config
	if baseCfg != nil {
		cfg = *baseCfg
	}
	cfg.Storage = &S3BucketInfo{c.Endpoint, bucketName, name}
	cfg.LogFunc = func(s string) { fmt.Println(s) }
	cfg.KeysLike = key
	cfg.ValuesLike = value
	if cfg.BranchFactor == 0 {
		cfg.BranchFactor = 4
	}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	if err != nil {
		panic(err)
	}
	if err := s.Set(ctx, tm.next(), key, value); err != nil {
		panic(err)
	}
	if len(keyVals)%2 == 1 {
		panic("keyVals missing value")
	}
	for i := range keyVals {
		if i%2 == 0 {
			key = keyVals[i]
		} else {
			value = keyVals[i]
			if err := s.Set(ctx, tm.next(), key, value); err != nil {
				panic(err)
			}
		}
	}
	return s, c, cfg, tm, closer
}

func TestStructKeysAndValues(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	type key struct {
		A int
		B string
	}
	type value struct {
		C int
		D string
	}
	updateThenDelete := key{1, "updateThenDelete"}
	deleteThenUpdate := key{1, "deleteThenUpdate"}
	updateOnly := key{1, "updateOnly"}
	deleteBeforeInsert := key{1, "deleteBeforeInsert"}
	origValue := value{123, "hello"}
	add := func(v value, n int) value { return value{v.C + n, v.D} }
	s, c, cfg, tm, closer := createTestTree("structKeysAndValues", updateThenDelete, origValue,
		deleteThenUpdate, origValue,
		updateOnly, origValue,
		deleteBeforeInsert, origValue)
	defer closer()
	_, err := s.Commit(ctx)
	require.NoError(t, err)
	origHash := contentHash(s)
	left, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	right, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	require.Equal(t, origHash, contentHash(s), "open single root should not result in a merge")
	require.NoError(t, left.Set(ctx, tm.next(), updateThenDelete, add(origValue, 1)))
	require.NoError(t, right.Tombstone(ctx, tm.next(), updateThenDelete))
	require.NoError(t, left.Tombstone(ctx, tm.next(), deleteThenUpdate))
	require.NoError(t, right.Set(ctx, tm.next(), deleteThenUpdate, add(origValue, 1)))
	require.NoError(t, left.Set(ctx, tm.next(), updateOnly, add(origValue, 1)))
	require.NoError(t, right.Set(ctx, tm.next(), updateOnly, add(origValue, 2)))
	require.NoError(t, left.Tombstone(ctx, tm.next(), deleteBeforeInsert))
	require.NoError(t, right.Set(ctx, tm.next(), deleteBeforeInsert, add(origValue, 1)))
	_, err = left.Commit(ctx)
	require.NoError(t, err)
	_, err = right.Commit(ctx)
	require.NoError(t, err)
	merged, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	var retrieved = value{999, "gah"}
	ok, err := merged.Get(ctx, updateThenDelete, &retrieved)
	require.NoError(t, err)
	assert.False(t, ok, "tombstone always wins")
	assert.Equal(t, value{999, "gah"}, retrieved, "get tombstone should preserve input")
	ok, err = merged.Get(ctx, deleteThenUpdate, &retrieved)
	assert.NoError(t, err)
	assert.False(t, ok, "tombstone always wins")
	assert.Equal(t, value{999, "gah"}, retrieved, "get tombstone should preserve input")
	ok, err = merged.Get(ctx, updateOnly, &retrieved)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, add(origValue, 2), retrieved, "later value wins")
	retrieved = value{999, "gah"}
	ok, err = merged.Get(ctx, deleteBeforeInsert, &retrieved)
	require.NoError(t, err)
	assert.False(t, ok, "tombstone always wins")
	assert.Equal(t, value{999, "gah"}, retrieved, "get tombstone should preserve input")
}

func TestTombstoneRemoval(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	s, c, cfg, tm, closer := createTestTree("TombstoneRemoval",
		0, "tombstone",
		1, "tombstoneLater",
		2, "preserve",
		3, "preserve",
		4, "preserve",
	)
	defer closer()
	reopen(&s, c, cfg, tm.next())
	require.Equal(t, uint64(5), s.Size())
	reopen(&s, c, cfg, tm.next())
	require.NoError(t, s.Tombstone(ctx, tm.next(), 0))
	cutoff := tm.next()
	reopen(&s, c, cfg, tm.next())
	require.NoError(t, s.Tombstone(ctx, tm.next(), 1))
	reopen(&s, c, cfg, tm.next())
	require.Equal(t, uint64(5), s.Size())
	reopen(&s, c, cfg, tm.next())
	require.NoError(t, s.RemoveTombstones(ctx, cutoff))
	reopen(&s, c, cfg, tm.next())
	require.Equal(t, uint64(4), s.Size())
	require.NoError(t, s.RemoveTombstones(ctx, tm.next()))
	reopen(&s, c, cfg, tm.next())
	require.Equal(t, uint64(3), s.Size())

	count := 0
	require.NoError(t, s.Diff(ctx, nil, func(key, myValue, fromValue interface{}) (keepGoing bool, err error) {
		require.Equal(t, myValue, "preserve")
		count++
		return true, nil
	}))
	require.Equal(t, 3, count)
}

func reopen(s **DB, c S3Interface, cfg Config, tm time.Time) {
	_, err := (*s).Commit(ctx)
	if err != nil {
		panic(err)
	}
	ns, err := Open(ctx, c, cfg, OpenOptions{}, tm)
	if err != nil {
		panic(err)
	}
	*s = ns
}

func TestDeleteHistory(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	s, c, cfg, tm, closer := createTestTree("DeleteHistory",
		1, "preserve",
		4, "preserve",
		5, "delete",
		6, "delete",
		7, "delete",
		8, "delete",
	)
	defer closer()
	require.Equal(t, uint64(6), s.Size())
	reopen(&s, c, cfg, tm.next())
	require.NoError(t, s.Tombstone(ctx, tm.next(), 5))
	require.NoError(t, s.Tombstone(ctx, tm.next(), 6))
	require.NoError(t, s.Tombstone(ctx, tm.next(), 7))
	require.NoError(t, s.Tombstone(ctx, tm.next(), 8))
	require.Equal(t, uint64(6), s.Size())
	require.NoError(t, s.RemoveTombstones(ctx, tm.next()))
	require.Equal(t, uint64(2), s.Size())
	reopen(&s, c, cfg, tm.next())

	t.Logf("deleting version with formerly-tombstoned entries, leaving 2\n")
	require.NoError(t, DeleteHistoricVersions(ctx, s, tm.next()))
	origHash := contentHash(s)

	require.Equal(t, uint64(2), s.Size())
	o, err := s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(o))
	o, err = s.listMergedRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(o))
	o, err = s.listNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(o))

	t.Logf("test DeleteHistoryBefore idempotency\n")
	require.NoError(t, DeleteHistoricVersions(ctx, s, tm.next()))
	newHash := contentHash(s)
	assert.Equal(t, origHash, newHash)

	count := 0
	require.NoError(t, s.Diff(ctx, nil, func(key, myValue, fromValue interface{}) (keepGoing bool, err error) {
		require.Equal(t, myValue, "preserve")
		count++
		return true, nil
	}))
	require.Equal(t, 2, count)

	t.Logf("now, delete everything. tombstoning...\n")
	require.NoError(t, s.Tombstone(ctx, tm.next(), 1))
	require.NoError(t, s.Tombstone(ctx, tm.next(), 4))
	t.Logf("tombstoned entries, now committing...\n")
	reopen(&s, c, cfg, tm.next())
	t.Logf("removing tombstones...\n")
	require.Equal(t, uint64(2), s.Size())
	require.NoError(t, s.RemoveTombstones(ctx, tm.next()))
	require.Equal(t, uint64(0), s.Size())
	require.True(t, s.tombstoned)
	t.Logf("removed tombstones, now committing...\n")
	reopen(&s, c, cfg, tm.next())
	require.False(t, s.tombstoned)
	require.Equal(t, uint64(0), s.Size())
	require.False(t, s.IsDirty())
	t.Logf("deleting historic versions...\n")
	require.NoError(t, DeleteHistoricVersions(ctx, s, tm.next()))
	require.False(t, s.IsDirty())

	o, err = s.listRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{}, o, "all the current roots should have been removed")
	o, err = s.listMergedRoots(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(o), "there really shouldn't be any merged roots left")
	o, err = s.listNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(o), "no nodes left")
}

func TestDecryptionWithWrongKey(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	cfg := Config{
		Storage:       &S3BucketInfo{c.Endpoint, bucketName, "badkey"},
		KeysLike:      "stringy",
		ValuesLike:    "stringy",
		BranchFactor:  4,
		NodeEncryptor: V1NodeEncryptor(nil),
	}
	defer closer()
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)
	require.NoError(t, s.Set(ctx, tm.next(), "foo", "bar"))
	_, err = s.Commit(ctx)
	require.NoError(t, err)

	newKey := []byte{1}
	cfg.NodeEncryptor = V1NodeEncryptor(newKey)
	_, err = Open(ctx, c, cfg, OpenOptions{ReadOnly: true}, tm.next())
	require.True(t, errors.Is(err, ErrMACVerificationFailure))
}

func TestTraceHistory(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	s, c, cfg, tm, closer := createTestTree("traceHistory", "foo", 1)
	defer closer()
	reopen(&s, c, cfg, tm.next())
	require.NoError(t, s.Set(ctx, tm.next(), "foo", 2))
	reopen(&s, c, cfg, tm.next())
	require.NoError(t, s.Set(ctx, tm.next(), "foo", 3))
	reopen(&s, c, cfg, tm.next())
	history := []int{}
	require.NoError(t, s.TraceHistory(ctx, "foo", time.Time{}, func(when time.Time, value interface{}) (keepGoing bool, err error) {
		history = append(history, value.(int))
		return true, nil
	}))
	require.Equal(t, []int{3, 2, 1}, history)
}

func TestConflictDetection(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}

	t.Run("HappyCase", func(t *testing.T) {
		if s3test.CanParallelize() {
			t.Parallel()
		}
		conflicts := 0
		cfg := Config{
			OnConflictMerged: func(key, v1, v2 interface{}) error {
				conflicts++
				return nil
			},
		}
		s, c, cfg, tm, closer := createTestTreeWithConfig("conflictDetection_happycase", &cfg, "key", "1")
		defer closer()
		sAddition1, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, sAddition1.Set(ctx, tm.next(), "key2", "1"))
		_, err = sAddition1.Commit(ctx)
		require.NoError(t, err)
		sAddition2, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, sAddition2.Set(ctx, tm.next(), "key2", "2"))
		_, err = sAddition2.Commit(ctx)
		require.NoError(t, err)
		reopen(&s, c, cfg, tm.next())
		require.Equal(t, 1, conflicts)
		require.NoError(t, s.Set(ctx, tm.next(), "foo", "bar"))
		reopen(&s, c, cfg, tm.next())
		require.NoError(t, s.Set(ctx, tm.next(), "foo2", "bar"))
		reopen(&s, c, cfg, tm.next())
		require.NoError(t, s.Set(ctx, tm.next(), "foo3", "bar"))
		reopen(&s, c, cfg, tm.next())
		require.Equal(t, 1, conflicts)
	})

	t.Run("AdditionIsNotAConflict", func(t *testing.T) {
		if s3test.CanParallelize() {
			t.Parallel()
		}
		conflicts := 0
		cfg := Config{
			OnConflictMerged: func(key, v1, v2 interface{}) error {
				conflicts++
				return nil
			},
		}
		s, c, cfg, tm, closer := createTestTreeWithConfig("conflictDetection_addition", &cfg, "key", "1")
		defer closer()
		sAddition1, err := s.Clone(ctx)
		require.NoError(t, err)
		at := tm.next()
		require.NoError(t, sAddition1.Set(ctx, at, "key2", "1"))
		_, err = sAddition1.Commit(ctx)
		require.NoError(t, err)
		sAddition2, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, sAddition2.Set(ctx, at, "key3", "1"))
		_, err = sAddition2.Commit(ctx)
		require.NoError(t, err)
		reopen(&s, c, cfg, tm.next())
		require.Equal(t, 0, conflicts)
	})

	t.Run("IdempotentUpdateIsNotAConflict", func(t *testing.T) {
		if s3test.CanParallelize() {
			t.Parallel()
		}
		conflicts := 0
		cfg := Config{
			OnConflictMerged: func(key, v1, v2 interface{}) error {
				fmt.Printf("key=%v, v1=%v vs v2=%v\n", key, v1, v2)
				conflicts++
				return nil
			},
		}
		s, c, cfg, tm, closer := createTestTreeWithConfig("conflictDetection_idempotent_update", &cfg, "key", "1")
		defer closer()

		sAddition1, err := s.Clone(ctx)
		require.NoError(t, err)
		sAddition2, err := s.Clone(ctx)
		require.NoError(t, err)

		require.NoError(t, sAddition1.Set(ctx, tm.next(), "key2", "1"))
		_, err = sAddition1.Commit(ctx)
		require.NoError(t, err)
		require.NoError(t, sAddition2.Set(ctx, tm.next(), "key2", "1"))
		_, err = sAddition2.Commit(ctx)
		require.NoError(t, err)

		reopen(&s, c, cfg, tm.next())
		require.Equal(t, 0, conflicts)
	})

	t.Run("SetWithoutMergeIsNotAConflict", func(t *testing.T) {
		if s3test.CanParallelize() {
			t.Parallel()
		}
		conflicts := 0
		cfg := Config{
			OnConflictMerged: func(key, v1, v2 interface{}) error {
				conflicts++
				return nil
			},
		}
		s, c, cfg, tm, closer := createTestTreeWithConfig("conflictDetection_sametree", &cfg, "key", "1")
		defer closer()
		require.NoError(t, s.Set(ctx, tm.next(), "key", "2"))
		require.Equal(t, 0, conflicts)
		// not necessary to commit, but check anyway
		reopen(&s, c, cfg, tm.next())
		require.Equal(t, 0, conflicts)
	})

	t.Run("TombstoneIsNotAConflict", func(t *testing.T) {
		if s3test.CanParallelize() {
			t.Parallel()
		}
		conflicts := 0
		cfg := Config{
			OnConflictMerged: func(key, v1, v2 interface{}) error {
				conflicts++
				return nil
			},
		}
		s, c, cfg, tm, closer := createTestTreeWithConfig("conflictDetection_tombstone", &cfg, "key", "1")
		defer closer()
		sAddition1, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, sAddition1.Set(ctx, tm.next(), "key2", "1"))
		_, err = sAddition1.Commit(ctx)
		require.NoError(t, err)
		sTombstone, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, sTombstone.Tombstone(ctx, tm.next(), "key2"))
		_, err = sTombstone.Commit(ctx)
		require.NoError(t, err)
		reopen(&s, c, cfg, tm.next())
		require.Equal(t, 0, conflicts)
	})

	t.Run("ErrorStop", func(t *testing.T) {
		if s3test.CanParallelize() {
			t.Parallel()
		}
		conflicts := 0
		errKO := errors.New("momma said knock you out")
		cfg := Config{
			OnConflictMerged: func(key, v1, v2 interface{}) error {
				conflicts++
				return errKO
			},
		}
		s, c, cfg, tm, closer := createTestTreeWithConfig("conflictDetection_errorstop", &cfg, "key", "1")
		defer closer()
		_, err := s.Commit(ctx)
		require.NoError(t, err)
		s1, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, s1.Set(ctx, tm.next(), "key2", "1"))
		require.NoError(t, s1.Set(ctx, tm.next(), "key3", "1"))
		_, err = s1.Commit(ctx)
		require.NoError(t, err)
		s2, err := s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, s2.Set(ctx, tm.next(), "key2", "2"))
		require.NoError(t, s2.Set(ctx, tm.next(), "key3", "2"))
		_, err = s2.Commit(ctx)
		require.NoError(t, err)
		_, err = Open(ctx, c, cfg, OpenOptions{}, tm.next())
		require.EqualError(t, err, "merge: callback error: OnConflictMerged: momma said knock you out")
		require.Equal(t, 1, conflicts)
	})

}

func TestLinearCommitsMergePreviousVersions(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}

	t.Run("HappyCase", func(t *testing.T) {
		s, _, _, tm, closer := createTestTreeWithConfig("linear1", nil, "key", "1")
		defer closer()
		_, err := s.Commit(ctx)
		require.NoError(t, err)
		require.NoError(t, s.Set(ctx, tm.next(), "key2", "1"))
		_, err = s.Commit(ctx)
		require.NoError(t, err)
		roots, err := s.listRoots(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(roots), "previous root should be considered merged")
	})

	t.Run("WithClones", func(t *testing.T) {
		s, _, _, tm, closer := createTestTreeWithConfig("linear_with_clones", nil, "key", "1")
		defer closer()
		_, err := s.Commit(ctx)
		require.NoError(t, err)
		s, err = s.Clone(ctx)
		require.NoError(t, err)
		require.NoError(t, s.Set(ctx, tm.next(), "key2", "1"))
		_, err = s.Commit(ctx)
		require.NoError(t, err)
		s, err = s.Clone(ctx)
		require.NoError(t, err)
		roots, err := s.listRoots(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(roots), "previous root should be considered merged")
	})

}

func TestNodeCacheFiltersNodesCommittedByPeers(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}

	const bf = 4

	tm := newTestTime()
	realC, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:      &S3BucketInfo{realC.Endpoint, bucketName, "commitClonedBlocks"},
		KeysLike:     0,
		ValuesLike:   0,
		BranchFactor: bf,
		// turn on node caching
		NodeCache: mast.NewNodeCache(10),
	}
	c := &countyS3{S3: *realC}
	s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
	require.NoError(t, err)

	// v1: populate a single node
	for i := 0; i < bf; i++ {
		require.NoError(t, s.Set(ctx, tm.next(), i, i))
	}

	// v2: clone v1 before committed
	v2, err := s.Clone(ctx)
	require.NoError(t, err)

	// v2: grow the tree, preserving the existing node
	require.Equal(t, 0, v2.Height())
	require.NoError(t, v2.Set(ctx, tm.next(), bf, bf))
	require.Equal(t, 1, v2.Height())

	// v1: store the node
	_, err = s.Commit(ctx)
	require.NoError(t, err)
	nodes, err := s.listNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(nodes), "v1 is a single node")

	// v2: commit, which should only write the new root, ignoring the preserved node
	c.putAttemptCount = 0
	_, err = v2.Commit(ctx)
	require.NoError(t, err)
	nodes, err = v2.listNodes(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(nodes), "v2 grows the tree preserving the existing node")
	require.Equal(t, 2, c.putAttemptCount, "caching should have prevented redundant store of node from v1")
}

func TestUnmergeableBranchFactor(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}

	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg1 := Config{
		Storage:      &S3BucketInfo{c.Endpoint, bucketName, "unmergeable"},
		KeysLike:     0,
		ValuesLike:   0,
		BranchFactor: 16,
	}
	s1, err := Open(ctx, c, cfg1, OpenOptions{}, tm.next())
	require.NoError(t, err)

	cfg2 := cfg1
	cfg2.BranchFactor = 4

	s2, err := Open(ctx, c, cfg2, OpenOptions{}, tm.next())
	require.NoError(t, err)

	err = s1.Set(ctx, time.Now(), 1, 1)
	require.NoError(t, err)
	err = s2.Set(ctx, time.Now(), 2, 2)
	require.NoError(t, err)
	_, err = s1.Commit(ctx)
	require.NoError(t, err)
	_, err = s2.Commit(ctx)
	require.NoError(t, err)

	_, err = Open(ctx, c, cfg2, OpenOptions{}, tm.next())
	require.Contains(t, err.Error(), "varying branch factors")
	require.Contains(t, err.Error(), "without OpenOptions.ForceRebranch")

	cfg3 := cfg1
	cfg3.BranchFactor = 9
	merged, err := Open(ctx, c, cfg3, OpenOptions{ForceRebranch: true}, tm.next())
	require.NoError(t, err)
	require.Equal(t, uint(9), merged.BranchFactor())
	var v int
	_, err = merged.Get(ctx, 1, &v)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	_, err = merged.Get(ctx, 2, &v)
	require.NoError(t, err)
	require.Equal(t, 2, v)
	require.Equal(t, uint64(2), merged.Size())
}

func TestCursor(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	s, _, _, _, closer := createTestTree("cursor",
		"a", 1,
		"b", 2)
	defer closer()
	_, err := s.Commit(ctx)
	require.NoError(t, err)
	cursor, err := s.Cursor(ctx)
	require.NoError(t, err)
	require.NoError(t, cursor.Min(ctx))
	k, v, ok := cursor.Get()
	require.True(t, ok)
	require.Equal(t, "a", k)
	require.Equal(t, 1, v.Value)
	require.NoError(t, cursor.Forward(ctx))
	k, v, ok = cursor.Get()
	require.True(t, ok)
	require.Equal(t, "b", k)
	require.Equal(t, 2, v.Value)
	require.NoError(t, cursor.Forward(ctx))
	k, v, ok = cursor.Get()
	require.False(t, ok)
	require.Nil(t, k)
	require.Nil(t, v)
}

func TestStatefulMarshalingEfficiency(t *testing.T) {
	if s3test.CanParallelize() {
		t.Parallel()
	}
	tm := newTestTime()
	c, bucketName, closer := s3test.Client()
	t.Cleanup(closer)
	cfg := Config{
		Storage:        &S3BucketInfo{c.Endpoint, bucketName, "inefficient"},
		KeysLike:       1234,
		ValuesLike:     "hi",
		MastNodeFormat: string(mast.V115Binary),
	}
	populate := func(cfg Config) {
		s, err := Open(ctx, c, cfg, OpenOptions{}, tm.next())
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			err := s.Set(ctx, tm.next(), i, "")
			require.NoError(t, err)
		}
		_, err = s.Commit(ctx)
		require.NoError(t, err)
	}
	populate(cfg)

	cfg.Storage = &S3BucketInfo{c.Endpoint, bucketName, "statefulMarshal"}
	cfg.MastNodeFormat = string(mast.V1Marshaler)
	populate(cfg)

	v115Usage := bucketUsage(c, bucketName, "inefficient")
	// t.Logf("with v115Binary: %d", v115Usage)
	v1Usage := bucketUsage(c, bucketName, "statefulMarshal")
	// t.Logf("with statefun marshaling: %d", v1Usage)
	require.Less(t, v1Usage, v115Usage)
}
