// Package s3db turns your S3 bucket into a serverless database and lets you diff versions to track changes. 🤯
//
// s3db is an opinionated modern serverless datastore built on diffing
// that integrates streaming, reduces dependencies and makes it natural
// to write distributed systems with fewer idempotency bugs.
//
// s3db makes it easy to put and get structured data in S3 from Go, and
// track changes.
//
// s3db doesn't depend on any services other than S3-compatible storage.
//
// s3db is on a mission to make it easier to build applications that can
// take advantage of the high availability and durability of object
// storage, while also being predictable and resilient in the face of failures,
// by leveraging immutable data and CRDTs to get the best from local-first
// and shared-nothing architectures.
//
// # Requirements
//
// - go1.14+
//
// - S3-compatible storage, like minio, Wasabi, or AWS
//
// # Disambiguation
//
// This s3db isn't related to the Simple Sloppy Semantic Database
// (https://en.wikipedia.org/wiki/Simple_Sloppy_Semantic_Database) that
// predates Amazon S3.
//
// # Links
//
// * Merkle Search Tree, https://github.com/jrhy/mast
package s3db

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jrhy/mast"
	s3Persist "github.com/jrhy/mast/persist/s3"
	crdtpub "github.com/jrhy/s3db/crdt"
	"github.com/jrhy/s3db/internal/crdt"
	"github.com/minio/blake2b-simd"
)

const (
	// DefaultBranchFactor is the number of entries that will be stored per S3 object, if
	// not overridden by Config.BranchFactor.
	DefaultBranchFactor = 4096
)

var (
	// ErrReadOnly is the result of an attempt to Set or Commit on a tree that was not opened for writing.
	ErrReadOnly = errors.New("opened as read-only")
)

// DB is an Open()ed database.
type DB struct {
	readonly         bool
	s3Client         S3Interface
	persist          mast.Persist
	root             *s3Persist.Persist
	merged           *s3Persist.Persist
	cfg              *Config
	crdt             crdt.Tree
	mergedRoots      map[string][]byte
	unmergeableRoots int
	tombstoned       bool
	s3dbVersion      int
}

// Config defines how values are stored and (un)marshaled.
type Config struct {
	// S3 endpoint, bucket name, and database prefix
	Storage *S3BucketInfo
	// An optional label that will be added to committed versions' names for easier listing (e.g. "daily-report-")
	CustomRootPrefix string
	// An example of a key, to know what concrete type to make when unmarshaling
	KeysLike interface{}
	// An example of a value, to know what concrete type to make when unmarshaling
	ValuesLike interface{}
	// Sets the entries-per-S3 object for new trees (ignored unless tree is empty)
	BranchFactor uint
	// An optional S3-endpoint-scoped cache, instead of re-downloading recently-used tree nodes
	NodeCache mast.NodeCache
	// A way to keep your cloud provider out of your nodes
	NodeEncryptor Encryptor
	// OnConflictMerged is a callback that will be invoked whenever entries for a key have
	// different values in different trees that are being merged. It can be used to detect
	// when a uniqueness constraint has been broken and which keys need fixing.
	OnConflictMerged
	// LogFunc is a callback that will be invoked to provide details on potential corruption.
	LogFunc func(string)

	CustomMerge func(key interface{}, v1, v2 crdtpub.Value) crdtpub.Value

	MastNodeFormat string

	CustomMarshal func(interface{}) ([]byte, error)
	// CustomUnmarshal, if using registered types, will be invoked
	// to read tree nodes which are mast.PersistedNode with keys of
	// KeysLike, and values of crdtpub.Value of ValuesLike. All of
	// those should be registered before invoking s3db.Open().
	CustomUnmarshal              func([]byte, interface{}) error
	UnmarshalUsesRegisteredTypes bool
}

// OnConflictMerged is a callback that will be invoked whenever entries for a key have
// different values in different trees that are being merged. It can be used to detect
// when a uniqueness constraint has been broken and which keys need fixing.
type OnConflictMerged func(key, v1, v2 interface{}) error

// Encryptor encrypts bytes with a nonce derived from the given path.
type Encryptor interface {
	Encrypt(path string, value []byte) ([]byte, error)
	Decrypt(path string, value []byte) ([]byte, error)
}

// S3BucketInfo describes where DB objects, including nodes and roots, are stored.
type S3BucketInfo struct {
	// EndpointURL is used to distinguish servers for caching
	EndpointURL string
	// BucketName where the node objects are stored
	BucketName string
	// Prefix is a key prefix that distinguishes objects for this tree
	Prefix string
}

// OpenOptions control how databases are opened.
type OpenOptions struct {
	// ReadOnly means the database should not permit modifications.
	ReadOnly bool
	// SingleVersion is used to open a historic version
	SingleVersion string
	// ForceRebranch is used to change the branch factor of an existing tree, rewriting nodes and roots if necessary
	ForceRebranch bool
}

// S3Interface is the subset of the AWS SDK for S3 that s3db uses to access compatible buckets
type S3Interface interface {
	// à la AWS SDK for S3
	DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error)
	// à la AWS SDK for S3
	GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error)
	// à la AWS SDK for S3
	ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error)
	// à la AWS SDK for S3
	PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error)
}

// Open returns a database. 'when' marks the creation time of the new version.
func Open(ctx context.Context, S3 S3Interface, cfg Config, opts OpenOptions, when time.Time) (*DB, error) {
	if !opts.ReadOnly && opts.SingleVersion != "" {
		return nil, fmt.Errorf("opts.SingleVersion requires opts.ReadOnly")
	}
	if cfg.KeysLike == nil {
		return nil, fmt.Errorf("config KeysLike must be non-nil")
	}
	if cfg.Storage.EndpointURL == "" {
		return nil, fmt.Errorf("config Storage.EndpointURL unset")
	}
	nodePersist := cfg.Storage.fixPrefix().toPersistEncrypt(S3, "node/", cfg.NodeEncryptor)
	rootPersist := cfg.Storage.fixPrefix().toPersist(S3, "root/current/")
	mergedPersist := cfg.Storage.fixPrefix().toPersist(S3, "root/merged/")
	if cfg.BranchFactor == 0 {
		cfg.BranchFactor = DefaultBranchFactor
	}
	var (
		tree             *crdt.Tree
		mergedRoots      map[string][]byte
		unmergeableRoots int
	)
	marshal := marshalGob
	unmarshal := unmarshalGob
	unmarshalUsesRegisteredTypes := true
	if cfg.CustomMarshal != nil {
		marshal = cfg.CustomMarshal
		unmarshal = cfg.CustomUnmarshal
		unmarshalUsesRegisteredTypes = cfg.UnmarshalUsesRegisteredTypes
	} else {
		gob.Register(crdtpub.Value{})
		gob.Register(cfg.KeysLike)
		if cfg.ValuesLike != nil {
			gob.Register(cfg.ValuesLike)
		} else {
			gob.Register(map[string]interface{}{})
		}
	}
	crdtConfig := crdt.Config{
		KeysLike:                       cfg.KeysLike,
		ValuesLike:                     cfg.ValuesLike,
		StoreImmutablePartsWith:        nodePersist,
		NodeCache:                      cfg.NodeCache,
		MastNodeFormat:                 cfg.MastNodeFormat,
		Marshal:                        marshal,
		Unmarshal:                      unmarshal,
		UnmarshalerUsesRegisteredTypes: unmarshalUsesRegisteredTypes,
	}
	if cfg.CustomMerge != nil {
		crdtConfig.CustomMerge = cfg.CustomMerge
	}
	if cfg.OnConflictMerged != nil {
		crdtConfig.OnConflictMerged = crdt.OnConflictMerged(cfg.OnConflictMerged)
	}
	var s3dbVersion int
	if opts.SingleVersion != "" {
		root, _, err := loadNamedRoot(ctx, rootPersist, mergedPersist, opts.SingleVersion)
		if err != nil {
			return nil, fmt.Errorf("loadNamedRoot(%s): %w", opts.SingleVersion, err)
		}
		tree, err = crdt.Load(ctx, crdtConfig, &opts.SingleVersion, *root)
		if err != nil {
			return nil, fmt.Errorf("load: %w", err)
		}
		s3dbVersion = root.S3DBVersion
	} else {
		var err error
		tree, mergedRoots, unmergeableRoots, err = mergeRoots(ctx, S3, cfg, crdtConfig, rootPersist, when, opts.ForceRebranch, &s3dbVersion)
		if err != nil {
			return nil, fmt.Errorf("merge: %w", err)
		}
	}

	s := DB{
		readonly:         opts.ReadOnly,
		s3Client:         S3,
		persist:          nodePersist,
		root:             rootPersist,
		merged:           mergedPersist,
		cfg:              &cfg,
		crdt:             *tree,
		s3dbVersion:      s3dbVersion,
		mergedRoots:      mergedRoots,
		unmergeableRoots: unmergeableRoots,
	}
	if !opts.ReadOnly {
		_, err := s.Commit(ctx)
		if err != nil {
			return nil, fmt.Errorf("save merged root: %w", err)
		}
		runtime.SetFinalizer(&s, func(s *DB) {
			if s.IsDirty() {
				panic(fmt.Sprintf("dirty tree should have been Commit()ted or Cancel()ed; path %s", cfg.Storage.Prefix))
			}
		})
	}
	return &s, nil
}

func (spc *S3BucketInfo) fixPrefix() *S3BucketInfo {
	if len(spc.Prefix) > 0 && !strings.HasSuffix(spc.Prefix, "/") {
		spc.Prefix += "/"
	}
	return spc
}

func (spc S3BucketInfo) toPersist(
	S3 S3Interface,
	suffix string,
) *s3Persist.Persist {
	p := s3Persist.NewPersist(S3, spc.EndpointURL, spc.BucketName, spc.Prefix+suffix)
	return &p
}

func (spc S3BucketInfo) toPersistEncrypt(
	S3 S3Interface,
	suffix string,
	encryptor Encryptor,
) *persistEncryptor {
	p := s3Persist.NewPersist(S3, spc.EndpointURL, spc.BucketName, spc.Prefix+suffix)
	if encryptor == nil {
		encryptor = &noEncryption{}
	}
	return &persistEncryptor{encryptor, &p}
}

type noEncryption struct{}

func (p *noEncryption) Encrypt(path string, value []byte) ([]byte, error) { return value, nil }
func (p *noEncryption) Decrypt(path string, value []byte) ([]byte, error) { return value, nil }

type persistEncryptor struct {
	encryptor Encryptor
	*s3Persist.Persist
}

func (e *persistEncryptor) Store(ctx context.Context, path string, value []byte) error {
	encrypted, err := e.encryptor.Encrypt(path, value)
	if err != nil {
		return err
	}
	return e.Persist.Store(ctx, path, encrypted)
}
func (e *persistEncryptor) Load(ctx context.Context, path string) ([]byte, error) {
	value, err := e.Persist.Load(ctx, path)
	if err != nil {
		return nil, err
	}
	return e.encryptor.Decrypt(path, value)
}
func (e *persistEncryptor) NodeURLPrefix() string {
	return e.Persist.NodeURLPrefix()
}

func mergeRoots(
	ctx context.Context,
	S3 S3Interface,
	cfg Config,
	crdtConfig crdt.Config,
	rootPersist *s3Persist.Persist,
	when time.Time,
	forceRebranch bool,
	maxVersion *int,
) (*crdt.Tree, map[string][]byte, int, error) {
	roots, err := listObjects(ctx, S3, rootPersist.BucketName, rootPersist.Prefix)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("s3 list: %w", err)
	}

	// Even if we can't merge all the roots, due to errors, it's nice to make some
	// progress merging what we can. In order to not be persistently blocked by a
	// lowest-named root, we randomize the order.
	rand.Shuffle(len(roots), func(i, j int) {
		roots[i], roots[j] = roots[j], roots[i]
	})

	mergedRoots := make(map[string][]byte, len(roots))
	var tree *crdt.Tree
	for _, key := range roots {
		root, rootBytes, err := loadRoot(ctx, rootPersist, key)
		if err != nil {
			var ae awserr.Error
			if errors.As(err, &ae) && ae.Code() == s3.ErrCodeNoSuchKey {
				if cfg.LogFunc != nil {
					cfg.LogFunc(fmt.Sprintf("skipping merge for deleted root %v: %v", key, err))
				}
				continue
			}
			return nil, nil, 0, err
		}
		if root.S3DBVersion > *maxVersion {
			*maxVersion = root.S3DBVersion
		}
		graft, err := crdt.Load(ctx, crdtConfig, &key, *root)
		if err != nil {
			var ae awserr.Error
			if errors.As(err, &ae) && ae.Code() == s3.ErrCodeNoSuchKey {
				// TODO: log via callback instead
				if cfg.LogFunc != nil {
					cfg.LogFunc(fmt.Sprintf("skipping merge for deleted root %v: %v", key, err))
				}
				continue
			}
			return nil, nil, 0, err
		}
		if tree == nil && (!forceRebranch || cfg.BranchFactor == graft.Mast.BranchFactor()) {
			tree = graft
			tree.Source = nil
			tree.MergeSources = []string{key}
		} else {
			if !forceRebranch && tree.Mast.BranchFactor() != graft.Mast.BranchFactor() {
				return nil, nil, 0, fmt.Errorf(
					"cannot merge roots with varying branch factors, %d and %d, without OpenOptions.ForceRebranch",
					tree.Mast.BranchFactor(),
					graft.Mast.BranchFactor())
			}
			if tree == nil {
				tree, err = crdt.Load(ctx, crdtConfig, nil, emptyRoot(when, cfg.BranchFactor, crdtConfig))
				if err != nil {
					return nil, nil, 0, fmt.Errorf("new root: %w", err)
				}
			}

			newTree, err := tree.Clone(ctx)
			if err != nil {
				if cfg.LogFunc != nil {
					cfg.LogFunc(fmt.Sprintf("skipping merge un-cloneable tree %v: %v", key, err))
				}
				continue
			}
			err = newTree.Merge(ctx, graft)
			if _, ok := err.(crdt.MergeError); ok {
				return nil, nil, 0, err
			}
			if err != nil {
				if cfg.LogFunc != nil {
					cfg.LogFunc(fmt.Sprintf("skipping merge un-cloneable tree %v: %v", key, err))
				}
				continue
			}
			tree = newTree
		}
		mergedRoots[key] = rootBytes
	}

	unmergedRoots := len(roots) - len(mergedRoots)

	if tree == nil {
		root := emptyRoot(when, cfg.BranchFactor, crdtConfig)
		tree, err = crdt.Load(ctx, crdtConfig, nil, root)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("new root: %w", err)
		}
		*maxVersion = root.S3DBVersion
	} else {
		if len(mergedRoots) == 1 {
			tree.Source = getFirstKey(mergedRoots)
		}
		tree.Created = &when
	}

	return tree, mergedRoots, unmergedRoots, nil
}

func emptyRoot(when time.Time, branchFactor uint, crdtConfig crdt.Config) crdt.Root {
	empty := crdt.NewRoot(when, branchFactor)
	if crdtConfig.CustomMerge != nil {
		empty.MergeMode = crdt.MergeModeCustom
	} else if crdtConfig.OnConflictMerged != nil {
		empty.MergeMode = crdt.MergeModeCustomLWW
	}
	empty.NodeFormat = crdtConfig.MastNodeFormat
	empty.S3DBVersion = 1
	return empty
}

func loadRoot(ctx context.Context, persist mast.Persist, key string) (*crdt.Root, []byte, error) {
	rootBytes, err := persist.Load(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	var root crdt.Root
	jsonErr := json.Unmarshal(rootBytes, &root)
	if jsonErr != nil {
		err = unmarshalGob(rootBytes, &root)
		if err != nil {
			return nil, nil, fmt.Errorf("could not unmarshal as either json: %v, or unmarshal as gob: %w", jsonErr, err)
		}
	}
	return &root, rootBytes, nil
}

// Commit ensures any Set() entries become accessible on subsequent Open()s.
func (s *DB) Commit(ctx context.Context) (*string, error) {
	if !s.IsDirty() && !s.tombstoned && (s.crdt.Source != nil && len(s.crdt.MergeSources) <= 1 ||
		s.crdt.Source == nil && len(s.crdt.MergeSources) == 0) {
		return s.crdt.Source, nil
	}
	if s.readonly {
		return nil, ErrReadOnly
	}
	root, err := s.crdt.MakeRoot(ctx)
	if err != nil {
		return nil, fmt.Errorf("mast makeroot: %w", err)
	}
	root.S3DBVersion = s.s3dbVersion
	var rootBytes []byte
	switch s.s3dbVersion {
	case 0:
		rootBytes, err = marshalGob(root)
		if err != nil {
			return nil, fmt.Errorf("marshal root: gob: %w", err)
		}
	case 1:
		rootBytes, err = json.Marshal(root)
		if err != nil {
			return nil, fmt.Errorf("marshal root: json: %w", err)
		}
	default:
		return nil, fmt.Errorf("unhandled s3db version: %v", root.S3DBVersion)
	}

	hashBytes := blake2b.Sum256(rootBytes)
	crTime := big.NewInt(root.Created.Unix()).Text(62)
	hash := big.NewInt(0).SetBytes(hashBytes[:12]).Text(62)
	name := fmt.Sprintf("%s%06s_%s", s.cfg.CustomRootPrefix, crTime, hash)
	err = s.root.Store(ctx, name, rootBytes)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	s.moveMergedRoots(ctx, name, s.mergedRoots)
	s.mergedRoots = map[string][]byte{name: rootBytes}
	s.crdt.MergeSources = []string{name}
	s.crdt.Source = &name
	s.tombstoned = false
	return &name, nil
}

func (s DB) listRoots(ctx context.Context) ([]string, error) {
	return listObjects(ctx, s.s3Client, s.root.BucketName, s.root.Prefix)
}

func (s DB) listMergedRoots(ctx context.Context) ([]string, error) {
	return listObjects(ctx, s.s3Client, s.merged.BucketName, s.merged.Prefix)
}

func (s DB) listNodes(ctx context.Context) ([]string, error) {
	p := s.persist.(*persistEncryptor)
	return listObjects(ctx, s.s3Client, p.BucketName, p.Prefix)
}

func listObjects(ctx context.Context, c S3Interface, bucketName, prefix string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: aws.String(prefix),
	}
	res := []string{}
	for {
		out, err := c.ListObjectsV2WithContext(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("s3: %w", err)
		}
		for _, obj := range out.Contents {
			res = append(res, strings.TrimPrefix(*obj.Key, prefix))
		}
		if *(*out).IsTruncated {
			input.ContinuationToken = out.NextContinuationToken
		} else {
			break
		}
	}
	return res, nil
}

// Clone returns an independent database, with the same entries and uncommitted values as its
// source. Clones don't duplicate nodes that can be shared.
func (s *DB) Clone(ctx context.Context) (*DB, error) {
	s3dbCopy := *s
	cfgCopy := *s.cfg
	s3dbCopy.cfg = &cfgCopy
	crdtCopy, err := s.crdt.Clone(ctx)
	if err != nil {
		return nil, err
	}
	s3dbCopy.crdt = *crdtCopy
	return &s3dbCopy, nil
}

// Cancel indicates you don't want any of the previously-set entries persisted.
func (s *DB) Cancel() {
	runtime.SetFinalizer(s, nil)
}

func getFirstKey(m map[string][]byte) *string {
	for k := range m {
		return &k
	}
	return nil
}

type rootGraph map[string]*crdt.Root

func getFirst(m map[string]interface{}) (string, bool) {
	for k := range m {
		return k, true
	}
	return "", false
}

func (s DB) loadRootGraph(ctx context.Context) (rootGraph, error) {
	g := rootGraph{}
	todo := map[string]interface{}{}
	for _, rootName := range s.crdt.MergeSources {
		todo[rootName] = nil
	}
	for {
		rootName, ok := getFirst(todo)
		if !ok {
			break
		}
		root, _, err := loadNamedRoot(ctx, s.root, s.merged, rootName)
		if err != nil {
			var ae awserr.Error
			if errors.As(err, &ae) && ae.Code() == s3.ErrCodeNoSuchKey {
				// un-merged (current), or delayed root
				delete(todo, rootName)
				continue
			}
			return nil, fmt.Errorf("loadNamedRoot(%s): %w", rootName, err)
		}
		g[rootName] = root
		for _, parentName := range root.MergeSources {
			if _, ok := g[parentName]; !ok {
				todo[parentName] = nil
			}
		}
		delete(todo, rootName)
	}
	return g, nil
}

func loadNamedRoot(ctx context.Context, rootPersist, mergedPersist mast.Persist, rootName string) (*crdt.Root, []byte, error) {
	// common case: the root is historic, not current
	root, rootBytes, err := loadRoot(ctx, mergedPersist, rootName)
	if err == nil {
		return root, rootBytes, nil
	}
	if rootPersist == nil {
		// caller only wants merged roots; give up now
		return nil, nil, err
	}
	var ae awserr.Error
	if !errors.As(err, &ae) || ae.Code() != s3.ErrCodeNoSuchKey {
		return nil, nil, err
	}
	// is the root current?
	root, rootBytes, err = loadRoot(ctx, rootPersist, rootName)
	if err == nil {
		return root, rootBytes, nil
	}
	if !errors.As(err, &ae) || ae.Code() != s3.ErrCodeNoSuchKey {
		return nil, nil, err
	}
	// did it JUST get merged?
	return loadRoot(ctx, mergedPersist, rootName)
}

type dependentRoots map[string]map[string]*crdt.Root

func getDependents(mergedRoots rootGraph) dependentRoots {
	dependents := make(map[string]map[string]*crdt.Root, len(mergedRoots))
	for name, root := range mergedRoots {
		for _, source := range root.MergeSources {
			if dependentsForSource, ok := dependents[source]; ok {
				dependentsForSource[name] = root
			} else {
				dependents[source] = map[string]*crdt.Root{name: root}
			}
		}
	}
	return dependents
}

func (s *DB) moveMergedRoots(ctx context.Context, newRoot string, mergedRoots map[string][]byte) {
	for key, mergedRoot := range mergedRoots {
		if newRoot == key {
			continue
		}
		err := s.merged.Store(ctx, key, mergedRoot)
		if err != nil {
			// LOG return nil, fmt.Errorf("store merged root: %w", err)
			return
		}
		_, err = s.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Bucket: &s.root.BucketName,
			Key:    aws.String(s.root.Prefix + key),
		})
		if err != nil {
			// LOG return nil, fmt.Errorf("delete merged root: s3: %w", err)
			return
		}
	}
}

func (s *DB) getHistoricRootsAndNodes(
	ctx context.Context,
	olderThan time.Time,
) (roots, nodes []string, err error) {
	rootCacheByName, err := s.loadRootGraph(ctx)
	if err != nil {
		return nil, nil, err
	}
	candidateRoots := dependentRoots{}
	parentToChildren := getDependents(rootCacheByName)
	for parent, children := range parentToChildren {
		tooNew := false
		for _, childRoot := range children {
			if childRoot.Created == nil || childRoot.Created.After(olderThan) {
				tooNew = true
				break
			}
		}
		if !tooNew {
			candidateRoots[parent] = children
		}
	}
	candidateBlocks := make(map[string]int) // track root that can be deleted too
	for parentName, children := range candidateRoots {
		parentRoot, ok := rootCacheByName[parentName]
		if !ok {
			fmt.Printf("")
			continue
		}
		parent, err := crdt.Load(ctx, s.crdt.Config, &parentName, *parentRoot)
		if err != nil {
			// TODO: log
			continue
		}
		for childName, childRoot := range children {
			child, err := crdt.Load(ctx, s.crdt.Config, &childName, *childRoot)
			if err != nil {
				// TODO: log
				continue
			}
			err = child.Mast.DiffLinks(ctx, parent.Mast,
				func(removed bool, link interface{}) (bool, error) {
					if removed {
						if ls, ok := link.(string); ok {
							candidateBlocks[ls] = 1
						}
					}
					return true, nil
				})
			if err != nil {
				// TODO: log
			}
		}
	}
	nodes = make([]string, 0, len(candidateBlocks))
	for k := range candidateBlocks {
		nodes = append(nodes, k)
	}
	roots = make([]string, 0, len(candidateRoots))
	for k := range candidateRoots {
		roots = append(roots, k)
	}
	return roots, nodes, nil
}

// IsDirty returns true if there are entries in memory that haven't been Commit()ted.
func (s DB) IsDirty() bool {
	return s.tombstoned || s.crdt.IsDirty()
}

// Set puts a new value in memory. If the database already has a value later than "when", this does
// nothing. Any new values are buffered in memory until Commit()ted or Cancel()ed.
func (s *DB) Set(ctx context.Context, when time.Time, key interface{}, value interface{}) error {
	if s.readonly {
		return ErrReadOnly
	}
	return s.crdt.Set(ctx, when, key, value)
}

// Get retrieves the value for the given key. value must be a pointer that to
// which an Config.ValuesLike can be assigned, OR a pointer to a crdt.Value,
// in which case the CRDT value metadata will be included.
func (s *DB) Get(ctx context.Context, key interface{}, value interface{}) (bool, error) {
	return s.crdt.Get(ctx, key, value)
}

// IsTombstoned returns true if Tombstone() was invoked and committed
// since the last RemoveTombstones().
func (s *DB) IsTombstoned(ctx context.Context, key interface{}) (bool, error) {
	return s.crdt.IsTombstoned(ctx, key)
}

// Tombstone sets a persistent record indicating an entry will no longer be accessible, until
// RemoveTombstones() is done. If multiple versions have different values or tombstones for a key,
// they'll eventually get merged into the earliest tombstone. You can set a tombstone even if the
// entry was never set to a value, just to ensure that any future sets are ineffective.
func (s *DB) Tombstone(ctx context.Context, when time.Time, key interface{}) error {
	if s.readonly {
		return ErrReadOnly
	}
	return s.crdt.Tombstone(ctx, when, key)
}

// Height is the number of nodes between a leaf and the root—the number of nodes that need to be retrieved to do a Get() in the worst case.
func (s DB) Height() int {
	return int(s.crdt.Mast.Height())
}

// Size returns the number of entries and tombstones in this tree.
func (s DB) Size() uint64 {
	return s.crdt.Mast.Size()
}

// Diff shows the differences from the the given tree to this one,
// invoking the given callback for each added/removed/changed value.
func (s DB) Diff(
	ctx context.Context,
	from *DB,
	f func(key, myValue, fromValue interface{}) (keepGoing bool, err error),
) error {
	var fromMast *mast.Mast = nil
	if from != nil {
		fromMast = from.crdt.Mast
	}
	err := s.crdt.Mast.DiffIter(ctx, fromMast,
		func(added, removed bool,
			key, addedValue, removedValue interface{},
		) (keepGoing bool, err error) {
			myValue := innerValue(addedValue)
			fromValue := innerValue(removedValue)
			if reflect.DeepEqual(myValue, fromValue) {
				return true, nil
			}
			return f(key, myValue, fromValue)
		})
	return err
}

func innerValue(v interface{}) interface{} {
	if cv, ok := v.(crdtpub.Value); ok {
		if cv.TombstoneSinceEpochNanos != 0 {
			return nil
		}
		return cv.Value
	}
	return nil
}

// RemoveTombstones gets rid of the old tombstones, in the current version. The
// time should be chosen such that there are not going to be any merges for
// affected entries, otherwise merged sets will start new rounds of values for
// the affected entries. For example, if it would be valid for clients to retry
// hour-old requests, 'before' should be at least an hour ago.
func (s *DB) RemoveTombstones(ctx context.Context, before time.Time) error {
	sc, err := s.Clone(ctx)
	if err != nil {
		return fmt.Errorf("clone: %w", err)
	}
	cutoff := before.UnixNano()
	origSize := s.Size()
	err = sc.crdt.Mast.DiffIter(ctx, nil, func(added, removed bool, key, addedValue, removedValue interface{}) (keepGoing bool, err error) {
		cv := addedValue.(crdtpub.Value)
		ts := cv.TombstoneSinceEpochNanos
		if ts == 0 || ts >= cutoff {
			return true, nil
		}
		return true, s.crdt.Mast.Delete(ctx, key, addedValue)
	})
	if err != nil {
		return err
	}
	if s.Size() < origSize {
		s.tombstoned = true
	}
	return nil
}

// DeleteeHistoricalVersionsAndData deletes storage associated with old
// versions, and any nodes from those versions that are no longer necessary for
// reading later versions. Deletion reduces the number of objects in the bucket
// but means that attempts to Diff(), TraceHistory(), or merge versions before
// the cutoff will fail.
func DeleteHistoricVersions(ctx context.Context, s *DB, before time.Time) error {
	if s.readonly {
		return ErrReadOnly
	}
	roots, nodes, err := s.getHistoricRootsAndNodes(ctx, before)
	if err != nil {
		return err
	}
	for _, l := range nodes {
		_, err := s.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Key:    aws.String(s.persist.(*persistEncryptor).Prefix + l),
			Bucket: aws.String(s.persist.(*persistEncryptor).BucketName),
		})
		if err != nil {
			return err
		}
	}
	for _, l := range roots {
		_, err := s.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
			Key:    aws.String(s.merged.Prefix + l),
			Bucket: aws.String(s.merged.BucketName),
		})
		if err != nil {
			return err
		}
	}
	// An empty current root is equivalent to a nonexistent root,
	// so we could delete it too if it meets the history requirement.
	if s.crdt.Source != nil && !s.IsDirty() && s.Size() == 0 {
		root, _, err := loadRoot(ctx, s.root, *s.crdt.Source)
		if err == nil && root.Created.Before(before) {
			_, err := s.s3Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Key:    aws.String(s.root.Prefix + *s.crdt.Source),
				Bucket: aws.String(s.root.BucketName),
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type dbAndCutoff struct {
	db     *DB
	cutoff int64
}

// TraceHistory invokes the given callback for each historic value of 'key', up
// to the last RemoveTombstones().
func (s *DB) TraceHistory(
	ctx context.Context,
	key interface{},
	after time.Time,
	cb func(when time.Time, value interface{}) (keepGoing bool, err error),
) error {
	round := []dbAndCutoff{{s, math.MaxInt64}}
	for len(round) > 0 {
		nextRound := []dbAndCutoff{}
		for _, r := range round {
			var gv crdtpub.Value
			contains, err := r.db.crdt.Mast.Get(ctx, key, &gv)
			if err != nil {
				return fmt.Errorf("crdt.get: %w", err)
			}
			if !contains || gv.ModEpochNanos >= r.cutoff {
				continue
			}
			when := time.Unix(0, gv.ModEpochNanos)
			if time.Unix(0, gv.ModEpochNanos).Before(after) {
				continue
			}
			keepGoing, err := cb(when, gv.Value)
			if err != nil {
				return fmt.Errorf("cb: %w", err)
			}
			if !keepGoing {
				return nil
			}
			if gv.PreviousRoot == "" {
				continue
			}
			prev, err := Open(ctx, s.s3Client, *s.cfg, OpenOptions{ReadOnly: true, SingleVersion: gv.PreviousRoot}, time.Time{})
			if err != nil {
				return fmt.Errorf("open previous root: %s: %w", gv.PreviousRoot, err)
			}
			nextRound = append(nextRound, dbAndCutoff{prev, gv.ModEpochNanos})
			for _, source := range prev.crdt.MergeSources {
				prev, err := Open(ctx, s.s3Client, *s.cfg, OpenOptions{ReadOnly: true, SingleVersion: source}, time.Time{})
				if err != nil {
					return fmt.Errorf("open previous source %s: %w", source, err)
				}
				nextRound = append(nextRound, dbAndCutoff{prev, gv.ModEpochNanos})
			}
		}

		trimmed := []dbAndCutoff{}
		queuedOrDone := map[string]bool{}
		for _, r := range round {
			queuedOrDone[r.root()] = true
		}
		for _, r := range nextRound {
			if !queuedOrDone[r.root()] {
				trimmed = append(trimmed, r)
				queuedOrDone[r.root()] = true
			}
		}
		round = trimmed
	}
	return nil
}

func (r *dbAndCutoff) root() string {
	if r.db.crdt.Source == nil {
		return ""
	}
	return *r.db.crdt.Source
}

func (d *DB) BranchFactor() uint {
	return d.crdt.Mast.BranchFactor()
}

type Cursor struct {
	*mast.Cursor
}

func (d *DB) Cursor(ctx context.Context) (*Cursor, error) {
	inner, err := d.crdt.Mast.Cursor(ctx)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		Cursor: inner,
	}, nil
}

// Get overrides mast.Cursor.Get() to make the crdt.Value wrapping clear.
func (c *Cursor) Get() (interface{}, *crdtpub.Value, bool) {
	innerKey, innerValue, ok := c.Cursor.Get()
	if !ok {
		return nil, nil, false
	}
	v := innerValue.(crdtpub.Value)
	return innerKey, &v, true
}
