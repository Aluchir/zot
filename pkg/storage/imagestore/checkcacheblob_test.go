package imagestore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/distribution/distribution/v3/registry/storage/driver"
	godigest "github.com/opencontainers/go-digest"
	ispec "github.com/opencontainers/image-spec/specs-go/v1"
	. "github.com/smartystreets/goconvey/convey"

	"zotregistry.dev/zot/v2/pkg/extensions/monitoring"
	"zotregistry.dev/zot/v2/pkg/log"
	"zotregistry.dev/zot/v2/pkg/storage/cache"
	"zotregistry.dev/zot/v2/pkg/storage/constants"
	"zotregistry.dev/zot/v2/pkg/storage/gcs"
	"zotregistry.dev/zot/v2/pkg/storage/imagestore"
	"zotregistry.dev/zot/v2/pkg/test/mocks"
)

// objectStoreMock is a minimal in-memory object store: a flat map of object paths to
// bytes plus the directory prefixes they imply, so List/Walk/Stat behave like a remote
// prefix-based backend rather than a filesystem. Unlike makeStatefulMigrationStoreMock
// it holds an arbitrary set of repos, which is what the multi-repo dedupe layout this
// file exercises needs.
func objectStoreMock(rootDir string, files map[string][]byte) *mocks.StorageDriverMock {
	objects := map[string][]byte{}
	for objectPath, content := range files {
		objects[objectPath] = append([]byte(nil), content...)
	}

	dirs := map[string]struct{}{}

	ensureParents := func(objectPath string) {
		parent := path.Dir(objectPath)
		for parent != "." && parent != "/" && strings.HasPrefix(parent, rootDir) {
			dirs[parent] = struct{}{}
			parent = path.Dir(parent)
		}

		dirs[rootDir] = struct{}{}
	}

	for objectPath := range objects {
		ensureParents(objectPath)
	}

	listUnder := func(prefix string) []string {
		seen := map[string]struct{}{}
		entries := []string{}

		add := func(entry string) {
			if path.Dir(entry) != prefix {
				return
			}

			if _, ok := seen[entry]; ok {
				return
			}

			seen[entry] = struct{}{}
			entries = append(entries, entry)
		}

		for dir := range dirs {
			add(dir)
		}

		for objectPath := range objects {
			add(objectPath)
		}

		sort.Strings(entries)

		return entries
	}

	var mutex sync.Mutex

	return &mocks.StorageDriverMock{
		GetContentFn: func(_ context.Context, objectPath string) ([]byte, error) {
			mutex.Lock()
			defer mutex.Unlock()

			content, ok := objects[objectPath]
			if !ok {
				return nil, driver.PathNotFoundError{Path: objectPath}
			}

			return append([]byte(nil), content...), nil
		},
		StatFn: func(_ context.Context, objectPath string) (driver.FileInfo, error) {
			mutex.Lock()
			defer mutex.Unlock()

			if content, ok := objects[objectPath]; ok {
				size := int64(len(content))

				return &mocks.FileInfoMock{
					IsDirFn: func() bool { return false },
					PathFn:  func() string { return objectPath },
					SizeFn:  func() int64 { return size },
				}, nil
			}

			if _, ok := dirs[objectPath]; ok {
				return &mocks.FileInfoMock{
					IsDirFn: func() bool { return true },
					PathFn:  func() string { return objectPath },
					SizeFn:  func() int64 { return 0 },
				}, nil
			}

			return nil, driver.PathNotFoundError{Path: objectPath}
		},
		ListFn: func(_ context.Context, objectPath string) ([]string, error) {
			mutex.Lock()
			defer mutex.Unlock()

			if _, ok := dirs[objectPath]; !ok {
				return nil, driver.PathNotFoundError{Path: objectPath}
			}

			return listUnder(objectPath), nil
		},
		WalkFn: func(_ context.Context, root string, walkFn driver.WalkFn,
			_ ...func(*driver.WalkOptions),
		) error {
			mutex.Lock()

			entries := []string{}

			for dir := range dirs {
				if dir != root && strings.HasPrefix(dir, root+"/") {
					entries = append(entries, dir)
				}
			}

			sort.Strings(entries)
			mutex.Unlock()

			skipped := []string{}

			for _, entry := range entries {
				dir := entry

				isSkipped := false

				for _, prefix := range skipped {
					if strings.HasPrefix(dir, prefix+"/") {
						isSkipped = true

						break
					}
				}

				if isSkipped {
					continue
				}

				err := walkFn(&mocks.FileInfoMock{
					IsDirFn: func() bool { return true },
					PathFn:  func() string { return dir },
					SizeFn:  func() int64 { return 0 },
				})
				if err != nil {
					if errors.Is(err, driver.ErrSkipDir) {
						skipped = append(skipped, dir)

						continue
					}

					return err
				}
			}

			return nil
		},
		ReaderFn: func(_ context.Context, objectPath string, offset int64) (io.ReadCloser, error) {
			mutex.Lock()
			defer mutex.Unlock()

			content, ok := objects[objectPath]
			if !ok {
				return nil, driver.PathNotFoundError{Path: objectPath}
			}

			return io.NopCloser(bytes.NewReader(content[offset:])), nil
		},
		WriterFn: func(_ context.Context, objectPath string, isAppend bool) (driver.FileWriter, error) {
			mutex.Lock()
			base := []byte(nil)

			if isAppend {
				base = append(base, objects[objectPath]...)
			}
			mutex.Unlock()

			buf := bytes.NewBuffer(base)

			return &mocks.FileWriterMock{
				WriteFn: func(content []byte) (int, error) { return buf.Write(content) },
				CommitFn: func() error {
					mutex.Lock()
					defer mutex.Unlock()

					ensureParents(objectPath)

					objects[objectPath] = append([]byte(nil), buf.Bytes()...)

					return nil
				},
			}, nil
		},
		PutContentFn: func(_ context.Context, objectPath string, content []byte) error {
			mutex.Lock()
			defer mutex.Unlock()

			ensureParents(objectPath)

			objects[objectPath] = append([]byte(nil), content...)

			return nil
		},
		DeleteFn: func(_ context.Context, objectPath string) error {
			mutex.Lock()
			defer mutex.Unlock()

			_, isObject := objects[objectPath]
			if _, isDir := dirs[objectPath]; !isObject && !isDir {
				return driver.PathNotFoundError{Path: objectPath}
			}

			delete(objects, objectPath)
			delete(dirs, objectPath)

			return nil
		},
	}
}

// TestCheckCacheBlobAfterBlobstoreMigration covers the first reads issued against a
// store that has just been upgraded to the global blobstore while carrying a cache
// populated by a pre-blobstore release. There the cache's "original" record for a
// digest is a per-repository path, and the upgrade deletes exactly those objects once
// the payload is promoted, so the original record names an object that no longer
// exists. checkCacheBlob must resolve such a record to the global blobstore rather than
// report the blob missing and prune the repository's ownership reference.
func TestCheckCacheBlobAfterBlobstoreMigration(t *testing.T) {
	Convey("A store carrying a pre-blobstore cache serves reads right after the upgrade", t, func() {
		logger := log.NewTestLogger()
		metrics := monitoring.NewMetricsServer(false, logger)

		rootDir := "/oci-repo-test/post-migration-reads"
		promotedRepo := "golang"
		dedupedRepo := "mirror/golang"
		content := []byte("layer-content-owned-by-two-repos")
		digest := godigest.FromBytes(content)

		blobRelPath := path.Join(ispec.ImageBlobsDir, digest.Algorithm().String(), digest.Encoded())
		promotedBlobPath := path.Join(rootDir, promotedRepo, blobRelPath)
		dedupedBlobPath := path.Join(rootDir, dedupedRepo, blobRelPath)

		storeMock := objectStoreMock(rootDir, map[string][]byte{
			path.Join(rootDir, promotedRepo, ispec.ImageIndexFile):  []byte("{}"),
			path.Join(rootDir, promotedRepo, ispec.ImageLayoutFile): []byte("{}"),
			promotedBlobPath: content,
			path.Join(rootDir, dedupedRepo, ispec.ImageIndexFile):  []byte("{}"),
			path.Join(rootDir, dedupedRepo, ispec.ImageLayoutFile): []byte("{}"),
			// pre-blobstore remote dedupe left an empty placeholder object behind
			dedupedBlobPath: {},
		})

		cacheDriver, err := cache.NewBoltDBCache(cache.BoltDBDriverParameters{
			RootDir:     t.TempDir(),
			Name:        "cache",
			UseRelPaths: false,
		}, logger)
		So(err, ShouldBeNil)

		// the pre-blobstore cache: the repo that held the real bytes is the "original",
		// the deduped repo is a duplicate
		So(cacheDriver.PutBlob(digest, promotedBlobPath), ShouldBeNil)
		So(cacheDriver.PutBlob(digest, dedupedBlobPath), ShouldBeNil)

		store := imagestore.NewImageStore(rootDir, "", true, false, logger, metrics, nil,
			gcs.New(storeMock), cacheDriver, nil, nil)
		So(store, ShouldNotBeNil)

		globalBlobPath := path.Join(rootDir, constants.GlobalBlobsRepo, blobRelPath)

		globalContent, err := storeMock.GetContent(context.Background(), globalBlobPath)
		So(err, ShouldBeNil)
		So(globalContent, ShouldResemble, content)

		// the upgrade left the stale per-repo path as the cache's original record
		originalRecord, err := cacheDriver.GetBlob(digest)
		So(err, ShouldBeNil)
		So(originalRecord, ShouldEqual, promotedBlobPath)

		_, err = storeMock.GetContent(context.Background(), promotedBlobPath)
		So(err, ShouldNotBeNil)

		Convey("the very first CheckBlob for each repo finds the promoted payload", func() {
			for _, repo := range []string{promotedRepo, dedupedRepo} {
				found, size, err := store.CheckBlob(context.Background(), repo, digest)
				So(err, ShouldBeNil)
				So(found, ShouldBeTrue)
				So(size, ShouldEqual, int64(len(content)))
			}
		})

		Convey("a failed lookup never drops a repository's ownership reference", func() {
			blobs, err := store.GetAllBlobs(promotedRepo)
			So(err, ShouldBeNil)
			So(blobs, ShouldResemble, []godigest.Digest{digest})

			// the enumeration is asserted before the read's own result so this stays a
			// check on the reference index rather than a duplicate of the case above
			_, _, checkErr := store.CheckBlob(context.Background(), promotedRepo, digest)

			// GetAllBlobs feeds garbage collection's stale-manifest and unreferenced-blob
			// decisions, so a read must never be able to empty it
			blobs, err = store.GetAllBlobs(promotedRepo)
			So(err, ShouldBeNil)
			So(blobs, ShouldResemble, []godigest.Digest{digest})
			So(checkErr, ShouldBeNil)
		})

		Convey("GetBlob still streams the payload for both repos", func() {
			for _, repo := range []string{promotedRepo, dedupedRepo} {
				_, _, err := store.CheckBlob(context.Background(), repo, digest)
				So(err, ShouldBeNil)

				reader, size, err := store.GetBlob(repo, digest, ispec.MediaTypeImageLayerGzip)
				So(err, ShouldBeNil)
				So(size, ShouldEqual, int64(len(content)))

				streamed, err := io.ReadAll(reader)
				So(err, ShouldBeNil)
				So(streamed, ShouldResemble, content)
				So(reader.Close(), ShouldBeNil)
			}
		})

		Convey("a digest whose payload is gone everywhere is still reported missing", func() {
			So(storeMock.Delete(context.Background(), globalBlobPath), ShouldBeNil)

			found, _, err := store.CheckBlob(context.Background(), promotedRepo, digest)
			So(err, ShouldNotBeNil)
			So(found, ShouldBeFalse)
		})
	})
}
