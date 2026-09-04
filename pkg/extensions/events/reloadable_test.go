package events_test

import (
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"zotregistry.dev/zot/v2/pkg/extensions/events"
)

// countingRecorder counts what it was asked to publish, so a test can tell
// which delegate an event reached.
type countingRecorder struct {
	created atomic.Int64
	updated atomic.Int64
	closed  atomic.Bool
}

func (r *countingRecorder) Close() { r.closed.Store(true) }

func (r *countingRecorder) RepositoryCreated(string, *events.EventContext) { r.created.Add(1) }

func (r *countingRecorder) ImageUpdated(_, _, _, _, _ string, _ *events.EventContext) {
	r.updated.Add(1)
}

func (r *countingRecorder) ImageDeleted(_, _, _, _ string, _ *events.EventContext) {}

func (r *countingRecorder) ImageLintFailed(_, _, _, _, _ string, _ *events.EventContext) {}

func (r *countingRecorder) ImageScanned(_, _, _, _ string, _ events.ImageScanSummary,
	_ *events.EventContext,
) {
}

func TestReloadableRecorder(t *testing.T) {
	Convey("A swap routes later events to the new delegate", t, func() {
		first, second := &countingRecorder{}, &countingRecorder{}
		reloadable := events.NewReloadableRecorder(first)

		reloadable.RepositoryCreated("repo", nil)
		So(first.created.Load(), ShouldEqual, 1)

		replaced := reloadable.Swap(second)
		So(replaced, ShouldEqual, first)

		reloadable.RepositoryCreated("repo", nil)
		reloadable.ImageUpdated("repo", "tag", "digest", "mediaType", "manifest", nil)

		So(first.created.Load(), ShouldEqual, 1)
		So(second.created.Load(), ShouldEqual, 1)
		So(second.updated.Load(), ShouldEqual, 1)
	})

	Convey("Events are dropped while no delegate is installed", t, func() {
		recorder := &countingRecorder{}
		reloadable := events.NewReloadableRecorder(nil)

		So(func() { reloadable.RepositoryCreated("repo", nil) }, ShouldNotPanic)
		So(func() { reloadable.Close() }, ShouldNotPanic)

		// disabled at startup, enabled by a reload
		So(reloadable.Swap(recorder), ShouldBeNil)
		reloadable.RepositoryCreated("repo", nil)
		So(recorder.created.Load(), ShouldEqual, 1)

		// and disabled again by a later reload
		So(reloadable.Swap(nil), ShouldEqual, recorder)
		reloadable.RepositoryCreated("repo", nil)
		So(recorder.created.Load(), ShouldEqual, 1)
	})

	Convey("Close reaches the current delegate", t, func() {
		recorder := &countingRecorder{}
		reloadable := events.NewReloadableRecorder(recorder)

		reloadable.Close()
		So(recorder.closed.Load(), ShouldBeTrue)
	})

	Convey("A nil wrapper records nothing instead of panicking", t, func() {
		var reloadable *events.ReloadableRecorder

		So(func() { reloadable.RepositoryCreated("repo", nil) }, ShouldNotPanic)
		So(func() { reloadable.Close() }, ShouldNotPanic)
	})

	Convey("Publishing while swapping is race free", t, func() {
		reloadable := events.NewReloadableRecorder(&countingRecorder{})

		var waitGroup sync.WaitGroup

		waitGroup.Add(2)

		go func() {
			defer waitGroup.Done()

			for range 200 {
				reloadable.RepositoryCreated("repo", nil)
			}
		}()

		go func() {
			defer waitGroup.Done()

			for range 200 {
				reloadable.Swap(&countingRecorder{})
			}
		}()

		waitGroup.Wait()
	})
}
