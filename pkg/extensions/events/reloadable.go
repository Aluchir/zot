package events

import (
	"sync/atomic"
)

// ReloadableRecorder is a Recorder whose delegate can be replaced while the
// server runs. The image stores and the CVE scanner are handed this wrapper
// once, so swapping the delegate reaches all of them without touching any
// emit site. A nil delegate records nothing, which is how a build or a
// configuration without events behaves.
type ReloadableRecorder struct {
	delegate atomic.Pointer[recorderBox]
}

// recorderBox lets the delegate be nil and change concrete type between swaps,
// neither of which storing the interface directly would allow.
type recorderBox struct {
	recorder Recorder
}

var _ Recorder = (*ReloadableRecorder)(nil)

func NewReloadableRecorder(recorder Recorder) *ReloadableRecorder {
	reloadable := &ReloadableRecorder{}
	reloadable.delegate.Store(&recorderBox{recorder: recorder})

	return reloadable
}

// Swap installs recorder and returns the delegate it replaced, which the caller
// closes once it is done with it. Events already in flight to the old sinks are
// published on a best-effort basis, as they are on any other publish.
func (r *ReloadableRecorder) Swap(recorder Recorder) Recorder {
	previous := r.delegate.Swap(&recorderBox{recorder: recorder})
	if previous == nil {
		return nil
	}

	return previous.recorder
}

// current also tolerates a nil wrapper, so a typed nil handed out as a Recorder
// records nothing instead of panicking.
func (r *ReloadableRecorder) current() Recorder {
	if r == nil {
		return nil
	}

	if box := r.delegate.Load(); box != nil {
		return box.recorder
	}

	return nil
}

func (r *ReloadableRecorder) Close() {
	if recorder := r.current(); recorder != nil {
		recorder.Close()
	}
}

func (r *ReloadableRecorder) RepositoryCreated(name string, ectx *EventContext) {
	if recorder := r.current(); recorder != nil {
		recorder.RepositoryCreated(name, ectx)
	}
}

func (r *ReloadableRecorder) ImageUpdated(name, reference, digest, mediaType, manifest string, ectx *EventContext) {
	if recorder := r.current(); recorder != nil {
		recorder.ImageUpdated(name, reference, digest, mediaType, manifest, ectx)
	}
}

func (r *ReloadableRecorder) ImageDeleted(name, reference, digest, mediaType string, ectx *EventContext) {
	if recorder := r.current(); recorder != nil {
		recorder.ImageDeleted(name, reference, digest, mediaType, ectx)
	}
}

func (r *ReloadableRecorder) ImageLintFailed(name, reference, digest, mediaType, manifest string, ectx *EventContext) {
	if recorder := r.current(); recorder != nil {
		recorder.ImageLintFailed(name, reference, digest, mediaType, manifest, ectx)
	}
}

func (r *ReloadableRecorder) ImageScanned(name, reference, digest, mediaType string,
	summary ImageScanSummary, ectx *EventContext,
) {
	if recorder := r.current(); recorder != nil {
		recorder.ImageScanned(name, reference, digest, mediaType, summary, ectx)
	}
}
