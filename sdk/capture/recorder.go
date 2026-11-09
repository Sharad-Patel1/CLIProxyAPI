// Package capture records observations without coupling inference to disk or network availability.
// The private spool is staging data. The uploader reconstructs and redacts complete
// bodies before any content leaves the machine. Never expose the spool to clients.
package capture

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MoeForge/moego/base64"
	"github.com/MoeForge/moego/ojson"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const chunkBytes = 32 << 10

type observation struct {
	at                                time.Time
	kind, request, invocation, stream string
	metadata                          map[string]string
	data                              []byte
}

// Recorder owns its worker and private spool. Submit never performs I/O. Accepted
// observations remain volatile until a sealed segment has been fsynced.
type Recorder struct {
	dir, boot                  string
	maxQueued                  int64
	maxSpool                   int64
	queue                      chan observation
	queued                     atomic.Int64
	attempted, dropped, failed atomic.Uint64
	gate                       sync.RWMutex
	closed                     bool
	stop, done                 chan struct{}
}

func NewRecorder(dir string, maxQueued int64) *Recorder {
	if maxQueued <= 0 {
		maxQueued = 64 << 20
	}
	r := &Recorder{dir: dir, boot: uuid.NewString(), maxQueued: maxQueued,
		maxSpool: 8 << 30,
		queue:    make(chan observation, 2048), stop: make(chan struct{}), done: make(chan struct{})}
	go r.run()
	return r
}

func (r *Recorder) submit(item observation) {
	if r == nil {
		return
	}
	r.gate.RLock()
	defer r.gate.RUnlock()
	if r.closed {
		return
	}
	r.attempted.Add(1)
	size := int64(len(item.data) + 1024)
	for {
		queued := r.queued.Load()
		if size > r.maxQueued-queued {
			r.dropped.Add(1)
			return
		}
		if r.queued.CompareAndSwap(queued, queued+size) {
			break
		}
	}
	item.data = bytes.Clone(item.data)
	item.at = time.Now().UTC()
	select {
	case r.queue <- item:
	default:
		r.queued.Add(-size)
		r.dropped.Add(1)
	}
}

// Close stops accepting observations, drains the queue, and seals its final segment.
// Call it only after requests have stopped; it may wait for disk I/O.
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.gate.Lock()
	if !r.closed {
		r.closed = true
		close(r.stop)
	}
	r.gate.Unlock()
	<-r.done
}

func (r *Recorder) encode(item observation, sequence uint64) []byte {
	doc := ojson.NewDocString(`{}`)
	_ = doc.Set("version", 1)
	_ = doc.Set("boot_id", r.boot)
	_ = doc.Set("sequence", sequence)
	_ = doc.Set("occurred_at", item.at.Format(time.RFC3339Nano))
	_ = doc.Set("kind", item.kind)
	_ = doc.Set("request_id", item.request)
	_ = doc.Set("invocation_id", item.invocation)
	_ = doc.Set("stream_id", item.stream)
	if len(item.metadata) > 0 {
		_ = doc.Set("metadata", item.metadata)
	}
	if len(item.data) > 0 {
		_ = doc.Set("data_b64", base64.StdEncoding.EncodeToString(item.data))
	}
	return append(doc.Bytes(), '\n')
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func (r *Recorder) run() {
	defer close(r.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var file *os.File
	var filePath string
	var sequence, durable uint64
	var contiguous uint64
	broken := false
	var lastErr string
	var previous []byte
	initialized := false
	var spoolBytes int64
	var spaceErr error
	measure := func() error {
		entries, err := os.ReadDir(r.dir)
		if err != nil {
			return err
		}
		spoolBytes = 0
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return err
			}
			spoolBytes += max(info.Size(), 4096)
		}
		return checkFreeSpace(r.dir)
	}

	fail := func() {
		r.failed.Add(1)
		broken = true
		if lastErr == "" {
			log.Error("AI capture persistence failed; inference continues, coverage uncertain")
		}
		lastErr = "PersistenceError"
		if file != nil {
			_ = file.Close()
			file = nil
		}
		if filePath != "" {
			_ = os.Rename(filePath, filePath+".recovered")
		}
	}
	initialize := func() error {
		if err := os.MkdirAll(r.dir, 0700); err != nil {
			return err
		}
		if err := os.Chmod(r.dir, 0700); err != nil {
			return err
		}
		previous, _ = os.ReadFile(filepath.Join(r.dir, "status.json"))
		// Preserve crash leftovers byte for byte. The uploader must report an
		// incomplete tail rather than silently trimming it or claiming coverage.
		paths, err := filepath.Glob(filepath.Join(r.dir, "*.open"))
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := os.Rename(path, path+".recovered"); err != nil {
				return err
			}
		}
		return syncDir(r.dir)
	}
	write := func(item observation) error {
		if spaceErr != nil || spoolBytes >= r.maxSpool {
			return fmt.Errorf("capture disk reserve reached")
		}
		if file == nil {
			filePath = filepath.Join(r.dir, fmt.Sprintf("%s-%020d.open", r.boot, sequence+1))
			var err error
			file, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
		}
		sequence++
		raw := r.encode(item, sequence)
		n, err := file.Write(raw)
		spoolBytes += int64(n)
		if err != nil {
			return err
		}
		if n != len(raw) {
			return fmt.Errorf("short capture write")
		}
		return nil
	}
	seal := func() error {
		if file == nil {
			return nil
		}
		if err := file.Sync(); err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			file = nil
			return err
		}
		file = nil
		if err := os.Rename(filePath, filePath+".sealed"); err != nil {
			return err
		}
		if err := syncDir(r.dir); err != nil {
			return err
		}
		durable = sequence
		if !broken {
			contiguous = durable
		}
		return nil
	}
	status := func(clean bool) error {
		doc := ojson.NewDocString(`{}`)
		_ = doc.Set("boot_id", r.boot)
		_ = doc.Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano))
		_ = doc.Set("last_sealed_sequence", durable)
		_ = doc.Set("contiguous_durable_sequence", contiguous)
		_ = doc.Set("attempted_records", r.attempted.Load())
		_ = doc.Set("queue_bytes", r.queued.Load())
		_ = doc.Set("dropped_records", r.dropped.Load())
		_ = doc.Set("persistence_failures", r.failed.Load())
		_ = doc.Set("error_type", lastErr)
		_ = doc.Set("clean_shutdown", clean && lastErr == "")
		path := filepath.Join(r.dir, "status.tmp")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = f.Write(doc.Bytes())
		if err == nil {
			err = f.Sync()
		}
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		if err = os.Rename(path, filepath.Join(r.dir, "status.json")); err != nil {
			return err
		}
		return syncDir(r.dir)
	}
	process := func(item observation) {
		r.queued.Add(-int64(len(item.data) + 1024))
		if !initialized {
			fail()
			return
		}
		if err := write(item); err != nil {
			fail()
		}
	}
	flush := func(clean bool) {
		if !initialized {
			if err := initialize(); err != nil {
				fail()
				return
			}
			initialized = true
			spaceErr = measure()
			metadata := map[string]string{"previous_status": "unavailable"}
			if ojson.ValidBytes(previous) {
				metadata["previous_status"] = string(previous)
			}
			if err := write(observation{at: time.Now().UTC(), kind: "boot.start", metadata: metadata}); err != nil {
				fail()
			}
		}
		if err := seal(); err != nil {
			fail()
		}
		// Cumulative counters are persisted even after recovery; a later good
		// write cannot erase evidence of a dropped or failed observation.
		if file == nil && durable == sequence && spaceErr == nil && spoolBytes < r.maxSpool {
			lastErr = ""
		}
		if err := status(clean); err != nil {
			fail()
		}
	}
	flush(false)
	for {
		select {
		case item := <-r.queue:
			process(item)
		case <-ticker.C:
			if initialized {
				spaceErr = measure()
			}
			flush(false)
		case <-r.stop:
			for {
				select {
				case item := <-r.queue:
					process(item)
				default:
					flush(true)
					return
				}
			}
		}
	}
}
