// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	tagSharedFileEditV1 = 0x01

	maxSharedManifestSize = 2 << 20 // 2MB
)

// SharedFileMetadata holds all fields required to resolve paths to shared files.
// Only sstables will be referenced using this struct. SharedFileMetadata
// is encoded into a separated shared file manifest managed by the provider.
type SharedFileMetadata struct {
	// FileNum is the filenum of this sstable in the current instance of Pebble.
	FileNum base.FileNum
	// PhysicalFileNum is the filenum of the sstable at creation time in the
	// creator instance of Pebble.
	PhysicalFileNum base.FileNum
	// CreatorInstanceID is the instanceID of the Pebble instance that created
	// this shared file. See opts.Experimental.InstanceID.
	CreatorInstanceID uint64
}

// SharedFileEdit represents one mutation to the Provider.mu.sharedFiles map.
type SharedFileEdit struct {
	AddedFiles   []SharedFileMetadata
	RemovedFiles []base.FileNum
}

type sharedFileEditEncoder struct {
	*bytes.Buffer
}

func (e sharedFileEditEncoder) writeUvarint(u uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], u)
	e.Write(buf[:n])
}

func (s *SharedFileEdit) Encode(w io.Writer) error {
	e := sharedFileEditEncoder{new(bytes.Buffer)}
	e.Write([]byte{byte(tagSharedFileEditV1)})

	e.writeUvarint(uint64(len(s.AddedFiles)))
	for i := range s.AddedFiles {
		e.writeUvarint(uint64(s.AddedFiles[i].FileNum))
		e.writeUvarint(uint64(s.AddedFiles[i].PhysicalFileNum))
		e.writeUvarint(s.AddedFiles[i].CreatorInstanceID)
	}

	e.writeUvarint(uint64(len(s.RemovedFiles)))
	for i := range s.RemovedFiles {
		e.writeUvarint(uint64(s.RemovedFiles[i]))
	}

	_, err := w.Write(e.Bytes())
	return err
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

type sharedFileEditDecoder struct {
	byteReader
}

func (d sharedFileEditDecoder) readUvarint() (uint64, error) {
	u, err := binary.ReadUvarint(d)
	if err != nil {
		if err == io.EOF {
			return 0, base.CorruptionErrorf("corrupt shared file manifest")
		}
		return 0, err
	}
	return u, nil
}

func (s *SharedFileEdit) Decode(r io.Reader) error {
	br, ok := r.(byteReader)
	if !ok {
		br = bufio.NewReader(r)
	}
	d := sharedFileEditDecoder{br}
	tag, err := d.ReadByte()
	if err != nil {
		return err
	}
	if tag != tagSharedFileEditV1 {
		return base.CorruptionErrorf("unexpected tag on shared file edit %x", tag)
	}
	lenAdded, err := d.readUvarint()
	if err != nil {
		return err
	}
	s.AddedFiles = s.AddedFiles[:0]
	for i := uint64(0); i < lenAdded; i++ {
		var m SharedFileMetadata
		var fileNum, physicalFileNum uint64
		fileNum, err = d.readUvarint()
		if err != nil {
			return err
		}
		physicalFileNum, err = d.readUvarint()
		if err != nil {
			return err
		}
		m.CreatorInstanceID, err = d.readUvarint()
		if err != nil {
			return err
		}
		m.FileNum = base.FileNum(fileNum)
		m.PhysicalFileNum = base.FileNum(physicalFileNum)
		s.AddedFiles = append(s.AddedFiles, m)
	}
	lenRemoved, err := d.readUvarint()
	if err != nil {
		return err
	}
	s.RemovedFiles = s.RemovedFiles[:0]
	for i := uint64(0); i < lenRemoved; i++ {
		var fileNum uint64
		fileNum, err = d.readUvarint()
		if err != nil {
			return err
		}
		s.RemovedFiles = append(s.RemovedFiles, base.FileNum(fileNum))
	}
	return nil
}

// Provider is a singleton object used to access and manage objects.
//
// An object is conceptually like a large immutable file. The main use of
// objects is for storing sstables; in the future it could also be used for blob
// storage.
//
// Objects are currently backed by a vfs.File.
type Provider struct {
	st Settings

	mu struct {
		sync.RWMutex

		sharedFiles map[base.FileNum]SharedFileMetadata
	}

	sharedManifest io.WriteCloser

	// TODO(radu): add more functionality around listing, copying, linking, etc.
}

// Readable is the handle for an object that is open for reading.
type Readable interface {
	io.ReaderAt
	io.Closer

	// Size returns the size of the object.
	Size() int64

	// NewReadaheadHandle creates a read-ahead handle which encapsulates
	// read-ahead state. To benefit from read-ahead, ReadaheadHandle.ReadAt must
	// be used (as opposed to Readable.ReadAt).
	//
	// The ReadaheadHandle must be closed before the Readable is closed.
	//
	// Multiple separate ReadaheadHandles can be used.
	NewReadaheadHandle() ReadaheadHandle
}

// ReadaheadHandle is used to perform reads that might benefit from read-ahead.
type ReadaheadHandle interface {
	io.ReaderAt
	io.Closer

	// MaxReadahead configures the implementation to expect large sequential
	// reads. Used to skip any initial read-ahead ramp-up.
	MaxReadahead()

	// RecordCacheHit informs the implementation that we were able to retrieve a
	// block from cache.
	RecordCacheHit(offset, size int64)
}

// Writable is the handle for an object that is open for writing.
type Writable interface {
	// Unlike the specification for io.Writer.Write(), the Writable.Write()
	// method *is* allowed to modify the slice passed in, whether temporarily
	// or permanently. Callers of Write() need to take this into account.
	io.Writer
	io.Closer

	Sync() error
}

// Settings that must be specified when creating the Provider.
type Settings struct {
	// Local filesystem configuration.
	FS        vfs.FS
	FSDirName string

	// NoSyncOnClose decides whether the implementation will enforce a
	// close-time synchronization (e.g., fdatasync() or sync_file_range())
	// on files it writes to. Setting this to true removes the guarantee for a
	// sync on close. Some implementations can still issue a non-blocking sync.
	NoSyncOnClose bool

	// BytesPerSync enables periodic syncing of files in order to smooth out
	// writes to disk. This option does not provide any persistence guarantee, but
	// is used to avoid latency spikes if the OS automatically decides to write
	// out a large chunk of dirty filesystem buffers.
	BytesPerSync int
}

// DefaultSettings initializes default settings, suitable for tests and tools.
func DefaultSettings(fs vfs.FS, dirName string) Settings {
	return Settings{
		FS:            fs,
		FSDirName:     dirName,
		NoSyncOnClose: false,
		BytesPerSync:  512 * 1024, // 512KB
	}
}

// New creates the Provider.
func New(settings Settings) (*Provider, error) {
	p := &Provider{
		st: settings,
	}
	if err := p.loadSharedFiles(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Provider) loadSharedFiles() error {
	ls, err := p.st.FS.List(p.st.FSDirName)
	if err != nil {
		return err
	}

	highestFileNum := uint64(0)
	var providerFilename string
	for _, filename := range ls {
		ft, fn, ok := base.ParseFilename(p.st.FS, filename)
		if !ok {
			continue
		}
		switch ft {
		case base.FileTypeProvider:
			if highestFileNum < uint64(fn) {
				highestFileNum = uint64(fn)
				providerFilename = filename
			}
		}
	}
	if providerFilename == "" {
		// Empty map.
		return nil
	}
	f, err := p.st.FS.Open(providerFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	rr := record.NewReader(f, 0)
	for {
		r, err := rr.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		var sfe SharedFileEdit
		if err := sfe.Decode(r); err != nil {
			return err
		}
		for i := range sfe.AddedFiles {
			p.mu.sharedFiles[sfe.AddedFiles[i].FileNum] = sfe.AddedFiles[i]
		}
		for i := range sfe.RemovedFiles {
			delete(p.mu.sharedFiles, sfe.RemovedFiles[i])
		}
	}
}

func (p *Provider) addSharedFileEdit(sfe *SharedFileEdit) {
	var buf bytes.Buffer
	sfe.Encode(&buf)
	buf.Len()
}

// Path returns an internal path for an object. It is used for informative
// purposes (e.g. logging).
func (p *Provider) Path(fileType base.FileType, fileNum base.FileNum) string {
	return base.MakeFilepath(p.st.FS, p.st.FSDirName, fileType, fileNum)
}

// OpenForReading opens an existing object.
func (p *Provider) OpenForReading(fileType base.FileType, fileNum base.FileNum) (Readable, error) {
	var filename string
	p.mu.RLock()
	// Manually inline p.Path above to avoid unlocking/relocking.
	//
	// TODO(bilal): add a pathLocked that does this.
	if sfm, ok := p.mu.sharedFiles[fileNum]; ok {
		// TODO(bilal): swap out the FS with the shared file.
		filename = base.MakeFilepath(p.st.FS, p.st.FSDirName, fileType, sfm.PhysicalFileNum)
	} else {
		filename = p.Path(fileType, fileNum)
	}
	p.mu.RUnlock()
	file, err := p.st.FS.Open(filename, vfs.RandomReadsOption)
	if err != nil {
		return nil, err
	}
	if fd := file.Fd(); fd != vfs.InvalidFd {
		return newFileReadable(file, fd, p.st.FS, filename)
	}
	return newGenericFileReadable(file)
}

// NewFileInfo contains any information the provider could need to optimize
// placement of this file (eg. in local or shared storage), such as the level
// of this sstable. Note that these parameters of an sstable could change
// during the lifetime of this file (eg. move compactions).
type NewFileInfo struct {
	// Level is the level where this sstable will reside. Negative means unknown,
	// and values over numLevels are invalid.
	Level int
}

// Create creates a new object and opens it for writing.
func (p *Provider) Create(fileType base.FileType, fileNum base.FileNum, fileInfo NewFileInfo) (Writable, error) {
	file, err := p.st.FS.Create(p.Path(fileType, fileNum))
	if err != nil {
		return nil, err
	}
	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: p.st.NoSyncOnClose,
		BytesPerSync:  p.st.BytesPerSync,
	})
	return newFileBufferedWritable(file), nil
}

// Remove removes an object.
func (p *Provider) Remove(fileType base.FileType, fileNum base.FileNum) error {
	return p.st.FS.Remove(p.Path(fileType, fileNum))
}
