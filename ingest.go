// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type fileMetadataAndLevel struct {
	*fileMetadata

	targetLevel int
}

func sstableKeyCompare(userCmp Compare, a, b InternalKey) int {
	c := userCmp(a.UserKey, b.UserKey)
	if c != 0 {
		return c
	}
	if a.IsExclusiveSentinel() {
		if !b.IsExclusiveSentinel() {
			return -1
		}
	} else if b.IsExclusiveSentinel() {
		return +1
	}
	return 0
}

func ingestValidateKey(opts *Options, key *InternalKey, isShared bool) error {
	if key.Kind() == InternalKeyKindInvalid {
		return base.CorruptionErrorf("pebble: external sstable has corrupted key: %s",
			key.Pretty(opts.Comparer.FormatKey))
	}
	if key.SeqNum() != 0 {
		return base.CorruptionErrorf("pebble: external sstable has non-zero seqnum: %s",
			key.Pretty(opts.Comparer.FormatKey))
	}
	return nil
}

func ingestLoad1(
	opts *Options,
	fmv FormatMajorVersion,
	path string,
	smeta SharedSSTMeta,
	isShared bool,
	cacheID uint64,
	fileNum FileNum,
) (*fileMetadataAndLevel, error) {
	if isShared && opts.SharedFS == nil {
		panic("ingestLoad1: function called with shared meta but DB does not have shared fs")
	}

	fs := opts.FS
	if isShared {
		fs = opts.SharedFS
	}

	stat, err := fs.Stat(path)
	if err != nil {
		return nil, err
	}
	if isShared {
		meta := &fileMetadata{}

		meta.FileNum = fileNum
		meta.Size = uint64(stat.Size())
		meta.CreationTime = time.Now().Unix()
		var targetLevel int

		meta.IsShared = true
		meta.HasPointKeys = true
		meta.CreatorUniqueID = smeta.CreatorUniqueID
		meta.PhysicalFileNum = base.FileNum(smeta.PhysicalFileNum)
		targetLevel = 5
		seqNum := sstable.SeqNumL5PointKey
		endSeqNum := sstable.SeqNumL5RangeDel
		if smeta.SourceLevel == 6 {
			targetLevel = 6
			seqNum = sstable.SeqNumL6All
			endSeqNum = sstable.SeqNumL6All
		}
		meta.Smallest.UserKey = append([]byte(nil), smeta.Smallest...)
		meta.Smallest.SetKind(InternalKeyKindSet)
		meta.Smallest.SetSeqNum(uint64(seqNum))
		meta.Largest.UserKey = append([]byte(nil), smeta.Largest...)
		meta.Largest.SetKind(InternalKeyKindSet)
		meta.Largest.SetSeqNum(uint64(endSeqNum))
		meta.SmallestPointKey = meta.Smallest
		meta.LargestPointKey = meta.Largest
		meta.FileSmallest = meta.Smallest
		meta.FileLargest = meta.Largest

		// Sanity check that the various bounds on the file were set consistently.
		if err := meta.Validate(opts.Comparer.Compare, opts.Comparer.FormatKey); err != nil {
			return nil, err
		}

		return &fileMetadataAndLevel{fileMetadata: meta, targetLevel: targetLevel}, nil
	}

	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}

	cacheOpts := private.SSTableCacheOpts(cacheID, fileNum).(sstable.ReaderOption)
	// Create meta earlier and attach it to reader (does not affect non-shared sst)
	meta := &fileMetadata{}
	r, err := sstable.NewReader(f, opts.MakeReaderOptions(), cacheOpts, &sstable.FileMetadataOpt{Meta: meta, DBUniqueID: opts.UniqueID})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// Avoid ingesting tables with format versions this DB doesn't support.
	tf, err := r.TableFormat()
	if err != nil {
		return nil, err
	}
	if tf < fmv.MinTableFormat() || tf > fmv.MaxTableFormat() {
		return nil, errors.Newf(
			"pebble: table format %s is not within range supported at DB format major version %d, (%s,%s)",
			tf, fmv, fmv.MinTableFormat(), fmv.MaxTableFormat(),
		)
	}

	meta.FileNum = fileNum
	meta.Size = uint64(stat.Size())
	meta.CreationTime = time.Now().Unix()
	var targetLevel int

	if isShared {
		meta.IsShared = true
		meta.HasPointKeys = true
		meta.CreatorUniqueID = smeta.CreatorUniqueID
		meta.PhysicalFileNum = base.FileNum(smeta.PhysicalFileNum)
		targetLevel = 5
		seqNum := sstable.SeqNumL5PointKey
		endSeqNum := sstable.SeqNumL5RangeDel
		if smeta.SourceLevel == 6 {
			targetLevel = 6
			seqNum = sstable.SeqNumL6All
			endSeqNum = sstable.SeqNumL6All
		}
		meta.Smallest.UserKey = append([]byte(nil), smeta.Smallest...)
		meta.Smallest.SetKind(InternalKeyKindSet)
		meta.Smallest.SetSeqNum(uint64(seqNum))
		meta.Largest.UserKey = append([]byte(nil), smeta.Largest...)
		meta.Largest.SetKind(InternalKeyKindSet)
		meta.Largest.SetSeqNum(uint64(endSeqNum))
		meta.SmallestPointKey = meta.Smallest
		meta.LargestPointKey = meta.Largest
		meta.FileSmallest = meta.Smallest
		meta.FileLargest = meta.Largest
	}

	// Avoid loading into the table cache for collecting stats if we
	// don't need to. If there are no range deletions, we have all the
	// information to compute the stats here.
	//
	// This is helpful in tests for avoiding awkwardness around deletion of
	// ingested files from MemFS. MemFS implements the Windows semantics of
	// disallowing removal of an open file. Under MemFS, if we don't populate
	// meta.Stats here, the file will be loaded into the table cache for
	// calculating stats before we can remove the original link.
	maybeSetStatsFromProperties(meta, &r.Properties)

	// XXX(chen): I think the following logic also applies to shared ssts but
	// we must first "mount" the meta to the reader.. (this has been done above)
	{
		iter, err := r.NewIter(nil /* lower */, nil /* upper */)
		if err != nil {
			return nil, err
		}
		if isShared {
			// This is tricky because L6 shared sst will expose SeqNum = 0
			iter.SetLevel(6)
		}
		defer iter.Close()
		var smallest InternalKey
		if key, _ := iter.First(); key != nil {
			if err := ingestValidateKey(opts, key, isShared); err != nil {
				return nil, err
			}
			smallest = (*key).Clone()
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		if key, _ := iter.Last(); key != nil {
			if err := ingestValidateKey(opts, key, isShared); err != nil {
				return nil, err
			}
			meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallest, key.Clone())
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
	}

	iter, err := r.NewRawRangeDelIter()
	if err != nil {
		return nil, err
	}
	if iter != nil {
		//if isShared {
		//	sstRangeDelIter, ok := iter.(*sstable.RangeDelIter)
		//	if !ok {
		//		panic("ingestLoad1: rangeDelIter returned is not sstable.RangeDelIter")
		//	}
		//	sstRangeDelIter.SetLevel(6)
		//}
		defer iter.Close()
		var smallest InternalKey
		if s := iter.First(); s != nil {
			key := s.SmallestKey()
			if err := ingestValidateKey(opts, &key, isShared); err != nil {
				return nil, err
			}
			smallest = key.Clone()
		}
		if err := iter.Error(); err != nil {
			return nil, err
		}
		if s := iter.Last(); s != nil {
			k := s.SmallestKey()
			if err := ingestValidateKey(opts, &k, isShared); err != nil {
				return nil, err
			}
			largest := s.LargestKey().Clone()
			meta.ExtendPointKeyBounds(opts.Comparer.Compare, smallest, largest)
		}
	}

	// Update the range-key bounds for the table.
	// XXX(chen): only for local ssts as shared sst does not support rangekeys
	if !isShared {
		iter, err := r.NewRawRangeKeyIter()
		if err != nil {
			return nil, err
		}
		if iter != nil {
			defer iter.Close()
			var smallest InternalKey
			if s := iter.First(); s != nil {
				key := s.SmallestKey()
				if err := ingestValidateKey(opts, &key, isShared); err != nil {
					return nil, err
				}
				smallest = key.Clone()
			}
			if err := iter.Error(); err != nil {
				return nil, err
			}
			if s := iter.Last(); s != nil {
				k := s.SmallestKey()
				if err := ingestValidateKey(opts, &k, isShared); err != nil {
					return nil, err
				}
				// As range keys are fragmented, the end key of the last range key in
				// the table provides the upper bound for the table.
				largest := s.LargestKey().Clone()
				meta.ExtendRangeKeyBounds(opts.Comparer.Compare, smallest, largest)
			}
			if err := iter.Error(); err != nil {
				return nil, err
			}
		}
	}

	if isShared && meta.HasRangeKeys {
		panic("ingestLoad1: shared sst should not have range keys")
	}

	if !meta.HasPointKeys && !meta.HasRangeKeys {
		return nil, nil
	}

	// Sanity check that the various bounds on the file were set consistently.
	if err := meta.Validate(opts.Comparer.Compare, opts.Comparer.FormatKey); err != nil {
		return nil, err
	}

	return &fileMetadataAndLevel{fileMetadata: meta, targetLevel: targetLevel}, nil
}

func ingestLoad(
	opts *Options,
	fmv FormatMajorVersion,
	paths []string,
	smeta []SharedSSTMeta,
	cacheID uint64,
	pending []FileNum,
) ([]*fileMetadata, []string, []int, error) {
	nTables := len(paths)
	if opts.SharedFS != nil && smeta != nil {
		nTables += len(smeta)
	}
	meta := make([]*fileMetadata, 0, nTables)
	newPaths := make([]string, 0, nTables)
	shared := make([]int, 0, nTables)
	for i := range paths {
		m, err := ingestLoad1(opts, fmv, paths[i], SharedSSTMeta{}, false, cacheID, pending[i])
		if err != nil {
			return nil, nil, nil, err
		}
		if m != nil {
			meta = append(meta, m.fileMetadata)
			newPaths = append(newPaths, paths[i])
			shared = append(shared, 0)
		}
	}
	// Handle shared sstable for the len(paths)+1-th to the end of the slice
	if opts.SharedFS != nil && smeta != nil {
		for i := range smeta {
			j := i + len(paths)
			spath := base.MakeSharedSSTPath(opts.SharedFS, opts.SharedDir, smeta[i].CreatorUniqueID, base.FileNum(smeta[i].PhysicalFileNum))
			m, err := ingestLoad1(opts, fmv, spath, smeta[i], true, cacheID, pending[j])
			if err != nil {
				return nil, nil, nil, err
			}
			if m != nil {
				meta = append(meta, m.fileMetadata)
				newPaths = append(newPaths, spath)
				shared = append(shared, m.targetLevel)
			}
		}
	}
	return meta, newPaths, shared, nil
}

// Struct for sorting metadatas by smallest user keys, while ensuring the
// matching path also gets swapped to the same index. For use in
// ingestSortAndVerify.
type metaAndPaths struct {
	meta   []*fileMetadata
	paths  []string
	shared []int
	cmp    Compare
}

func (m metaAndPaths) Len() int {
	return len(m.meta)
}

func (m metaAndPaths) Less(i, j int) bool {
	return m.cmp(m.meta[i].Smallest.UserKey, m.meta[j].Smallest.UserKey) < 0
}

func (m metaAndPaths) Swap(i, j int) {
	m.meta[i], m.meta[j] = m.meta[j], m.meta[i]
	m.paths[i], m.paths[j] = m.paths[j], m.paths[i]
	m.shared[i], m.shared[j] = m.shared[j], m.shared[i]
}

func ingestSortAndVerify(cmp Compare, meta []*fileMetadata, paths []string, shared []int) error {
	if len(meta) <= 1 {
		return nil
	}

	sort.Sort(&metaAndPaths{
		meta:   meta,
		paths:  paths,
		shared: shared,
		cmp:    cmp,
	})

	for i := 1; i < len(meta); i++ {
		if shared[i] != 0 || shared[i-1] != 0 {
			continue
		}
		if sstableKeyCompare(cmp, meta[i-1].Largest, meta[i].Smallest) >= 0 {
			return errors.New("pebble: external sstables have overlapping ranges")
		}
	}
	return nil
}

func ingestCleanup(fs vfs.FS, dirname string, meta []*fileMetadata) error {
	var firstErr error
	for i := range meta {
		target := base.MakeFilepath(fs, dirname, fileTypeTable, meta[i].FileNum)
		if err := fs.Remove(target); err != nil {
			firstErr = firstError(firstErr, err)
		}
	}
	return firstErr
}

func ingestLink(
	jobID int, opts *Options, dirname string, paths []string, meta []*fileMetadata, shared []int,
) error {
	// Wrap the normal filesystem with one which wraps newly created files with
	// vfs.NewSyncingFile.
	fs := syncingFS{
		FS: opts.FS,
		syncOpts: vfs.SyncingFileOptions{
			NoSyncOnClose: opts.NoSyncOnClose,
			BytesPerSync:  opts.BytesPerSync,
		},
	}

	for i := range paths {
		// Nothing to do for shared ssts
		if shared[i] != 0 {
			continue
		}
		target := base.MakeFilepath(fs, dirname, fileTypeTable, meta[i].FileNum)
		var err error
		if _, ok := opts.FS.(*vfs.MemFS); ok && opts.DebugCheck != nil {
			// The combination of MemFS+Ingest+DebugCheck produces awkwardness around
			// the subsequent deletion of files. The problem is that MemFS implements
			// the Windows semantics of disallowing removal of an open file. This is
			// desirable because it helps catch bugs where we violate the
			// requirements of the Windows semantics. The normal practice for Ingest
			// is for the caller to remove the source files after the ingest
			// completes successfully. Unfortunately, Options.DebugCheck causes
			// ingest to run DB.CheckLevels() before the ingest finishes, and
			// DB.CheckLevels() populates the table cache with the newly ingested
			// files.
			//
			// The combination of MemFS+Ingest+DebugCheck is primarily used in
			// tests. As a workaround, disable hard linking this combination
			// occurs. See https://github.com/cockroachdb/pebble/issues/495.
			err = vfs.Copy(fs, paths[i], target)
		} else {
			err = vfs.LinkOrCopy(fs, paths[i], target)
		}
		if err == nil && opts.SharedFS != nil {
			sharedTarget := base.MakeSharedSSTPath(opts.SharedFS, opts.SharedDir, meta[i].CreatorUniqueID, meta[i].PhysicalFileNum)
			opts.SharedFS.MkdirAll(opts.SharedFS.PathDir(sharedTarget), 0755)
			err = vfs.CopyAcrossFS(fs, paths[i], opts.SharedFS, sharedTarget)
		}
		if err != nil {
			if err2 := ingestCleanup(fs, dirname, meta[:i]); err2 != nil {
				opts.Logger.Infof("ingest cleanup failed: %v", err2)
			}
			return err
		}
		if opts.EventListener.TableCreated != nil {
			opts.EventListener.TableCreated(TableCreateInfo{
				JobID:   jobID,
				Reason:  "ingesting",
				Path:    target,
				FileNum: meta[i].FileNum,
			})
		}
	}

	return nil
}

func ingestMemtableOverlaps(cmp Compare, mem flushable, meta []*fileMetadata, exciseSpan keyspan.Span) bool {
	iter := mem.newIter(nil)
	rangeDelIter := mem.newRangeDelIter(nil)
	rkeyIter := mem.newRangeKeyIter(nil)

	for _, m := range meta {
		if overlapWithIterator(iter, &rangeDelIter, rkeyIter, m, cmp) {
			return true
		}
	}
	if exciseSpan.Valid() {
		if overlapWithIterator(iter, &rangeDelIter, rkeyIter, &fileMetadata{Smallest: InternalKey{UserKey: exciseSpan.Start}, Largest: InternalKey{UserKey: exciseSpan.End}}, cmp) {
			return true
		}
	}

	err := iter.Close()
	if rangeDelIter != nil {
		err = firstError(err, rangeDelIter.Close())
	}
	if rkeyIter != nil {
		err = firstError(err, rkeyIter.Close())
	}
	if err != nil {
		// Assume overlap if any iterator errored out.
		return true
	}

	return false
}

func ingestUpdateSeqNum(
	cmp Compare, format base.FormatKey, seqNum uint64, meta []*fileMetadata, shared []int,
) error {
	setSeqFn := func(k base.InternalKey) base.InternalKey {
		return base.MakeInternalKey(k.UserKey, seqNum, k.Kind())
	}
	for idx, m := range meta {
		if shared[idx] != 0 {
			// Shared files have sequence numbers set in ingestLoad1.
			continue
		}
		// NB: we set the fields directly here, rather than via their Extend*
		// methods, as we are updating sequence numbers.
		if m.HasPointKeys {
			m.SmallestPointKey = setSeqFn(m.SmallestPointKey)
		}
		if m.HasRangeKeys {
			m.SmallestRangeKey = setSeqFn(m.SmallestRangeKey)
		}
		m.Smallest = setSeqFn(m.Smallest)
		// Only update the seqnum for the largest key if that key is not an
		// "exclusive sentinel" (i.e. a range deletion sentinel or a range key
		// boundary), as doing so effectively drops the exclusive sentinel (by
		// lowering the seqnum from the max value), and extends the bounds of the
		// table.
		// NB: as the largest range key is always an exclusive sentinel, it is never
		// updated.
		if m.HasPointKeys && !m.LargestPointKey.IsExclusiveSentinel() {
			m.LargestPointKey = setSeqFn(m.LargestPointKey)
		}
		if !m.Largest.IsExclusiveSentinel() {
			m.Largest = setSeqFn(m.Largest)
		}
		// Setting smallestSeqNum == largestSeqNum triggers the setting of
		// Properties.GlobalSeqNum when an sstable is loaded.
		m.SmallestSeqNum = seqNum
		m.LargestSeqNum = seqNum
		// Ensure the new bounds are consistent.
		if err := m.Validate(cmp, format); err != nil {
			return err
		}
		seqNum++
	}
	return nil
}

func overlapWithIterator(
	iter internalIterator,
	rangeDelIter *keyspan.FragmentIterator,
	rkeyIter keyspan.FragmentIterator,
	meta *fileMetadata,
	cmp Compare,
) bool {
	// Check overlap with point operations.
	//
	// When using levelIter, it seeks to the SST whose boundaries
	// contain meta.Smallest.UserKey(S).
	// It then tries to find a point in that SST that is >= S.
	// If there's no such point it means the SST ends in a tombstone in which case
	// levelIter.SeekGE generates a boundary range del sentinel.
	// The comparison of this boundary with meta.Largest(L) below
	// is subtle but maintains correctness.
	// 1) boundary < L,
	//    since boundary is also > S (initial seek),
	//    whatever the boundary's start key may be, we're always overlapping.
	// 2) boundary > L,
	//    overlap with boundary cannot be determined since we don't know boundary's start key.
	//    We require checking for overlap with rangeDelIter.
	// 3) boundary == L and L is not sentinel,
	//    means boundary < L and hence is similar to 1).
	// 4) boundary == L and L is sentinel,
	//    we'll always overlap since for any values of i,j ranges [i, k) and [j, k) always overlap.
	key, _ := iter.SeekGE(meta.Smallest.UserKey, base.SeekGEFlagsNone)
	if key != nil {
		c := sstableKeyCompare(cmp, *key, meta.Largest)
		if c <= 0 {
			return true
		}
	}

	computeOverlapWithSpans := func(rIter keyspan.FragmentIterator) bool {
		// NB: The spans surfaced by the fragment iterator are non-overlapping.
		span := rIter.SeekLT(meta.Smallest.UserKey)
		if span == nil {
			span = rIter.Next()
		}
		for ; span != nil; span = rIter.Next() {
			if span.Empty() {
				continue
			}
			key := span.SmallestKey()
			c := sstableKeyCompare(cmp, key, meta.Largest)
			if c > 0 {
				// The start of the span is after the largest key in the
				// ingested table.
				return false
			}
			if cmp(span.End, meta.Smallest.UserKey) > 0 {
				// The end of the span is greater than the smallest in the
				// table. Note that the span end key is exclusive, thus ">0"
				// instead of ">=0".
				return true
			}
		}
		return false
	}

	// rkeyIter is either a range key level iter, or a range key iterator
	// over a single file.
	if rkeyIter != nil {
		if computeOverlapWithSpans(rkeyIter) {
			return true
		}
	}

	// Check overlap with range deletions.
	if rangeDelIter == nil || *rangeDelIter == nil {
		return false
	}
	return computeOverlapWithSpans(*rangeDelIter)
}

func ingestTargetLevel(
	d *DB,
	newIters tableNewIters,
	newRangeKeyIter keyspan.TableNewSpanIter,
	iterOps IterOptions,
	cmp Compare,
	v *version,
	baseLevel int,
	compactions map[*compaction]struct{},
	meta *fileMetadata,
) (int, error) {
	// Find the lowest level which does not have any files which overlap meta. We
	// search from L0 to L6 looking for whether there are any files in the level
	// which overlap meta. We want the "lowest" level (where lower means
	// increasing level number) in order to reduce write amplification.
	//
	// There are 2 kinds of overlap we need to check for: file boundary overlap
	// and data overlap. Data overlap implies file boundary overlap. Note that it
	// is always possible to ingest into L0.
	//
	// To place meta at level i where i > 0:
	// - there must not be any data overlap with levels <= i, since that will
	//   violate the sequence number invariant.
	// - no file boundary overlap with level i, since that will violate the
	//   invariant that files do not overlap in levels i > 0.
	//
	// The file boundary overlap check is simpler to conceptualize. Consider the
	// following example, in which the ingested file lies completely before or
	// after the file being considered.
	//
	//   |--|           |--|  ingested file: [a,b] or [f,g]
	//         |-----|        existing file: [c,e]
	//  _____________________
	//   a  b  c  d  e  f  g
	//
	// In both cases the ingested file can move to considering the next level.
	//
	// File boundary overlap does not necessarily imply data overlap. The check
	// for data overlap is a little more nuanced. Consider the following examples:
	//
	//  1. No data overlap:
	//
	//          |-|   |--|    ingested file: [cc-d] or [ee-ff]
	//  |*--*--*----*------*| existing file: [a-g], points: [a, b, c, dd, g]
	//  _____________________
	//   a  b  c  d  e  f  g
	//
	// In this case the ingested files can "fall through" this level. The checks
	// continue at the next level.
	//
	//  2. Data overlap:
	//
	//            |--|        ingested file: [d-e]
	//  |*--*--*----*------*| existing file: [a-g], points: [a, b, c, dd, g]
	//  _____________________
	//   a  b  c  d  e  f  g
	//
	// In this case the file cannot be ingested into this level as the point 'dd'
	// is in the way.
	//
	// It is worth noting that the check for data overlap is only approximate. In
	// the previous example, the ingested table [d-e] could contain only the
	// points 'd' and 'e', in which case the table would be eligible for
	// considering lower levels. However, such a fine-grained check would need to
	// be exhaustive (comparing points and ranges in both the ingested existing
	// tables) and such a check is prohibitively expensive. Thus Pebble treats any
	// existing point that falls within the ingested table bounds as being "data
	// overlap".

	targetLevel := 0

	// Do we overlap with keys in L0?
	// TODO(bananabrick): Use sublevels to compute overlap.
	iter := v.Levels[0].Iter()
	if !meta.IsShared {
		for meta0 := iter.First(); meta0 != nil; meta0 = iter.Next() {
			c1 := sstableKeyCompare(cmp, meta.Smallest, meta0.Largest)
			c2 := sstableKeyCompare(cmp, meta.Largest, meta0.Smallest)
			if c1 > 0 || c2 < 0 {
				continue
			}

			iter, rangeDelIter, err := newIters(meta0, nil, internalIterOpts{})
			if err != nil {
				return 0, err
			}
			rkeyIter, err := newRangeKeyIter(meta0, nil)
			if err != nil {
				return 0, err
			}
			overlap := overlapWithIterator(iter, &rangeDelIter, rkeyIter, meta, cmp)
			err = firstError(err, iter.Close())
			if rangeDelIter != nil {
				err = firstError(err, rangeDelIter.Close())
			}
			if rkeyIter != nil {
				err = firstError(err, rkeyIter.Close())
			}
			if err != nil {
				return 0, err
			}
			if overlap {
				return targetLevel, nil
			}
		}
	}

	level := baseLevel
	lowest := numLevels

	if d.opts.SharedFS != nil {
		// Shared SST only ingest into L5/L6
		if meta.IsShared {
			level = 5
		} else {
			lowest = sharedLevel
		}
	}

	for ; level < lowest; level++ {
		levelIter := newLevelIter(iterOps, cmp, nil /* split */, newIters,
			v.Levels[level].Iter(), manifest.Level(level), nil)
		var rangeDelIter keyspan.FragmentIterator
		// Pass in a non-nil pointer to rangeDelIter so that levelIter.findFileGE
		// sets it up for the target file.
		levelIter.initRangeDel(&rangeDelIter)

		rkeyLevelIter := &keyspan.LevelIter{}
		rkeyLevelIter.Init(
			keyspan.SpanIterOptions{}, cmp, newRangeKeyIter,
			v.Levels[level].Iter(), manifest.Level(level), manifest.KeyTypeRange,
		)

		overlap := overlapWithIterator(levelIter, &rangeDelIter, rkeyLevelIter, meta, cmp)
		err := levelIter.Close() // Closes range del iter as well.
		err = firstError(err, rkeyLevelIter.Close())
		if err != nil {
			return 0, err
		}
		if overlap {
			return targetLevel, nil
		}

		// Check boundary overlap.
		boundaryOverlaps := v.Overlaps(level, cmp, meta.Smallest.UserKey,
			meta.Largest.UserKey, meta.Largest.IsExclusiveSentinel())
		if !boundaryOverlaps.Empty() {
			continue
		}

		// Check boundary overlap with any ongoing compactions.
		//
		// We cannot check for data overlap with the new SSTs compaction will
		// produce since compaction hasn't been done yet. However, there's no need
		// to check since all keys in them will either be from c.startLevel or
		// c.outputLevel, both levels having their data overlap already tested
		// negative (else we'd have returned earlier).
		overlaps := false
		for c := range compactions {
			if c.outputLevel == nil || level != c.outputLevel.level {
				continue
			}
			if cmp(meta.Smallest.UserKey, c.largest.UserKey) <= 0 &&
				cmp(meta.Largest.UserKey, c.smallest.UserKey) >= 0 {
				overlaps = true
				break
			}
		}
		if !overlaps {
			targetLevel = level
		}
	}
	return targetLevel, nil
}

// SharedSSTMeta records the necessary information when ingesting a shared sstable
type SharedSSTMeta struct {
	CreatorUniqueID           uint32
	PhysicalFileNum           uint64
	SourceLevel               uint8
	Smallest, Largest         []byte
	FileSmallest, FileLargest []byte
}

// Ingest ingests a set of sstables into the DB. Ingestion of the files is
// atomic and semantically equivalent to creating a single batch containing all
// of the mutations in the sstables. Ingestion may require the memtable to be
// flushed. The ingested sstable files are moved into the DB and must reside on
// the same filesystem as the DB. Sstables can be created for ingestion using
// sstable.Writer. On success, Ingest removes the input paths.
//
// All sstables *must* be Sync()'d by the caller after all bytes are written
// and before its file handle is closed; failure to do so could violate
// durability or lead to corrupted on-disk state. This method cannot, in a
// platform-and-FS-agnostic way, ensure that all sstables in the input are
// properly synced to disk. Opening new file handles and Sync()-ing them
// does not always guarantee durability; see the discussion here on that:
// https://github.com/cockroachdb/pebble/pull/835#issuecomment-663075379
//
// Ingestion loads each sstable into the lowest level of the LSM which it
// doesn't overlap (see ingestTargetLevel). If an sstable overlaps a memtable,
// ingestion forces the memtable to flush, and then waits for the flush to
// occur.
//
// The steps for ingestion are:
//
//  1. Allocate file numbers for every sstable being ingested.
//  2. Load the metadata for all sstables being ingest.
//  3. Sort the sstables by smallest key, verifying non overlap.
//  4. Hard link (or copy) the sstables into the DB directory.
//  5. Allocate a sequence number to use for all of the entries in the
//     sstables. This is the step where overlap with memtables is
//     determined. If there is overlap, we remember the most recent memtable
//     that overlaps.
//  6. Update the sequence number in the ingested sstables.
//  7. Wait for the most recent memtable that overlaps to flush (if any).
//  8. Add the ingested sstables to the version (DB.ingestApply).
//  9. Publish the ingestion sequence number.
//
// Note that if the mutable memtable overlaps with ingestion, a flush of the
// memtable is forced equivalent to DB.Flush. Additionally, subsequent
// mutations that get sequence numbers larger than the ingestion sequence
// number get queued up behind the ingestion waiting for it to complete. This
// can produce a noticeable hiccup in performance. See
// https://github.com/cockroachdb/pebble/issues/25 for an idea for how to fix
// this hiccup.
func (d *DB) Ingest(paths []string, smeta []SharedSSTMeta) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	_, err := d.ingest(paths, smeta, ingestTargetLevel, keyspan.Span{})
	return err
}

// IngestAndExcise TODO.
func (d *DB) IngestAndExcise(paths []string, smeta []SharedSSTMeta, exciseSpan rangekey.Span) error {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	_, err := d.ingest(paths, smeta, ingestTargetLevel, exciseSpan)
	return err
}

// IngestOperationStats provides some information about where in the LSM the
// bytes were ingested.
type IngestOperationStats struct {
	// Bytes is the total bytes in the ingested sstables.
	Bytes uint64
	// ApproxIngestedIntoL0Bytes is the approximate number of bytes ingested
	// into L0.
	// Currently, this value is completely accurate, but we are allowing this to
	// be approximate once https://github.com/cockroachdb/pebble/issues/25 is
	// implemented.
	ApproxIngestedIntoL0Bytes uint64
}

// IngestWithStats does the same as Ingest, and additionally returns
// IngestOperationStats.
func (d *DB) IngestWithStats(paths []string, smeta []SharedSSTMeta) (IngestOperationStats, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return IngestOperationStats{}, ErrReadOnly
	}
	return d.ingest(paths, smeta, ingestTargetLevel, keyspan.Span{})
}

func sortAndMergeSpans(cmp Compare, spans []keyspan.Span) []keyspan.Span {
	if len(spans) == 0 {
		return spans
	}
	sort.Slice(spans, func(i, j int) bool {
		cmpVal := cmp(spans[i].Start, spans[j].Start)
		if cmpVal == 0 {
			return cmp(spans[i].End, spans[j].End) < 0
		}
		return cmpVal < 0
	})
	j := 0
	for i := 1; i < len(spans); i++ {
		if cmp(spans[j].End, spans[i].Start) >= 0 {
			// Overlap.
			if cmp(spans[j].End, spans[i].End) < 0 {
				spans[j].End = spans[i].End
			}
		} else {
			j++
			spans[j] = spans[i]
		}
	}
	return spans[:j+1]
}

func (d *DB) ingest(
	paths []string, smeta []SharedSSTMeta, targetLevelFunc ingestTargetLevelFunc, exciseSpan keyspan.Span,
) (IngestOperationStats, error) {
	// Allocate file numbers for all of the files being ingested and mark them as
	// pending in order to prevent them from being deleted. Note that this causes
	// the file number ordering to be out of alignment with sequence number
	// ordering. The sorting of L0 tables by sequence number avoids relying on
	// that (busted) invariant.
	d.mu.Lock()
	// Reserve slots for both local and shared sstables
	nTables := len(paths)
	if d.opts.SharedFS != nil && smeta != nil {
		nTables += len(smeta)
	}
	pendingOutputs := make([]FileNum, nTables)
	// pending[0:len(paths)] are for local sstables and the remaining are for shared tables
	for i := 0; i < nTables; i++ {
		pendingOutputs[i] = d.mu.versions.getNextFileNum()
	}
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.mu.Unlock()

	// Load the metadata for all of the files being ingested. This step detects
	// and elides empty sstables.
	meta, paths, shared, err := ingestLoad(d.opts, d.FormatMajorVersion(), paths, smeta, d.cacheID, pendingOutputs)
	if err != nil {
		return IngestOperationStats{}, err
	}
	if len(meta) == 0 {
		// All of the sstables to be ingested were empty. Nothing to do.
		return IngestOperationStats{}, nil
	}

	// Just to make sure
	if len(meta) != len(paths) || len(meta) != len(shared) {
		panic("ingest: meta, paths and shared have different length")
	}

	// Verify the sstables do not overlap.
	if err := ingestSortAndVerify(d.cmp, meta, paths, shared); err != nil {
		return IngestOperationStats{}, err
	}
	sharedSpans := []keyspan.Span{}
	for i, m := range meta {
		if shared[i] == 0 {
			continue
		}
		sharedSpans = append(sharedSpans, keyspan.Span{Start: m.Smallest.UserKey, End: m.Largest.UserKey})
	}
	//sharedSpans = sortAndMergeSpans(d.cmp, sharedSpans)
	for _, span := range sharedSpans {
		if !exciseSpan.Valid() || d.cmp(span.Start, exciseSpan.Start) < 0 || d.cmp(span.End, exciseSpan.End) > 0 {
			return IngestOperationStats{}, errors.Newf("ingest: shared span outside of excise span: %s - %s vs %s - %s", d.opts.Comparer.FormatKey(span.Start), d.opts.Comparer.FormatKey(span.End),
				d.opts.Comparer.FormatKey(exciseSpan.Start), d.opts.Comparer.FormatKey(exciseSpan.End))
		}
	}

	// Hard link the sstables into the DB directory. Since the sstables aren't
	// referenced by a version, they won't be used. If the hard linking fails
	// (e.g. because the files reside on a different filesystem), ingestLink will
	// fall back to copying, and if that fails we undo our work and return an
	// error.
	if err := ingestLink(jobID, d.opts, d.dirname, paths, meta, shared); err != nil {
		return IngestOperationStats{}, err
	}
	// Fsync the directory we added the tables to. We need to do this at some
	// point before we update the MANIFEST (via logAndApply), otherwise a crash
	// can have the tables referenced in the MANIFEST, but not present in the
	// directory.
	if err := d.dataDir.Sync(); err != nil {
		return IngestOperationStats{}, err
	}

	var mem *flushableEntry
	prepare := func() {
		// Note that d.commit.mu is held by commitPipeline when calling prepare.

		d.mu.Lock()
		defer d.mu.Unlock()

		// Check to see if any files overlap with any of the memtables. The queue
		// is ordered from oldest to newest with the mutable memtable being the
		// last element in the slice. We want to wait for the newest table that
		// overlaps.
		for i := len(d.mu.mem.queue) - 1; i >= 0; i-- {
			m := d.mu.mem.queue[i]
			if ingestMemtableOverlaps(d.cmp, m, meta, exciseSpan) {
				mem = m
				if mem.flushable == d.mu.mem.mutable {
					err = d.makeRoomForWrite(nil)
				}
				mem.flushForced = true
				d.maybeScheduleFlush()
				return
			}
		}
	}

	var ve *versionEdit
	apply := func(seqNum uint64) {
		if err != nil {
			// An error occurred during prepare.
			return
		}

		// Update the sequence number for all of the sstables in the
		// metadata. Writing the metadata to the manifest when the
		// version edit is applied is the mechanism that persists the
		// sequence number. The sstables themselves are left unmodified.
		if err = ingestUpdateSeqNum(
			d.cmp, d.opts.Comparer.FormatKey, seqNum, meta, shared,
		); err != nil {
			return
		}

		// If we overlapped with a memtable in prepare wait for the flush to
		// finish.
		if mem != nil {
			<-mem.flushed
		}

		// Assign the sstables to the correct level in the LSM and apply the
		// version edit.
		ve, err = d.ingestApply(jobID, meta, targetLevelFunc, exciseSpan, shared, seqNum)
	}

	d.commit.AllocateSeqNum(len(meta), prepare, apply)

	if err != nil {
		if err2 := ingestCleanup(d.opts.FS, d.dirname, meta); err2 != nil {
			d.opts.Logger.Infof("ingest cleanup failed: %v", err2)
		}
	} else {
		for i, path := range paths {
			// No removal for shared ssts
			// Here the items in paths and meta are matched
			if shared[i] != 0 {
				continue
			}
			if err2 := d.opts.FS.Remove(path); err2 != nil {
				d.opts.Logger.Infof("ingest failed to remove original file: %s", err2)
			}
		}
	}
	info := TableIngestInfo{
		JobID:        jobID,
		GlobalSeqNum: meta[0].SmallestSeqNum,
		Err:          err,
	}
	var stats IngestOperationStats
	if ve != nil {
		info.Tables = make([]struct {
			TableInfo
			Level int
		}, len(ve.NewFiles))
		for i := range ve.NewFiles {
			e := &ve.NewFiles[i]
			info.Tables[i].Level = e.Level
			info.Tables[i].TableInfo = e.Meta.TableInfo()
			stats.Bytes += e.Meta.Size
			if e.Level == 0 {
				stats.ApproxIngestedIntoL0Bytes += e.Meta.Size
			}
		}
	}
	d.opts.EventListener.TableIngested(info)

	return stats, err
}

type ingestTargetLevelFunc func(
	d *DB,
	newIters tableNewIters,
	newRangeKeyIter keyspan.TableNewSpanIter,
	iterOps IterOptions,
	cmp Compare,
	v *version,
	baseLevel int,
	compactions map[*compaction]struct{},
	meta *fileMetadata,
) (int, error)

func (d *DB) ingestApply(
	jobID int, meta []*fileMetadata, findTargetLevel ingestTargetLevelFunc, exciseSpan keyspan.Span, targetLevels []int, seqNum uint64,
) (*versionEdit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if exciseSpan.Valid() && len(meta) > 0 {
		minFileNum := uint64(math.MaxUint64)
		for _, m := range meta {
			if uint64(m.FileNum) < minFileNum {
				minFileNum = uint64(m.FileNum)
			}
		}
		d.mu.compact.excisedSpan = append(d.mu.compact.excisedSpan, excisedSpan{span: exciseSpan, curFileNum: base.FileNum(minFileNum)})
	}

	ve := &versionEdit{
		NewFiles: make([]newFileEntry, len(meta)),
	}
	metrics := make(map[int]*LevelMetrics)

	// Lock the manifest for writing before we use the current version to
	// determine the target level. This prevents two concurrent ingestion jobs
	// from using the same version to determine the target level, and also
	// provides serialization with concurrent compaction and flush jobs.
	// logAndApply unconditionally releases the manifest lock, but any earlier
	// returns must unlock the manifest.
	d.mu.versions.logLock()

	current := d.mu.versions.currentVersion()
	baseLevel := d.mu.versions.picker.getBaseLevel()
	iterOps := IterOptions{logger: d.opts.Logger}
	obsoleteFiles := make([]obsoleteFile, 0)
	for i := range meta {
		// Determine the lowest level in the LSM for which the sstable doesn't
		// overlap any existing files in the level.
		m := meta[i]
		f := &ve.NewFiles[i]
		var err error
		if targetLevels[i] != 0 {
			// Shared file owned by a different node.
			f.Level = targetLevels[i]
		} else {
			f.Level, err = findTargetLevel(d, d.newIters, d.tableNewRangeKeyIter, iterOps, d.cmp, current, baseLevel, d.mu.compact.inProgress, m)
			if err != nil {
				d.mu.versions.logUnlock()
				return nil, err
			}
		}
		if f.Level >= sharedLevel && d.opts.SharedFS != nil && targetLevels[i] == 0 {
			// Shared file owned by us.
			m.IsShared = true
			m.CreatorUniqueID = d.opts.UniqueID
			m.FileSmallest = m.Smallest
			m.FileLargest = m.Largest
			obsoleteFiles = append(obsoleteFiles, obsoleteFile{
				dir:         d.dirname,
				fileNum:     m.FileNum,
				fileType:    fileTypeTable,
				fileSize:    m.Size,
				skipMetrics: true,
			})
		}
		f.Meta = m
		levelMetrics := metrics[f.Level]
		if levelMetrics == nil {
			levelMetrics = &LevelMetrics{}
			metrics[f.Level] = levelMetrics
		}
		levelMetrics.NumFiles++
		levelMetrics.Size += int64(m.Size)
		levelMetrics.BytesIngested += m.Size
		levelMetrics.TablesIngested++
	}
	if exciseSpan.Valid() {
		for level := 0; level < numLevels; level++ {
			if d.mu.versions.currentVersion().Levels[level].Empty() {
				continue
			}

			var filesToExcise []*fileMetadata
			slice := d.mu.versions.currentVersion().Overlaps(level, d.cmp, exciseSpan.Start, exciseSpan.End, true)
			slice.Each(func(meta *manifest.FileMetadata) {
				filesToExcise = append(filesToExcise, meta)
			})
			sort.Slice(filesToExcise, func(i, j int) bool {
				return d.cmp(filesToExcise[i].Smallest.UserKey, filesToExcise[j].Smallest.UserKey) < 0
			})
			levelMetrics := metrics[level]
			if levelMetrics == nil {
				levelMetrics = &LevelMetrics{}
				metrics[level] = levelMetrics
			}
			for _, meta := range filesToExcise {
				if d.cmp(meta.Largest.UserKey, exciseSpan.Start) < 0 ||
					(d.cmp(meta.Largest.UserKey, exciseSpan.Start) == 0 && meta.Largest.IsExclusiveSentinel()) ||
					d.cmp(meta.Smallest.UserKey, exciseSpan.End) > 0 {
					continue
				}
				if ve.DeletedFiles == nil {
					ve.DeletedFiles = map[manifest.DeletedFileEntry]*manifest.FileMetadata{}
				}
				ve.DeletedFiles[manifest.DeletedFileEntry{
					Level:   level,
					FileNum: meta.FileNum,
				}] = meta
				levelMetrics.NumFiles--
				levelMetrics.Size -= int64(meta.Size)
				newMeta := &fileMetadata{}
				*newMeta = *meta
				meta.IsExcised = true
				newMeta.CompactionState = manifest.CompactionStateNotCompacting
				newMeta.IsIntraL0Compacting = false
				newMeta.FileNum = d.mu.versions.getNextFileNum()
				newMeta.CreatorUniqueID = d.opts.UniqueID
				if meta.PhysicalFileNum != 0 {
					newMeta.PhysicalFileNum = meta.PhysicalFileNum
				} else {
					newMeta.PhysicalFileNum = meta.FileNum
				}

				// exciseSpan overlaps with the current file. We need to splice
				// away [exciseSpan.Start, exciseSpan.End]. Produce up to
				// two virtual sstables, one with bounds:
				// [oldMeta.Smallest, exciseSpan.Start.PrevKeyInSST()]
				// And another with bounds:
				// [exciseSpan.End.NextKeyInSST(), oldMeta.Largest]
				// If any of the bounds above have no keys in them, or have smallest > largest,
				// then that sstable does not need to be created. It's also possible
				// for there to be no virtual sstable created, if the shared sstable span is very wide.
				newMeta2 := &fileMetadata{}
				*newMeta2 = *newMeta
				newMeta2.FileNum = d.mu.versions.getNextFileNum()
				if d.cmp(exciseSpan.Start, newMeta.Smallest.UserKey) > 0 {
					iter, rangeDelIter, err := d.newIters(meta, &IterOptions{LowerBound: meta.Smallest.UserKey, UpperBound: meta.Largest.UserKey, UpperBoundIsInclusive: true, level: manifest.Level(level)}, internalIterOpts{})
					if err != nil {
						return nil, err
					}
					ikey, _ := iter.SeekLT(exciseSpan.Start, base.SeekLTFlagsNone)
					found := false
					if ikey != nil {
						newMeta.Largest = ikey.Clone()
						newMeta.LargestPointKey = newMeta.Largest
						found = true
					}
					if rangeDelIter != nil {
						rangeDelIter = keyspan.Truncate(d.cmp, rangeDelIter, meta.Smallest.UserKey, meta.Largest.UserKey, &meta.Smallest, &meta.Largest)
						rdel := rangeDelIter.SeekLT(exciseSpan.Start)
						if rdel != nil && (!found || d.cmp(rdel.End, newMeta.Largest.UserKey) > 0) {
							if d.cmp(rdel.End, exciseSpan.Start) > 0 {
								newMeta.Largest.UserKey = append([]byte(nil), exciseSpan.Start...)
							} else {
								newMeta.Largest.UserKey = append([]byte(nil), rdel.End...)
							}
							newMeta.Largest.Trailer = InternalKeyRangeDeleteSentinel
							newMeta.LargestPointKey = newMeta.Largest
							found = true
						}
					}
					if found {
						if d.cmp(newMeta.Smallest.UserKey, newMeta.Largest.UserKey) == 0 && base.InternalCompare(d.cmp, newMeta.Smallest, newMeta.Largest) > 0 {
							if ikey == nil {
								// We only have a rangedel remaining that's also shrunk to being
								// nothing. Don't add this file.
								found = false
							} else {
								newMeta.Largest.Trailer = ikey.Trailer
								newMeta.LargestPointKey.Trailer = ikey.Trailer
							}
						}
					}
					if newMeta.HasRangeKeys {
						if d.cmp(newMeta.LargestRangeKey.UserKey, exciseSpan.Start) >= 0 {
							newMeta.HasRangeKeys = false
							newMeta.SmallestRangeKey = InternalKey{}
							newMeta.LargestRangeKey = InternalKey{}
						} else if base.InternalCompare(d.cmp, newMeta.Largest, newMeta.LargestRangeKey) < 0 {
							newMeta.Largest = newMeta.LargestRangeKey
							found = true
						}
					}
					if found {
						meta.IsNowVirtual = true
						ve.NewFiles = append(ve.NewFiles, manifest.NewFileEntry{
							Level: level,
							Meta:  newMeta,
						})
						levelMetrics.NumFiles++
						levelMetrics.Size += int64(newMeta.Size)
					}
					if rangeDelIter != nil {
						if err := rangeDelIter.Close(); err != nil {
							return nil, err
						}
					}
					if err := iter.Close(); err != nil {
						return nil, err
					}
				}
				if d.cmp(exciseSpan.End, newMeta2.Largest.UserKey) <= 0 {
					iter, rangeDelIter, err := d.newIters(meta, &IterOptions{LowerBound: meta.Smallest.UserKey, UpperBound: meta.Largest.UserKey, UpperBoundIsInclusive: true, level: manifest.Level(level)}, internalIterOpts{})
					if err != nil {
						return nil, err
					}
					ikey, _ := iter.SeekGE(exciseSpan.End, base.SeekGEFlagsNone)
					found := false
					if ikey != nil {
						newMeta2.Smallest = ikey.Clone()
						newMeta2.SmallestPointKey = newMeta2.Smallest
						found = true
					}
					if rangeDelIter != nil {
						rangeDelIter = keyspan.Truncate(d.cmp, rangeDelIter, meta.Smallest.UserKey, meta.Largest.UserKey, &meta.Smallest, &meta.Largest)
						rdel := rangeDelIter.SeekGE(exciseSpan.End)
						if rdel != nil && (!found || d.cmp(rdel.Start, newMeta2.Smallest.UserKey) < 0) {
							if d.cmp(rdel.Start, exciseSpan.End) < 0 {
								newMeta2.Smallest.UserKey = append([]byte(nil), exciseSpan.End...)
							} else {
								newMeta2.Smallest.UserKey = append([]byte(nil), rdel.Start...)
							}
							newMeta2.Smallest.SetKind(InternalKeyKindRangeKeyDelete)
							newMeta2.Smallest.SetSeqNum(rdel.LargestSeqNum())
							newMeta2.SmallestPointKey = newMeta2.Smallest
							found = true
						}
					}
					if found {
						if d.cmp(newMeta2.Smallest.UserKey, newMeta2.Largest.UserKey) == 0 && base.InternalCompare(d.cmp, newMeta2.Smallest, newMeta2.Largest) > 0 {
							if ikey == nil {
								// We only have a rangedel remaining that's also shrunk to being
								// nothing. Don't add this file.
								found = false
							} else {
								newMeta2.Smallest.Trailer = ikey.Trailer
								newMeta2.SmallestPointKey.Trailer = ikey.Trailer
							}
						}
					}
					if newMeta2.HasRangeKeys {
						if d.cmp(newMeta2.SmallestRangeKey.UserKey, exciseSpan.End) < 0 {
							newMeta2.HasRangeKeys = false
							newMeta2.SmallestRangeKey = InternalKey{}
							newMeta2.LargestRangeKey = InternalKey{}
						} else if base.InternalCompare(d.cmp, newMeta2.Smallest, newMeta2.SmallestRangeKey) > 0 {
							newMeta2.Smallest = newMeta2.SmallestRangeKey
							found = true
						}
					}
					if found {
						meta.IsNowVirtual = true
						ve.NewFiles = append(ve.NewFiles, manifest.NewFileEntry{
							Level: level,
							Meta:  newMeta2,
						})
						levelMetrics.NumFiles++
						levelMetrics.Size += int64(newMeta2.Size)
					}
					if err := iter.Close(); err != nil {
						return nil, err
					}
					if rangeDelIter != nil {
						if err := rangeDelIter.Close(); err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	if err := d.mu.versions.logAndApply(jobID, ve, metrics, false /* forceRotation */, func() []compactionInfo {
		return d.getInProgressCompactionInfoLocked(nil)
	}); err != nil {
		return nil, err
	}
	d.updateReadStateLocked(d.opts.DebugCheck)
	d.updateTableStatsLocked(ve.NewFiles)
	d.deleters.Add(1)
	go d.paceAndDeleteObsoleteFiles(jobID, obsoleteFiles)
	d.deleteObsoleteFiles(jobID, false /* waitForOngoing */)
	// The ingestion may have pushed a level over the threshold for compaction,
	// so check to see if one is necessary and schedule it.
	d.maybeScheduleCompaction()
	d.maybeValidateSSTablesLocked(ve.NewFiles)
	return ve, nil
}

// maybeValidateSSTablesLocked adds the slice of newFileEntrys to the pending
// queue of files to be validated, when the feature is enabled.
// DB.mu must be locked when calling.
func (d *DB) maybeValidateSSTablesLocked(newFiles []newFileEntry) {
	// Only add to the validation queue when the feature is enabled.
	if !d.opts.Experimental.ValidateOnIngest {
		return
	}

	d.mu.tableValidation.pending = append(d.mu.tableValidation.pending, newFiles...)
	if d.shouldValidateSSTablesLocked() {
		go d.validateSSTables()
	}
}

// shouldValidateSSTablesLocked returns true if SSTable validation should run.
// DB.mu must be locked when calling.
func (d *DB) shouldValidateSSTablesLocked() bool {
	return !d.mu.tableValidation.validating &&
		d.closed.Load() == nil &&
		d.opts.Experimental.ValidateOnIngest &&
		len(d.mu.tableValidation.pending) > 0
}

// validateSSTables runs a round of validation on the tables in the pending
// queue.
func (d *DB) validateSSTables() {
	d.mu.Lock()
	if !d.shouldValidateSSTablesLocked() {
		d.mu.Unlock()
		return
	}

	pending := d.mu.tableValidation.pending
	d.mu.tableValidation.pending = nil
	d.mu.tableValidation.validating = true
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	rs := d.loadReadState()

	// Drop DB.mu before performing IO.
	d.mu.Unlock()

	// Validate all tables in the pending queue. This could lead to a situation
	// where we are starving IO from other tasks due to having to page through
	// all the blocks in all the sstables in the queue.
	// TODO(travers): Add some form of pacing to avoid IO starvation.
	for _, f := range pending {
		// The file may have been moved or deleted since it was ingested, in
		// which case we skip.
		if !rs.current.Contains(f.Level, d.cmp, f.Meta) {
			// Assume the file was moved to a lower level. It is rare enough
			// that a table is moved or deleted between the time it was ingested
			// and the time the validation routine runs that the overall cost of
			// this inner loop is tolerably low, when amortized over all
			// ingested tables.
			found := false
			for i := f.Level + 1; i < numLevels; i++ {
				if rs.current.Contains(i, d.cmp, f.Meta) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		err := d.tableCache.withReader(f.Meta, func(r *sstable.Reader) error {
			return r.ValidateBlockChecksums()
		})
		if err != nil {
			// TODO(travers): Hook into the corruption reporting pipeline, once
			// available. See pebble#1192.
			d.opts.Logger.Fatalf("pebble: encountered corruption during ingestion: %s", err)
		}

		d.opts.EventListener.TableValidated(TableValidatedInfo{
			JobID: jobID,
			Meta:  f.Meta,
		})
	}
	rs.unref()

	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.tableValidation.validating = false
	d.mu.tableValidation.cond.Broadcast()
	if d.shouldValidateSSTablesLocked() {
		go d.validateSSTables()
	}
}
