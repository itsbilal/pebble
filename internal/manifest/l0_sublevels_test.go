// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/stretchr/testify/require"
)

func readManifest(filename string) (*Version, error) {
	f, err := os.Open("testdata/MANIFEST_import")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)
	var v *Version
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var ve VersionEdit
		if err = ve.Decode(r); err != nil {
			return nil, err
		}
		var bve BulkVersionEdit
		bve.Accumulate(&ve)
		if v, _, err = bve.Apply(v, base.DefaultComparer.Compare, base.DefaultFormatter, 10 << 20); err != nil {
			return nil, err
		}
	}
	fmt.Printf("L0 filecount: %d\n", len(v.Files[0]))
	return v, nil
}

func TestL0SubLevels_LargeImportL0(t *testing.T) {
	// TODO(bilal): Fix this test.
	t.Skip()
	v, err := readManifest("testdata/MANIFEST_import")
	require.NoError(t, err)

	subLevels, err := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, base.DefaultFormatter, 5<<20)
	require.NoError(t, err)
	fmt.Printf("L0SubLevels:\n%s\n\n", subLevels)

	for i := 0; ; i++ {
		c, err := subLevels.PickBaseCompaction(2, nil)
		require.NoError(t, err)
		if c == nil {
			break
		}
		fmt.Printf("%d: base compaction: filecount: %d, bytes: %d, interval: [%d, %d], seed depth: %d\n",
			i, len(c.Files), c.fileBytes, c.minIntervalIndex, c.maxIntervalIndex, c.seedIntervalStackDepthReduction)
		var files []*FileMetadata
		for i := range c.Files {
			if c.FilesIncluded[i] {
				c.Files[i].Compacting = true
				files = append(files, c.Files[i])
			}
		}
		require.NoError(t, subLevels.UpdateStateForStartedCompaction([][]*FileMetadata{files}, true))
	}

	for i := 0; ; i++ {
		c, err := subLevels.PickIntraL0Compaction(math.MaxUint64, 2)
		require.NoError(t, err)
		if c == nil {
			break
		}
		fmt.Printf("%d: intra-L0 compaction: filecount: %d, bytes: %d, interval: [%d, %d], seed depth: %d\n",
			i, len(c.Files), c.fileBytes, c.minIntervalIndex, c.maxIntervalIndex, c.seedIntervalStackDepthReduction)
		var files []*FileMetadata
		for i := range c.Files {
			if c.FilesIncluded[i] {
				c.Files[i].Compacting = true
				c.Files[i].IsIntraL0Compacting = true
				files = append(files, c.Files[i])
			}
		}
		require.NoError(t, subLevels.UpdateStateForStartedCompaction([][]*FileMetadata{files}, false))
	}
}

func visualizeSublevels(
	s *L0SubLevels, compactionFiles bitSet, otherLevels [][]*FileMetadata,
) string {
	var buf strings.Builder
	if compactionFiles == nil {
		compactionFiles = newBitSet(len(s.filesByAge))
	}
	largestChar := byte('a')
	printLevel := func(files []*FileMetadata, level string, isL0 bool) {
		lastChar := byte('a')
		fmt.Fprintf(&buf, "L%s:", level)
		for i := 0; i < 5-len(level); i++ {
			buf.WriteByte(' ')
		}
		for j, f := range files {
			for lastChar < f.Smallest.UserKey[0] {
				buf.WriteString("   ")
				lastChar++
			}
			buf.WriteByte(f.Smallest.UserKey[0])
			middleChar := byte('-')
			if isL0 {
				if compactionFiles[f.l0Index] {
					middleChar = '+'
				} else if f.Compacting {
					if f.IsIntraL0Compacting {
						middleChar = '^'
					} else {
						middleChar = 'v'
					}
				}
			} else if f.Compacting {
				middleChar = '='
			}
			if largestChar < f.Largest.UserKey[0] {
				largestChar = f.Largest.UserKey[0]
			}
			if f.Smallest.UserKey[0] == f.Largest.UserKey[0] {
				buf.WriteByte(f.Largest.UserKey[0])
				if compactionFiles[f.l0Index] {
					buf.WriteByte('+')
				} else if j < len(files)-1 {
					buf.WriteByte(' ')
				}
				lastChar++
				continue
			}
			buf.WriteByte(middleChar)
			buf.WriteByte(middleChar)
			lastChar++
			for lastChar < f.Largest.UserKey[0] {
				buf.WriteByte(middleChar)
				buf.WriteByte(middleChar)
				buf.WriteByte(middleChar)
				lastChar++
			}
			buf.WriteByte(middleChar)
			buf.WriteByte(f.Largest.UserKey[0])
			if j < len(files)-1 {
				buf.WriteByte(' ')
			}
			lastChar++
		}
		fmt.Fprintf(&buf, "\n")
	}
	for i := len(s.Files) - 1; i >= 0; i-- {
		printLevel(s.Files[i], fmt.Sprintf("0.%d", i), true)
	}
	for i := range otherLevels {
		if len(otherLevels[i]) == 0 {
			continue
		}
		printLevel(otherLevels[i], strconv.Itoa(i+1), false)
	}
	buf.WriteString("       ")
	for b := byte('a'); b <= largestChar; b++ {
		buf.WriteByte(b)
		buf.WriteByte(b)
		if b < largestChar {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte('\n')
	return buf.String()
}

func TestL0SubLevels(t *testing.T) {
	parseMeta := func(s string) (*FileMetadata, error) {
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		fileNum, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		keyRange := strings.Split(strings.TrimSpace(fields[0]), "-")
		m := FileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(keyRange[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(keyRange[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		m.FileNum = base.FileNum(fileNum)
		m.Size = uint64(256)

		if len(fields) > 1 {
			for _, field := range fields[1:] {
				parts := strings.Split(field, "=")
				switch parts[0] {
				case "base_compacting":
					m.IsIntraL0Compacting = false
					m.Compacting = true
				case "intra_l0_compacting":
					m.IsIntraL0Compacting = true
					m.Compacting = true
				case "compacting":
					m.Compacting = true
				case "size":
					sizeInt, err := strconv.Atoi(parts[1])
					if err != nil {
						return nil, err
					}
					m.Size = uint64(sizeInt)
				}
			}
		}

		return &m, nil
	}

	var level int
	var err error
	var fileMetas [NumLevels][]*FileMetadata
	var explicitSublevels [][]*FileMetadata
	var sublevels *L0SubLevels
	baseLevel := NumLevels - 1

	datadriven.RunTest(t, "testdata/l0_sublevels", func(td *datadriven.TestData) string {
		pickBaseCompaction := false
		switch td.Cmd {
		case "define":
			fileMetas = [NumLevels][]*FileMetadata{}
			explicitSublevels = [][]*FileMetadata{}
			baseLevel = NumLevels - 1
			sublevel := -1
			sublevels = nil
			for _, data := range strings.Split(td.Input, "\n") {
				data = strings.TrimSpace(data)
				switch data[:2] {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:2])
					if err != nil {
						return err.Error()
					}
					if level == 0 && len(data) > 3 {
						// Sublevel was specified.
						sublevel, err = strconv.Atoi(data[3:])
						if err != nil {
							return err.Error()
						}
					} else {
						sublevel = -1
					}
				default:
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					if level != 0 && level < baseLevel {
						baseLevel = level
					}
					fileMetas[level] = append(fileMetas[level], meta)
					if sublevel != -1 {
						for len(explicitSublevels) <= sublevel {
							explicitSublevels = append(explicitSublevels, []*FileMetadata{})
						}
						explicitSublevels[sublevel] = append(explicitSublevels[sublevel], meta)
					}
				}
			}

			flushSplitMaxBytes := 64
			initialize := true
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "flush_split_max_bytes":
					flushSplitMaxBytes, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						t.Fatal(err)
					}
				case "no_initialize":
					// This case is for use with explicitly-specified sublevels
					// only.
					initialize = false
				}
			}
			SortBySeqNum(fileMetas[0])
			for i := 1; i < NumLevels; i++ {
				SortBySmallest(fileMetas[i], base.DefaultComparer.Compare)
			}

			if initialize {
				sublevels, err = NewL0SubLevels(
					fileMetas[0],
					base.DefaultComparer.Compare,
					base.DefaultFormatter,
					int64(flushSplitMaxBytes))
				sublevels.InitCompactingFileInfo()
			} else {
				// This case is for use with explicitly-specified sublevels
				// only.
				sublevels = &L0SubLevels{
					Files:      explicitSublevels,
					cmp:        base.DefaultComparer.Compare,
					formatKey:  base.DefaultFormatter,
					filesByAge: fileMetas[0],
				}
			}

			if err != nil {
				t.Fatal(err)
			}

			var builder strings.Builder
			builder.WriteString(sublevels.describe(true))
			builder.WriteString(visualizeSublevels(sublevels, nil, fileMetas[1:]))
			return builder.String()
		case "pick-base-compaction":
			pickBaseCompaction = true
			fallthrough
		case "pick-intra-l0-compaction":
			minCompactionDepth := 3
			earliestUnflushedSeqNum := uint64(math.MaxUint64)
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "min_depth":
					minCompactionDepth, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						t.Fatal(err)
					}
				case "earliest_unflushed_seqnum":
					eusnInt, err := strconv.Atoi(arg.Vals[0])
					if err != nil {
						t.Fatal(err)
					}
					earliestUnflushedSeqNum = uint64(eusnInt)
				}
			}

			var lcf *L0CompactionFiles
			if pickBaseCompaction {
				lcf, err = sublevels.PickBaseCompaction(minCompactionDepth, fileMetas[baseLevel])
				if err == nil && lcf != nil {
					// Try to extend the base compaction into a more rectangular
					// shape, using the smallest/largest keys of overlapping
					// base files. This mimics the logic the compactor is
					// expected to implement.
					baseFiles := fileMetas[baseLevel]
					firstFile := sort.Search(len(baseFiles), func(i int) bool {
						return sublevels.cmp(baseFiles[i].Largest.UserKey, sublevels.orderedIntervals[lcf.minIntervalIndex].startKey.key) >= 0
					})
					lastFile := sort.Search(len(baseFiles), func(i int) bool {
						return sublevels.cmp(baseFiles[i].Smallest.UserKey, sublevels.orderedIntervals[lcf.maxIntervalIndex+1].startKey.key) >= 0
					})
					lastFile--
					sublevels.ExtendL0ForBaseCompactionTo(
						baseFiles[firstFile].Smallest.UserKey,
						baseFiles[lastFile].Largest.UserKey,
						lcf)
				}
			} else {
				lcf, err = sublevels.PickIntraL0Compaction(earliestUnflushedSeqNum, minCompactionDepth)
			}
			if err != nil {
				return fmt.Sprintf("error: %s", err.Error())
			}
			if lcf == nil {
				return "no compaction picked"
			}
			var builder strings.Builder
			builder.WriteString(fmt.Sprintf("compaction picked with stack depth reduction %d\n", lcf.seedIntervalStackDepthReduction))
			for i, file := range lcf.Files {
				builder.WriteString(file.FileNum.String())
				if i < len(lcf.Files)-1 {
					builder.WriteByte(',')
				}
			}
			startKey := sublevels.orderedIntervals[lcf.seedInterval].startKey
			endKey := sublevels.orderedIntervals[lcf.seedInterval+1].startKey
			builder.WriteString(fmt.Sprintf("\nseed interval: %s-%s\n", startKey.key, endKey.key))
			builder.WriteString(visualizeSublevels(sublevels, lcf.FilesIncluded, fileMetas[1:]))

			return builder.String()
		case "read-amp":
			return strconv.Itoa(sublevels.ReadAmplification())
		case "flush-split-keys":
			var builder strings.Builder
			builder.WriteString("flush user split keys: ")
			flushSplitKeys := sublevels.FlushSplitKeys()
			for i, key := range flushSplitKeys {
				builder.Write(key)
				if i < len(flushSplitKeys)-1 {
					builder.WriteString(", ")
				}
			}
			return builder.String()
		case "max-depth-after-ongoing-compactions":
			return strconv.Itoa(sublevels.MaxDepthAfterOngoingCompactions())
		case "l0-check-ordering":
			for sublevel, files := range sublevels.Files {
				if err := CheckOrdering(base.DefaultComparer.Compare,
					base.DefaultFormatter, L0Sublevel(sublevel), files); err != nil {
					return err.Error()
				}
			}
			return "OK"
		case "update-state-for-compaction":
			var fileNums []base.FileNum
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "files":
					for _, val := range arg.Vals {
						fileNum, err := strconv.ParseUint(val, 10, 64)
						if err != nil {
							return err.Error()
						}
						fileNums = append(fileNums, base.FileNum(fileNum))
					}
				}
			}
			files := make([]*FileMetadata, 0, len(fileNums))
			for _, num := range fileNums {
				for _, f := range fileMetas[0] {
					if f.FileNum == num {
						f.Compacting = true
						files = append(files, f)
						break
					}
				}
			}
			if err := sublevels.UpdateStateForStartedCompaction([][]*FileMetadata{files}, true); err != nil {
				return err.Error()
			}
			return "OK"
		case "describe":
			var builder strings.Builder
			builder.WriteString(sublevels.describe(true))
			builder.WriteString(visualizeSublevels(sublevels, nil, fileMetas[1:]))
			return builder.String()
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

func BenchmarkL0SubLevelsInit(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl, err := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, base.DefaultFormatter, 5<<20)
		require.NoError(b, err)
		if sl == nil {
			b.Fatal("expected non-nil L0SubLevels to be generated")
		}
	}
}

func BenchmarkL0SubLevelsInitAndPick(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl, err := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, base.DefaultFormatter, 5<<20)
		require.NoError(b, err)
		if sl == nil {
			b.Fatal("expected non-nil L0SubLevels to be generated")
		}
		c, err := sl.PickBaseCompaction(2, nil)
		require.NoError(b, err)
		if c == nil {
			b.Fatal("expected non-nil compaction to be generated")
		}
	}
}
