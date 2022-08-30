// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testkeys/blockprop"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestExternalIterator(t *testing.T) {
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		Comparer:           testkeys.Comparer,
		FormatMajorVersion: FormatRangeKeys,
	}
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	datadriven.RunTest(t, "testdata/external_iterator", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			opts := IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var externalIterOpts []ExternalIterOption
			var files [][]sstable.ReadableFile
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "fwd-only":
					externalIterOpts = append(externalIterOpts, ExternalIterForwardOnly{})
				case "mask-suffix":
					opts.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				case "files":
					for _, v := range arg.Vals {
						f, err := mem.Open(v)
						require.NoError(t, err)
						files = append(files, []sstable.ReadableFile{f})
					}
				}
			}
			it, err := NewExternalIter(o, &opts, files, externalIterOpts...)
			require.NoError(t, err)
			return runIterCmd(td, it, true /* close iter */)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestSimpleLevelIter(t *testing.T) {
	mem := vfs.NewMem()
	o := &Options{
		FS:                 mem,
		Comparer:           testkeys.Comparer,
		FormatMajorVersion: FormatRangeKeys,
	}
	o.EnsureDefaults()
	d, err := Open("", o)
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()

	datadriven.RunTest(t, "testdata/simple_level_iter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			mem = vfs.NewMem()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			var files []sstable.ReadableFile
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "files":
					for _, v := range arg.Vals {
						f, err := mem.Open(v)
						require.NoError(t, err)
						files = append(files, f)
					}
				}
			}
			readers, err := openExternalTables(o, files, 0, o.MakeReaderOptions())
			require.NoError(t, err)
			defer func() {
				for i := range readers {
					_ = readers[i].Close()
				}
			}()
			var internalIters []internalIterator
			for i := range readers {
				iter, err := readers[i].NewIter(nil, nil)
				require.NoError(t, err)
				internalIters = append(internalIters, iter)
			}
			it := &simpleLevelIter{cmp: o.Comparer.Compare, iters: internalIters}
			it.init(IterOptions{})

			response := runInternalIterCmd(td, it)
			require.NoError(t, it.Close())
			return response
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestSimpleIterError(t *testing.T) {
	s := simpleLevelIter{cmp: DefaultComparer.Compare, iters: []internalIterator{&errorIter{err: errors.New("injected")}}}
	s.init(IterOptions{})
	defer s.Close()

	iterKey, _ := s.First()
	require.Nil(t, iterKey)
	require.Error(t, s.Error())
}

func TestIterRandomizedMaybeFilteredKeys(t *testing.T) {
	mem := vfs.NewMem()

	seed := *seed
	if seed == 0 {
		seed = uint64(time.Now().UnixNano())
		fmt.Printf("seed: %d\n", seed)
	}
	rng := rand.New(rand.NewSource(seed))
	numKeys := 100 + rng.Intn(5000)
	// The block property filter will exclude keys with suffixes [0, tsSeparator-1].
	// We use the first "part" of the keyspace below to write keys >= tsSeparator,
	// and the second part to write keys < tsSeparator. Successive parts (if any)
	// will contain keys at random before or after the separator.
	tsSeparator := 10 + rng.Intn(5000)
	const keyLen = 5

	// We split the keyspace into logical "parts" which are disjoint slices of the
	// keyspace. That is, the keyspace a-z could be comprised of parts {a-k, l-z}.
	// We rely on this partitioning when generating timestamps to give us some
	// predictable clustering of timestamps in sstable blocks, however it is not
	// strictly necessary for this test.
	alpha := testkeys.Alpha(keyLen)
	numParts := rng.Intn(3) + 2
	blockSize := 16 + rng.Intn(64)

	c := cache.New(128 << 20)
	defer c.Unref()

	for fileIdx, twoLevelIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("twoLevelIndex=%v", twoLevelIndex), func(t *testing.T) {
			keys := make([][]byte, 0, numKeys)

			filename := fmt.Sprintf("test-%d", fileIdx)
			f0, err := mem.Create(filename)
			require.NoError(t, err)

			indexBlockSize := 4096
			if twoLevelIndex {
				indexBlockSize = 1
			}
			w := sstable.NewWriter(f0, sstable.WriterOptions{
				BlockSize:      blockSize,
				Comparer:       testkeys.Comparer,
				IndexBlockSize: indexBlockSize,
				TableFormat:    sstable.TableFormatPebblev2,
				BlockPropertyCollectors: []func() BlockPropertyCollector{
					func() BlockPropertyCollector {
						return blockprop.NewBlockPropertyCollector()
					},
				},
			})
			buf := make([]byte, alpha.MaxLen()+testkeys.MaxSuffixLen)
			valBuf := make([]byte, 20)
			keyIdx := 0
			for i := 0; i < numParts; i++ {
				// The first two parts of the keyspace are special. The first one has
				// all keys with timestamps greater than tsSeparator, while the second
				// one has all keys with timestamps less than tsSeparator. Any additional
				// keys could have timestamps at random before or after the tsSeparator.
				maxKeysPerPart := numKeys / numParts
				for j := 0; j < maxKeysPerPart; j++ {
					ts := 0
					if i == 0 {
						ts = rng.Intn(5000) + tsSeparator
					} else if i == 1 {
						ts = rng.Intn(tsSeparator)
					} else {
						ts = rng.Intn(tsSeparator + 5000)
					}
					n := testkeys.WriteKeyAt(buf, alpha, keyIdx*alpha.Count()/numKeys, ts)
					keys = append(keys, append([]byte(nil), buf[:n]...))
					randStr(valBuf, rng)
					require.NoError(t, w.Set(buf[:n], valBuf))
					keyIdx++
				}
			}
			require.NoError(t, w.Close())

			// Re-open that filename for reading.
			f1, err := mem.Open(filename)
			require.NoError(t, err)

			r, err := sstable.NewReader(f1, sstable.ReaderOptions{
				Cache:    c,
				Comparer: testkeys.Comparer,
			})
			require.NoError(t, err)
			defer r.Close()

			filter := blockprop.NewBlockPropertyFilter(uint64(tsSeparator), math.MaxUint64)
			filterer := sstable.NewBlockPropertiesFilterer([]BlockPropertyFilter{filter}, nil)
			ok, err := filterer.IntersectsUserPropsAndFinishInit(r.Properties.UserProperties)
			require.True(t, ok)
			require.NoError(t, err)

			var iter sstable.Iterator
			iter, err = r.NewIterWithBlockPropertyFilters(nil, nil, filterer, false /* useFilterBlock */, nil /* stats */)
			require.NoError(t, err)
			defer iter.Close()
			var lastSeekKey, lowerBound, upperBound []byte
			narrowBoundsMode := false

			for i := 0; i < 10000; i++ {
				if rng.Intn(8) == 0 {
					// Toggle narrow bounds mode.
					if narrowBoundsMode {
						// Reset bounds.
						lowerBound, upperBound = nil, nil
						iter.SetBounds(nil /* lower */, nil /* upper */)
					}
					narrowBoundsMode = !narrowBoundsMode
				}
				keyIdx := rng.Intn(len(keys))
				seekKey := keys[keyIdx]
				if narrowBoundsMode {
					// Case 1: We just entered narrow bounds mode, and both bounds
					// are nil. Set a lower/upper bound.
					//
					// Case 2: The seek key is outside our last bounds.
					//
					// In either case, pick a narrow range of keys to set bounds on,
					// let's say keys[keyIdx-5] and keys[keyIdx+5], before doing our
					// seek operation. Picking narrow bounds increases the chance of
					// monotonic bound changes.
					cmp := testkeys.Comparer.Compare
					case1 := lowerBound == nil && upperBound == nil
					case2 := (lowerBound != nil && cmp(lowerBound, seekKey) > 0) || (upperBound != nil && cmp(upperBound, seekKey) <= 0)
					if case1 || case2 {
						lowerBound = nil
						if keyIdx-5 >= 0 {
							lowerBound = keys[keyIdx-5]
						}
						upperBound = nil
						if keyIdx+5 < len(keys) {
							upperBound = keys[keyIdx+5]
						}
						iter.SetBounds(lowerBound, upperBound)
					}
					// Case 3: The current seek key is within the previously-set bounds.
					// No need to change bounds.
				}
				flags := base.SeekGEFlagsNone
				if lastSeekKey != nil && bytes.Compare(seekKey, lastSeekKey) > 0 {
					flags = flags.EnableTrySeekUsingNext()
				}
				lastSeekKey = append(lastSeekKey[:0], seekKey...)

				newKey, _ := iter.SeekGE(seekKey, flags)
				if newKey == nil || !bytes.Equal(newKey.UserKey, seekKey) {
					// We skipped some keys. Check if maybeFilteredKeys is true.
					formattedNewKey := "<nil>"
					if newKey != nil {
						formattedNewKey = fmt.Sprintf("%s", testkeys.Comparer.FormatKey(newKey.UserKey))
					}
					require.True(t, iter.MaybeFilteredKeys(), "seeked for key = %s, got key = %s indicating block property filtering but MaybeFilteredKeys = false", testkeys.Comparer.FormatKey(seekKey), formattedNewKey)
				}
			}
		})
	}
}
