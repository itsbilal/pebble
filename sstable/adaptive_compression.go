// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/block"
)

type AdaptiveCompressionResolver interface {
	// Resolve returns the compression algorithm to use for a given block.
	Resolve(info BlockCompressionInfo) block.Compression
}

// SstableCompressionInfo is used by AdaptiveComperssionResolver to
// decide on a compression algorithm for a given sstable block.
type BlockCompressionInfo struct {
	Level int
	Key   []byte
}

type AdaptiveCompressionResolverFunc func(info BlockCompressionInfo) block.Compression

func (f AdaptiveCompressionResolverFunc) Resolve(info BlockCompressionInfo) block.Compression {
	return f(info)
}

var SimpleAdaptiveCompressionResolver AdaptiveCompressionResolverFunc = func(info BlockCompressionInfo) block.Compression {
	switch info.Level {
	case 0, 1, 2:
		return block.NoCompression
	case 3, 4, 5:
		return block.SnappyCompression
	case 6:
		return block.ZstdCompression
	default:
		panic(fmt.Sprintf("unexpected level: %d\n", info.Level))
	}
}

type levelCompressionState struct {
	boundaryKeys [][]byte
	age          []float64
}

type StatefulAdaptiveCompressionResolver struct {
	Cmp base.Compare
	mu  struct {
		sync.RWMutex

		levels [6]levelCompressionState
	}
}

func (r *StatefulAdaptiveCompressionResolver) Resolve(info BlockCompressionInfo) block.Compression {
	if info.Level == 0 {
		return block.NoCompression
	}

	const lowCompression = block.SnappyCompression
	const mediumCompression = block.S2Compression
	const highCompression = block.ZstdCompression

	r.mu.RLock()
	defer r.mu.RUnlock()

	lcState := &r.mu.levels[info.Level-1]

	if len(lcState.boundaryKeys) == 0 {
		// No metrics for this level.
		return SimpleAdaptiveCompressionResolver(info)
	}
	i := sort.Search(len(lcState.boundaryKeys), func(i int) bool {
		return r.Cmp(lcState.boundaryKeys[i], info.Key) >= 0
	})
	i--
	if i < 0 {
		// We're past the first boundary key.
		i++
	}
	switch {
	case lcState.age[i] < 0.2:
		return block.NoCompression
	case lcState.age[i] < 0.4:
		return lowCompression
	case lcState.age[i] < 0.6:
		switch info.Level {
		case 0, 1, 2, 3, 4:
			return lowCompression
		default:
			return mediumCompression
		}
	case lcState.age[i] < 0.8:
		return mediumCompression
	default:
		switch info.Level {
		case 0, 1, 2, 3, 4, 5:
			return mediumCompression
		default:
			return highCompression
		}
	}
}

func (r *StatefulAdaptiveCompressionResolver) UpdateMetricsForLevel(level int, startKeys [][]byte, age []float64) {
	if level == 0 {
		// Nothing to do.
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.levels[level-1].boundaryKeys = append(r.mu.levels[level-1].boundaryKeys[:0], startKeys...)
	r.mu.levels[level-1].age = append(r.mu.levels[level-1].age[:0], age...)
}
