// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build linux

package vfs

import "golang.org/x/sys/unix"

func fadviseRandom(f File) error {
	type fd interface {
		Fd() uintptr
	}
	if d, ok := f.(fd); ok {
		return unix.Fadvise(int(d.Fd()), 0, 0, unix.FADV_RANDOM)
	}
	return nil
}
