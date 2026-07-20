//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"os"
	"path/filepath"
	"testing"
)

// TestRemoveFilesToleratesMissing guards the fix for an intermittent
// reopen failure: OpenStore lists the directory, then removes stale
// files, but store-close/compaction removes superseded files
// asynchronously (removeFileOnClose spawns a goroutine).  A file can
// therefore already be gone by the time removeFiles() runs, and an
// already-absent file must not be treated as an error (else
// OpenStore/OpenStoreCollection intermittently returns a nil store).
func TestRemoveFilesToleratesMissing(t *testing.T) {
	dir := t.TempDir()

	real := "data-0000000000000001.moss"
	if err := os.WriteFile(filepath.Join(dir, real), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	// The list mixes a present file with an already-removed one.
	err := removeFiles(dir, []string{real, "data-0000000000000002.moss"})
	if err != nil {
		t.Fatalf("removeFiles must tolerate an already-missing file, got: %v", err)
	}

	// The present file must actually have been removed.
	if _, err := os.Stat(filepath.Join(dir, real)); !os.IsNotExist(err) {
		t.Fatalf("expected %q to be removed, stat err: %v", real, err)
	}

	// Removing an entirely-empty/all-missing set is a no-op success.
	if err := removeFiles(dir, []string{"nope-a.moss", "nope-b.moss"}); err != nil {
		t.Fatalf("removeFiles over all-missing files should succeed, got: %v", err)
	}
}
