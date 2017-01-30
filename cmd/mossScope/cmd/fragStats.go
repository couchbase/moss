// Copyright © 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// fragStatsCmd represents the frag command
var fragStatsCmd = &cobra.Command{
	Use:   "fragmentation",
	Short: "Dumps the fragmentation stats",
	Long: `This command dumps the key-val sizes and directory size info,
and alongside that estimates the fragmentation levels. This data
could assist with decisions around invoking manual compaction.
	./mossScope stats frag <path_to_store>`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("USAGE: mossScope stats frag <path_to_store>, " +
				"more details with --help")
			return
		}

		fragStats(args)
	},
}

func fragStats(dirs []string) {
	if len(dirs) == 0 {
		return
	}

	if jsonFormat {
		fmt.Printf("[")
	}
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, moss.StoreOptions{})
		if err != nil || store == nil {
			fmt.Printf("Moss-OpenStore() API failed, err: %v\n", err)
			os.Exit(-1)
		}
		defer store.Close()

		stats_map := make(map[string]int64)

		stats_map["data_bytes"] = 0
		stats_map["dir_size"] = 0
		stats_map["fragmentation_bytes"] = 0
		stats_map["fragmentation_percent"] = 0

		read_size := func(dir string, file os.FileInfo, err error) error {
			if !file.IsDir() {
				stats_map["dir_size"] += file.Size()
			}

			return nil
		}

		curr_snap, err := store.Snapshot()
		if err != nil || curr_snap == nil {
			continue
		}

		footer := curr_snap.(*moss.Footer)

		// Acquire key, val bytes from all segments of latest footer
		for i := range footer.SegmentLocs {
			sloc := &footer.SegmentLocs[i]

			stats_map["data_bytes"] += int64(sloc.TotKeyByte)
			stats_map["data_bytes"] += int64(sloc.TotValByte)
		}

		for {
			// header signature
			stats_map["data_bytes"] += 4096

			// footer signature
			footer = curr_snap.(*moss.Footer)
			jBuf, err := json.Marshal(footer)
			if err != nil {
				fmt.Printf("Json-Marshal() failed!, err: %v\n", err)
				os.Exit(-1)
			}

			stats_map["data_bytes"] += int64(len(jBuf)) // footer length

			// Also account for the magic that repeats twice at start and end
			stats_map["data_bytes"] += int64(2 * len(moss.STORE_MAGIC_BEG))
			stats_map["data_bytes"] += int64(2 * len(moss.STORE_MAGIC_END))

			prev_snap, err := store.SnapshotPrevious(curr_snap)
			curr_snap.Close()
			curr_snap = prev_snap

			if err != nil || curr_snap == nil {
				break
			}
		}

		filepath.Walk(dir, read_size)

		stats_map["fragmentation_bytes"] = stats_map["dir_size"] -
			stats_map["data_bytes"]
		stats_map["fragmentation_percent"] = int64(100 *
			((float64(stats_map["fragmentation_bytes"])) /
				float64(stats_map["dir_size"])))

		if jsonFormat {
			jBuf, err := json.Marshal(stats_map)
			if err != nil {
				fmt.Printf("Json-Marshal() failed!, err: %v\n", err)
			}
			if index != 0 {
				fmt.Printf(",")
			}
			fmt.Printf("{\"%s\":", dir)
			fmt.Printf("%s}", string(jBuf))
		} else {
			fmt.Println(dir)
			for k, v := range stats_map {
				fmt.Printf("%25s : %v\n", k, v)
			}
			fmt.Println()
		}
	}

	if jsonFormat {
		fmt.Printf("]\n")
	}
}

// The following wrapper (public) is for test purposes
func FragStats(dir string) {
	dirs := []string{dir}
	jsonFormat = true
	fragStats(dirs)
}

func init() {
	statsCmd.AddCommand(fragStatsCmd)
}
