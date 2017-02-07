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

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeFragStats(args)
	},
}

func invokeFragStats(dirs []string) error {
	if jsonFormat {
		fmt.Printf("[")
	}
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, moss.StoreOptions{})
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}
		defer store.Close()

		stats_map := make(map[string]uint64)

		err = fetchFragStats(store, stats_map)
		if err != nil {
			return err
		}

		if jsonFormat {
			jBuf, err := json.Marshal(stats_map)
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
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

	return nil
}

func fetchFragStats(store *moss.Store, stats map[string]uint64) error {
	if store == nil {
		return nil
	}

	stats["data_bytes"] = 0
	stats["dir_size"] = 0
	stats["fragmentation_bytes"] = 0
	stats["fragmentation_percent"] = 0

	curr_snap, err := store.Snapshot()
	if err != nil || curr_snap == nil {
		return nil
	}

	footer := curr_snap.(*moss.Footer)

	// Acquire key, val bytes from all segments of latest footer
	for i := range footer.SegmentLocs {
		sloc := &footer.SegmentLocs[i]

		stats["data_bytes"] += sloc.TotKeyByte
		stats["data_bytes"] += sloc.TotValByte
	}

	for {
		// header signature
		stats["data_bytes"] += moss.HeaderLength()

		// footer signature
		footer = curr_snap.(*moss.Footer)
		stats["data_bytes"] += footer.Length()

		prev_snap, err := store.SnapshotPrevious(curr_snap)
		curr_snap.Close()
		curr_snap = prev_snap

		if err != nil || curr_snap == nil {
			break
		}
	}

	sstats, err := store.Stats()
	if err != nil {
		return fmt.Errorf("Store-Stats() failed!, err: %v", err)
	}

	stats["dir_size"] = sstats["num_bytes_used_disk"].(uint64)

	stats["fragmentation_bytes"] = stats["dir_size"] - stats["data_bytes"]
	stats["fragmentation_percent"] = uint64(100 *
		((float64(stats["fragmentation_bytes"])) /
			float64(stats["dir_size"])))

	return nil
}

func init() {
	statsCmd.AddCommand(fragStatsCmd)

	// Local flag that is intended to work over stats fragmentation
	fragStatsCmd.Flags().BoolVar(&jsonFormat, "json", false,
		"Emits output in JSON")
}
