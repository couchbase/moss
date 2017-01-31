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

// footerStatsCmd represents the all command
var footerStatsCmd = &cobra.Command{
	Use:   "footer",
	Short: "Dumps aggregated stats from the latest footer of the store",
	Long: `This command dumps the aggregated stats from all segments
collected from the latest footer of the store.
	./mossScope stats footer <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeFooterStats(args)
	},
}

var getAll bool

func invokeFooterStats(dirs []string) error {
	if jsonFormat {
		fmt.Printf("[")
	}
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, moss.StoreOptions{})
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}
		defer store.Close()

		curr_snap, err := store.Snapshot()
		if err != nil || curr_snap == nil {
			continue
		}

		type stats map[string]uint64
		footer_stats := make(map[string]stats)
		id := 1

		for {

			footer := curr_snap.(*moss.Footer)
			footer_id := fmt.Sprintf("Footer_%d", id)
			footer_stats[footer_id] = make(stats)

			footer_stats[footer_id]["num_segments"] =
				uint64(len(footer.SegmentLocs))
			footer_stats[footer_id]["total_ops_set"] = 0
			footer_stats[footer_id]["total_ops_del"] = 0
			footer_stats[footer_id]["total_key_bytes"] = 0
			footer_stats[footer_id]["total_val_bytes"] = 0

			for i := range footer.SegmentLocs {
				sloc := &footer.SegmentLocs[i]

				footer_stats[footer_id]["total_ops_set"] += sloc.TotOpsSet
				footer_stats[footer_id]["total_ops_del"] += sloc.TotOpsDel
				footer_stats[footer_id]["total_key_bytes"] += sloc.TotKeyByte
				footer_stats[footer_id]["total_val_bytes"] += sloc.TotValByte
			}

			if !getAll {
				break
			}

			prev_snap, err := store.SnapshotPrevious(curr_snap)
			curr_snap.Close()
			curr_snap = prev_snap
			id++

			if err != nil || curr_snap == nil {
				break
			}
		}

		if jsonFormat {
			jBuf, err := json.Marshal(footer_stats)
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
			for f, fstats := range footer_stats {
				fmt.Printf("  %s\n", f)
				for k, v := range fstats {
					fmt.Printf("%25s : %v\n", k, v)
				}
			}
			fmt.Println()
		}
	}

	if jsonFormat {
		fmt.Printf("]\n")
	}

	return nil
}

func init() {
	statsCmd.AddCommand(footerStatsCmd)

	// Local flag that is intended to work as a flag over stats footer
	footerStatsCmd.Flags().BoolVar(&getAll, "all", false,
		"Fetches stats from all available footers (Footer_1 is latest)")
}
