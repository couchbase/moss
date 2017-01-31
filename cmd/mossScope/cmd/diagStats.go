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

// diagStatsCmd represents the diag command
var diagStatsCmd = &cobra.Command{
	Use:   "diag",
	Short: "Dumps all the diagnostic stats",
	Long: `This command dumps all the diagnostic stats for the store.
	./mossScope stats diag <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeDiagStats(args)
	},
}

func invokeDiagStats(dirs []string) error {
	if jsonFormat {
		fmt.Printf("[")
	}

	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, moss.StoreOptions{})
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}
		defer store.Close()

		snap, err := store.Snapshot()
		if err != nil || snap == nil {
			continue
		}

		footer := snap.(*moss.Footer)
		stats := make(map[string]uint64)

		fetchFooterStats(footer, stats)

		store_stats, err := store.Stats()
		if err != nil {
			return fmt.Errorf("Store-Stats() failed!, err: %v", err)
		}

		for k, v := range store_stats {
			stats[k] = v.(uint64)
		}

		if jsonFormat {
			jBuf, err := json.Marshal(stats)
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}
			if index != 0 {
				fmt.Printf(",")
			}
			fmt.Printf("{\"%s\":%s}", dir, string(jBuf))
		} else {
			fmt.Println(dir)
			for k, v := range stats {
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

func init() {
	statsCmd.AddCommand(diagStatsCmd)
}
