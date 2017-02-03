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

// footerCmd represents the footer command
var footerCmd = &cobra.Command{
	Use:   "footer",
	Short: "Dumps the latest footer in the store",
	Long: `This command will print out the latest footer in JSON
format, (optionally all) here's a sample command:
	./mossScope dump footer <path_to_store> [flag]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeFooter(args)
	},
}

var allAvailable bool

func invokeFooter(dirs []string) error {
	fmt.Printf("[")
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, ReadOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}

		curr_snap, err := store.Snapshot()
		if err != nil || curr_snap == nil {
			return fmt.Errorf("Store-Snapshot() API failed, err: %v", err)
		}

		if index != 0 {
			fmt.Printf(",")
		}
		fmt.Printf("{\"%s\":[", dir)

		if allAvailable {
			for {
				jBuf, err := json.Marshal(curr_snap.(*moss.Footer))
				if err != nil {
					return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
				}

				fmt.Printf("%s", string(jBuf))

				prev_snap, err := store.SnapshotPrevious(curr_snap)
				curr_snap.Close()
				curr_snap = prev_snap

				if err != nil || curr_snap == nil {
					fmt.Printf("")
					break
				}
				fmt.Printf(",")
			}
		} else {
			jBuf, err := json.Marshal(curr_snap.(*moss.Footer))
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}

			fmt.Printf("%s", string(jBuf))

			curr_snap.Close()
		}
		fmt.Printf("]}")

		store.Close()
	}
	fmt.Printf("]\n")

	return nil
}

func init() {
	dumpCmd.AddCommand(footerCmd)

	// Local flag that is intended to work as a flag over dump footer
	footerCmd.Flags().BoolVar(&allAvailable, "all", false,
		"Fetches all the available footers")
}
