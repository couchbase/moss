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
	"fmt"
	"os"
	"encoding/json"

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
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Println("USAGE: mossScope dump footer <path_to_store>, " +
			            "more details with --help");
			return
		}

		footer(args)
	},
}

var allAvailable bool

func footer(dirs []string) {
	if len(dirs) == 0 {
		return
	}

	fmt.Printf("[")
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, moss.StoreOptions{})
		if err != nil || store == nil {
			fmt.Printf("Moss-OpenStore() API failed, err: %v\n", err)
			os.Exit(-1)
		}

		curr_snap, err := store.Snapshot()
		if index != 0 {
			fmt.Printf(",")
		}
		fmt.Printf("{\"%s\":[", dir)

		if allAvailable {
			for {
				if err != nil || curr_snap == nil {
					break
				}

				jBuf, err := json.Marshal(curr_snap.(*moss.Footer))
				if err != nil {
					fmt.Printf("Json-Marshal() failed!, err: %v\n", err)
					os.Exit(-1)
				}

				fmt.Printf("%s", string(jBuf))

				prev_snap, err := store.SnapshotPrevious(curr_snap)
				curr_snap.Close()
				curr_snap = prev_snap

				if curr_snap != nil {
					fmt.Printf(",")
				} else {
					fmt.Printf("")
					break
				}
			}
		} else {
			if err == nil && curr_snap != nil {
				jBuf, err := json.Marshal(curr_snap.(*moss.Footer))
				if err != nil {
					fmt.Printf("Json-Marshal() failed!, err: %v\n", err)
					os.Exit(-1)
				}

				fmt.Printf("%s", string(jBuf))

				curr_snap.Close()
			}
		}
		fmt.Printf("]}")

		store.Close()
	}
	fmt.Printf("]\n")
}

// The following wrapper (public) is for test purposes
func Footer(dir string, getAll bool) {
	dirs := []string{dir}
	allAvailable = getAll
	footer(dirs)
}

func init() {
	dumpCmd.AddCommand(footerCmd)

	// Local flag that is intended to work as a flag over dump footer
	footerCmd.Flags().BoolVar(&allAvailable, "all", false,
	                          "Fetches all the available footers")
}
