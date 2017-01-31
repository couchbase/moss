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

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// keyCmd represents the key command
var keyCmd = &cobra.Command{
	Use:   "key",
	Short: "Dumps the key and value of the specified key",
	Long: `Dumps the key and value information of the requested key
from the latest snapshot in which it is available in JSON
format. For example:
	./mossScope dump key <keyname> <path_to_store> [flag]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("A keyname along with at least one path " +
				"are required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeKey(args[0], args[1:len(args)])
	},
}

var allVersions bool

func invokeKey(keyname string, dirs []string) error {
	fmt.Printf("[")
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, moss.StoreOptions{})
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}

		snap, err := store.Snapshot()
		if err != nil || snap == nil {
			return fmt.Errorf("Store-Snapshot() API failed, err: %v", err)
		}

		curr_snapshot := snap
		val, err := curr_snapshot.Get([]byte(keyname), moss.ReadOptions{})
		if err == nil && val != nil {
			if index != 0 {
				fmt.Printf(",")
			}
			fmt.Printf("{\"%s\":[", dir)

			err = dumpKeyVal([]byte(keyname), val, inHex)
			if err != nil {
				return err
			}

			if allVersions {
				for {
					prev_snapshot, err := store.SnapshotPrevious(curr_snapshot)
					curr_snapshot.Close()
					curr_snapshot = prev_snapshot

					if err != nil || curr_snapshot == nil {
						break
					}

					val, err := curr_snapshot.Get([]byte(keyname),
						moss.ReadOptions{})
					if err == nil && val != nil {
						fmt.Printf(",")
						err = dumpKeyVal([]byte(keyname), val, inHex)
						if err != nil {
							return err
						}
					}
				}
			}
			fmt.Printf("]}")
		}

		snap.Close()
		store.Close()

	}
	fmt.Printf("]\n")

	return nil
}

func init() {
	dumpCmd.AddCommand(keyCmd)

	// Local flags that are intended to work as a flag over dump key
	keyCmd.Flags().BoolVar(&allVersions, "all-versions", false,
		"Emits all the available versions of the key")
	keyCmd.Flags().BoolVar(&inHex, "hex", false,
		"Emits output in hex")
}
