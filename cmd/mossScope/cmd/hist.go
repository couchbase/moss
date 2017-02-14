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

	"github.com/couchbase/ghistogram"
	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// histCmd represents the hist command
var histCmd = &cobra.Command{
	Use:   "hist",
	Short: "Generates histograms for the store",
	Long: `This command generates histograms for various entities
available from the store.
	./mossScope stats hist <path_to_store>`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeHist(args)
	},
}

func invokeHist(dirs []string) error {
	for _, dir := range dirs {
		store, err := moss.OpenStore(dir, ReadOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}

		snap, err := store.Snapshot()
		if err != nil || snap == nil {
			fmt.Errorf("Store-Snapshot() API failed, err: %v", err)
		}

		iter, err := snap.StartIterator(nil, nil, moss.IteratorOptions{})
		if err != nil || iter == nil {
			return fmt.Errorf("Snaphot-StartItr() API failed, err: %v", err)
		}

		keySizes := ghistogram.NewNamedHistogram("KeySizes(B) ", 10, 4, 4)
		valSizes := ghistogram.NewNamedHistogram("ValSizes(B) ", 10, 4, 4)

		for {
			k, v, err := iter.Current()
			if err != nil {
				break
			}

			keySizes.Add(uint64(len(k)), 1)
			valSizes.Add(uint64(len(v)), 1)

			if iter.Next() == moss.ErrIteratorDone {
				break
			}
		}

		fmt.Printf("\"%s\"\n", dir)
		fmt.Println((keySizes.EmitGraph(nil, nil)).String())
		fmt.Println((valSizes.EmitGraph(nil, nil)).String())

		iter.Close()
		snap.Close()
		store.Close()
	}

	return nil
}

func init() {
	statsCmd.AddCommand(histCmd)
}
