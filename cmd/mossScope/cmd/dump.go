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
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/couchbase/moss"
	"github.com/spf13/cobra"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dumps key/val data in the specified store",
	Long: `Dumps every key-value persisted in the store in JSON
format. It has a set of options that it can used with.
For example:
	./mossScope dump [sub-command] <path_to_store> [flag]`,

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("At least one path is required!")
		}
		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		return invokeDump(args)
	},
}

var keysOnly bool
var inHex bool

func invokeDump(dirs []string) error {
	fmt.Printf("[")
	for index, dir := range dirs {
		store, err := moss.OpenStore(dir, ReadOnlyMode)
		if err != nil || store == nil {
			return fmt.Errorf("Moss-OpenStore() API failed, err: %v", err)
		}

		snap, err := store.Snapshot()
		if err != nil || snap == nil {
			return fmt.Errorf("Store-Snapshot() API failed, err: %v", err)
		}

		iter, err := snap.StartIterator(nil, nil, moss.IteratorOptions{})
		if err != nil || iter == nil {
			return fmt.Errorf("Snapshot-StartItr() API failed, err: %v", err)
		}

		if index != 0 {
			fmt.Printf(",")
		}
		fmt.Printf("{\"%s\":", dir)

		fmt.Printf("[")
		for {
			k, v, err := iter.Current()
			if err != nil {
				break
			}

			if keysOnly {
				err = dumpKeyVal(k, nil, inHex)
			} else {
				err = dumpKeyVal(k, v, inHex)
			}

			if err != nil {
				return err
			}

			if iter.Next() == moss.ErrIteratorDone {
				break
			}

			fmt.Printf(",")
		}
		fmt.Printf("]")

		iter.Close()
		snap.Close()
		store.Close()

		fmt.Printf("}")
	}
	fmt.Printf("]\n")

	return nil
}

func dumpKeyVal(key []byte, val []byte, toHex bool) error {
	if toHex {
		if val == nil {
			fmt.Printf("{\"k\":\"%s\"}", hex.EncodeToString(key))
		} else {
			fmt.Printf("{\"k\":\"%s\",\"v\":\"%s\"}",
				hex.EncodeToString(key), hex.EncodeToString(val))
		}
	} else {
		jBufk, err := json.Marshal(string(key))
		if err != nil {
			return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
		}
		if val == nil {
			fmt.Printf("{\"k\":%s}", string(jBufk))
		} else {
			jBufv, err := json.Marshal(string(val))
			if err != nil {
				return fmt.Errorf("Json-Marshal() failed!, err: %v", err)
			}
			fmt.Printf("{\"k\":%s,\"v\":%s}",
				string(jBufk), string(jBufv))
		}
	}
	return nil
}

func init() {
	RootCmd.AddCommand(dumpCmd)

	// Local flags that are intended to work as a filter over dump
	dumpCmd.Flags().BoolVar(&keysOnly, "keys-only", false,
		"Emits keys only, works on dump without sub-commands")
	dumpCmd.Flags().BoolVar(&inHex, "hex", false,
		"Emits output in hex")
}
