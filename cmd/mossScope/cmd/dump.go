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
	"encoding/hex"
	"encoding/json"

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
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Println("USAGE: mossScope dump [sub-command] <path_to_store> [flag], more details with --help");
			return
		}

		dump(args[0])
	},
}

var keysOnly bool
var inHex bool

func dump(dir string) {
	store, err := moss.OpenStore(dir, moss.StoreOptions{})
	if err != nil || store == nil {
		fmt.Printf("Moss-OpenStore() API failed, err: %v\n", err)
		os.Exit(-1)
	}

	snap, err := store.Snapshot()
	if err != nil || snap == nil {
		fmt.Printf("Store-Snapshot() API failed, err: %v\n", err)
		os.Exit(-1)
	}

	iter, err := snap.StartIterator(nil, nil, moss.IteratorOptions{})
	if err != nil || iter == nil {
		fmt.Printf("Snapshot-StartIterator() API failed, err: %v\n", err)
		os.Exit(-1)
	}

	fmt.Printf("[\n")
	for {
		k, v, err := iter.Current()
		if err != nil {
			break
		}

		if keysOnly {
			dumpKeyVal(k, nil, inHex)
		} else {
			dumpKeyVal(k, v, inHex)
		}

		if iter.Next() ==  moss.ErrIteratorDone {
			fmt.Printf("\n")
			break
		} else {
			fmt.Printf(",\n")
		}
	}
	fmt.Printf("]\n")

	iter.Close()
	snap.Close()
	store.Close()
}

func dumpKeyVal(key []byte, val []byte, toHex bool) {
	if toHex {
		if val == nil {
			fmt.Printf("  { \"K\" : \"%s\" }", hex.EncodeToString(key))
		} else {
			fmt.Printf("  { \"K\" : \"%s\", \"V\" : \"%s\" }", hex.EncodeToString(key), hex.EncodeToString(val))
		}
	} else {
		jBufk, err := json.Marshal(string(key))
		if err != nil {
			fmt.Printf("Json-Marshal() failed!, err: %v\n", err)
			os.Exit(-1)
		}
		if (val == nil) {
			fmt.Printf("  { \"K\" : %s }", string(jBufk))
		} else {
			jBufv, err := json.Marshal(string(val))
			if err != nil {
				fmt.Printf("Json-Marshal() failed!, err: %v\n", err)
				os.Exit(-1)
			}
			fmt.Printf("  { \"K\" : %s, \"V\" : %s }", string(jBufk), string(jBufv))
		}
	}
}

// The following wrapper (public) is for test purposes
func Dump(dir string, onlyKeys bool) {
	keysOnly = onlyKeys
	dump(dir)
}

func init() {
	RootCmd.AddCommand(dumpCmd)

	// Local flag that is intended to work as a filter over dump
	dumpCmd.Flags().BoolVar(&keysOnly, "keys-only", false, "Emits keys only, works on dump without sub-commands")
	// Persistent flag that would work for current command and sub commands
	dumpCmd.PersistentFlags().BoolVar(&inHex, "hex", false, "Emits output in hex")
}
