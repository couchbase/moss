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

	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Retrieves all the store related stats",
	Long: `This command can be used to dump stats available for the
specific moss store.
	./mossScope stats <sub-command> <path_to_store>`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("USAGE: mossScope stats <sub_command> <path_to_store>, " +
			"more details with --help")
	},
}

var jsonFormat bool

func init() {
	RootCmd.AddCommand(statsCmd)

	// Persistent flag that would work for current commands and sub commands
	statsCmd.PersistentFlags().BoolVar(&jsonFormat, "json", false,
		"Emits output in JSON")
}
