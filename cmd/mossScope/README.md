mossScope (v0.1.0)
==================

mossScope is a diagnostic command line tool, that is designed to assist
in browsing contents of, acquiring stats from, and otherwise diagnosing
moss store.

Standalone: How to use
----------------------
    (set $GOPATH)
    $ go get github.com/spf13/cobra
    $ go get github.com/couchbase/moss
    $ cd $GOPATH/src/github.com/couchbase/moss/cmd/mossScope
    ../moss/mossScope $ go build
    ../moss/mossSCope $ ./mossScope --help

Features
--------

* dump: This is to dump the contents of the store in JSON format
* import: This is to import docs from a JSON file into the store
* stats: This is to dump stats pertaining to the store in JSON format
* compact: This can be used to trigger compaction over the store

CLI
---

A preview of mossScope --help:

    Usage:
        mossScope [command] [sub-command] [flag] path_to_store

    Available Commands:
      dump              Dumps key/val data in the store
      import            Imports the docs from the JSON file into the store
      stats             Retrieves all the store related stats
      compact           Runs full compaction on the store specified

    Use "mossScope [command] --help" for more information about a command.

"dump"
------

    Usage:
        mossScope dump [sub-command] [flag] path_to_store

    Available Sub-commands:
      footer            Dumps the latest footer in the store
      key               Dumps the key and value of the specified key

    Available Flags:
      --keys-only       Dumps just the keys (without any values)

    Use "mossScope dump [sub-command] --help" for more information about a command.

footer:

    Usage:
        mossScope dump footer [flag] path_to_store

    Available Flags:
      --all             Dumps all the available footers from the store

key:

    Usage:
        mossScope dump key [flag] key_name path_to_store

    Available Flags:
      --all-versions    Dumps key and value of all persisted versions of the specified key

Examples:

    mossScope dump path_to_store --keys-only
    mossScope dump footers path_to_store --latest-only
    mossScope dump key key_name path_to_store

"import"
--------

    Usage:
        mossScope import <flag(s)> path_to_store

    Available Flags:
        --batchsize int Specifies the batch sizes for the set ops (default: all docs in one batch)
        --file string   Reads JSON content from file
        --json string   Reads JSON content from command-line
        --stdin         Reads JSON content from stdin (Enter to submit)

    Use "mossScope import --help" for more infomration about a command.

Examples:

    mossScope import path_to_store --file test.json --batchsize 100
    mossScope import path_to_store --json '[{"K":"key0","V":"val0"},{"K":"key1","V":"val1"}]'
    mossScope import path_to_store --stdin  // Program waits for user to submit JSON

"stats"
-------

    Usage:
        mossScope stats [sub-command] path_to_store

    Available Sub-commands:
      all               Dumps all available stats from the footers of the moss store
      frag              Dumps the fragmentation stats (to assist with manual compaction)
      hist              Scans the moss store and fetches histograms

    Use "mossScope stats [sub-command] --help" for more information about a command.

Examples:

    mossScope stats all path_to_store
    mossScope stats hist path_to_store

"compact"
---------

    Usage:
        mossScope compact path_to_store
