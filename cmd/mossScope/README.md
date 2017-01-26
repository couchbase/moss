mossScope (v0.1.0)
==================

mossScope is a diagnostic command line tool, that is designed to assist
in browsing contents of, acquiring stats from, and otherwise diagnosing
moss store.

Standalone: How to use
----------------------

    $ go get github.com/couchbase/moss
    $ cd $GOPATH/src/github.com/couchbase/moss/cmd/mossScope
    $ go build
    $ ./mossScope --help

Usage:
------

    mossScope <command> [sub-command] [flags] <store_path(s)>

The store_path(s) is one or more directories where moss files reside.

The command is requred. Available commands:

    dump              Dumps key/val data from the store
    import            Imports docs into the store
    stats             Emits store related stats
    version           Emits the current version of mossScope

Use "mossScope <command> --help" for more detailed information about
any command.

"dump"
------

    mossScope dump [sub-command] [flags] <store_path(s)>

    Available sub-commands:

        footer            Dumps the latest footer in the store
        key               Dumps the key and value of the specified key

    Available flags:

        --keys-only       Dumps just the keys (without any values)

footer:

    mossScope dump footer [flags] <store_path(s)>

    Available flags:

        --all             Dumps all the available footers from the store

key:

    mossScope dump key [flags] <key> <store_path(s)>

    Available flags:

        --all-versions    Dumps key and value of all persisted versions of the specified key

Examples:

    mossScope dump path/to/myStore --keys-only
    mossScope dump footer path/to/myStore
    mossScope dump key myKey path/to/myStore

"import"
--------

    mossScope import [flags] <store_path(s)>

    Available flags:

        --batchsize int Specifies the batch sizes for the set ops (default: all docs in one batch)
        --file <file_path> Reads JSON content from <file_path>
        --json <json>      Reads JSON content from command-line
        --stdin            Reads JSON content from stdin (Enter to submit)

Examples:

    mossScope import path/to/myStore --file test.json --batchsize 100
    mossScope import path/to/myStore --json '[{"k":"key0","v":"val0"},{"k":"key1","v":"val1"}]'
    mossScope import path/to/myStore --stdin // Program waits for user to submit JSON

"stats"
-------

    mossScope stats <sub-command> <store_path(s)>

    Available sub-commands:

        footer            Dumps aggregated stats from the latest footer of the moss store
        fragmentation     Dumps the fragmentation stats (to assist with manual compaction)

    Available flags:

        --json            Emits output in JSON

footer:

    mossScope stats footer [flags] <store_path(s)>

    Available flags:

        --all             Fetches stats from all available footers (Footer_1 is latest)

fragmentation:

    mossScope stats frag [flags] <store_path(s)>

Examples:

    mossScope stats footer path/to/myStore --all --json
    mossScope stats fragmentation path/to/myStore
