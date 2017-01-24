CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT(mossScope)

# This file is used by the build process for the complete Couchbase Server

GoInstall(TARGET mossScope
          PACKAGE github.com/couchbase/moss/cmd/mossScope
          GOPATH "${CMAKE_SOURCE_DIR}/goproj" "${GODEPSDIR}"
          INSTALL_PATH bin OUTPUT mossScope
          GOVERSION 1.7)
