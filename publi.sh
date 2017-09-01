#!/bin/bash
set -x # echo on

# The first time pushd dir is called, pushd pushes the current directory onto the stack, then cds to dir and pushes it onto the stack.
pushd sqlite-utils

# Subsequent calls to pushd dir cd to dir and push dir only onto the stack.

~/.gradle/wrapper/dists/gradle-3.5-rc-2-bin/7ktl4k9rdug30mawecgppf5ms/gradle-3.5-rc-2/bin/gradle uploadArchives

# popd removes the top directory off the stack, revealing a new top. Then it cds to the new top directory.
popd
