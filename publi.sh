#!/bin/bash

PATH_TO_GRADLE="~/.gradle/wrapper/dists/gradle-4.8-bin/divx0s2uj4thofgytb7gf9fsi/gradle-4.8/bin/gradle"
eval PATH_TO_GRADLE="$PATH_TO_GRADLE"

"$PATH_TO_GRADLE" uploadArchives
