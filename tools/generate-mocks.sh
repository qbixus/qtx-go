#!/usr/bin/env bash
set -eu -o pipefail

# move to the project root
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

MOCKERY_DIST=https://github.com/vektra/mockery/releases/download/v3.5.5/mockery_3.5.5_Linux_x86_64.tar.gz
MOCKERY=tools/.bin/mockery/mockery

if [[ ! -f $MOCKERY ]]; then
  (DST=$(dirname "$MOCKERY"); mkdir -p "$DST"; curl -L "$MOCKERY_DIST" | tar -xz --one-top-level="$DST")
fi

$MOCKERY
