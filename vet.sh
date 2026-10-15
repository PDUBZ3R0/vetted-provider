#!/bin/bash
CHAIN="$1"
shift

if [ -z "$CHAIN" ]; then
	echo "usage: yarn vet <chain_id>"
fi

mkdir -p log
node --experimental-strip-types --disable-warning=ExperimentalWarning src/cli.ts vet $CHAIN $@ 2>&1 | tee log/$CHAIN.log