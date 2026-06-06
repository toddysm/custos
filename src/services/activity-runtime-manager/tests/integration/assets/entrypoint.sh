#!/bin/sh
# Contract-aware e2e activity for the ARM I/O bridge integration suite.
#
# Reads the optional /custos/in/inputs.json ARM streams in through the input
# bridge, writes a valid /custos/out/outputs.json (Activity Contract v1) plus
# one artifact under /custos/out/artifacts/, then exits 0. Only /custos/out is
# writable (the hardened pod runs read-only-root, runAsNonRoot), so the script
# touches nothing else.
set -eu

out=/custos/out
mkdir -p "$out/artifacts"

# Echo a value back through the bridge so the test can assert a true round-trip.
# inputs.json is optional and free-form; pull a "name" string if one is present,
# tolerating either compact or spaced JSON, else fall back to a default.
name=world
if [ -f /custos/in/inputs.json ]; then
    parsed=$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' /custos/in/inputs.json)
    if [ -n "$parsed" ]; then
        name=$parsed
    fi
fi

printf 'hello, %s\n' "$name" > "$out/artifacts/greeting.txt"
printf '{"status":"success","outputs":{"greeting":"hello, %s"}}\n' "$name" > "$out/outputs.json"
