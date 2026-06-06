#!/bin/sh
# Contract-aware e2e activity for the ARM I/O bridge integration suite.
#
# Reads the optional /custos/in/inputs.json ARM streams in through the input
# bridge, writes a valid /custos/out/outputs.json (Activity Contract v1) plus
# one artifact under /custos/out/artifacts/, then exits 0. Only /custos/out is
# writable (the hardened pod runs read-only-root, runAsNonRoot), so the script
# touches nothing else.
#
# Three modes, selected by optional inputs.json fields so the same image serves
# every scenario without breaking the plain happy-path (ARM-IMPL-028):
#   * default                    -> outputs {"greeting": "hello, <name>"}
#   * "emitArtifactRef": true    -> producer: also declares greeting.txt as an
#                                   {"kind":"ArtifactRef","name":"greeting.txt"}
#                                   output so ARM uploads + rewrites it
#   * "consume": "<name>"        -> consumer: reads the upstream artifact ARM
#                                   materialized at /custos/in/artifacts/<name>
#                                   and echoes it as {"consumed": "<content>"}
set -eu

out=/custos/out
mkdir -p "$out/artifacts"

# Echo a value back through the bridge so the test can assert a true round-trip.
# inputs.json is optional and free-form; pull a "name" string if one is present,
# tolerating either compact or spaced JSON, else fall back to a default. The
# regex's [^"]* already excludes embedded quotes, and `head -n1` keeps only the
# first match so a multi-line / multi-key payload can't inject a newline into
# the JSON string value.
name=world
emit_ref=0
consume=
if [ -f /custos/in/inputs.json ]; then
    parsed=$(sed -n 's/.*"name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
        /custos/in/inputs.json | head -n1)
    if [ -n "$parsed" ]; then
        name=$parsed
    fi
    if grep -q '"emitArtifactRef"[[:space:]]*:[[:space:]]*true' /custos/in/inputs.json; then
        emit_ref=1
    fi
    consume=$(sed -n 's/.*"consume"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
        /custos/in/inputs.json | head -n1)
fi

printf 'hello, %s\n' "$name" > "$out/artifacts/greeting.txt"

if [ -n "$consume" ]; then
    # Consumer: ARM fetched the upstream artifact and staged it for us. Echo its
    # content back so the test can assert the downstream materialization worked.
    # $(...) trims the trailing newline the producer wrote.
    consumed=$(cat "/custos/in/artifacts/$consume")
    printf '{"status":"success","outputs":{"consumed":"%s"}}\n' "$consumed" > "$out/outputs.json"
elif [ "$emit_ref" = "1" ]; then
    # Producer: declare greeting.txt as an ArtifactRef output for ARM to upload.
    printf '{"status":"success","outputs":{"greeting":"hello, %s","greetingFile":{"kind":"ArtifactRef","name":"greeting.txt"}}}\n' \
        "$name" > "$out/outputs.json"
else
    printf '{"status":"success","outputs":{"greeting":"hello, %s"}}\n' "$name" > "$out/outputs.json"
fi

