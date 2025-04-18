#!/bin/bash

# Run the test and capture the output
test_output=$(./unit_test 2>&1)

# Temporary files
BROADCAST_TMP=$(mktemp)
HANDSHAKE_TMP=$(mktemp)
trap 'rm -f $BROADCAST_TMP $HANDSHAKE_TMP' EXIT

# Function to extract and normalize join pairs
extract_join_pairs() {
    local test_name="$1"
    # Extract test section and join pairs
    echo "$test_output" | awk "/\[ RUN      \] .*$test_name/,/\[       OK \] .*$test_name/" |
    grep -E '\[debug\]' | 
    sed -E 's/.*\[debug\] (.*)/\1/' |
    sort
}

# Extract pairs for both joiners
extract_join_pairs "WindowTest.BroadcastJoinerBasic" > "$BROADCAST_TMP"
extract_join_pairs "WindowTest.HandshakeJoiner" > "$HANDSHAKE_TMP"

# Check for duplicates in HandshakeJoiner
handshake_dupes=$(sort "$HANDSHAKE_TMP" | uniq -d)
if [[ -n "$handshake_dupes" ]]; then
    echo "Duplicate entries found in HandshakeJoiner:"
    echo "$handshake_dupes"
    exit 1
fi

# Compare the unique sets
missing_in_handshake=$(comm -23 "$BROADCAST_TMP" "$HANDSHAKE_TMP")
extra_in_handshake=$(comm -13 "$BROADCAST_TMP" "$HANDSHAKE_TMP")

# Report results
if [[ -z "$missing_in_handshake" && -z "$extra_in_handshake" ]]; then
    echo "Success: HandshakeJoiner matches BroadcastJoiner exactly"
    exit 0
else
    [[ -n "$missing_in_handshake" ]] && echo "Missing in HandshakeJoiner:" && echo "$missing_in_handshake"
    [[ -n "$extra_in_handshake" ]] && echo "Extra in HandshakeJoiner:" && echo "$extra_in_handshake"
    exit 1
fi
