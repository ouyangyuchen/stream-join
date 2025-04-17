#!/bin/bash

# Run the test and capture the output
test_output=$(./unit_test 2>&1)

# Function to extract the sum of join counts for a specific test
extract_join_sum() {
    local test_name="$1"
    local join_pattern="$2"
    
    # Extract the relevant section of the test output
    local section=$(echo "$test_output" | awk "/\[ RUN      \] .*$test_name/,/\[       OK \] .*$test_name/")
    
    # Extract and sum all join counts
    echo "$section" | grep -E "$join_pattern" | awk '{sum += $NF} END {print sum}'
}

# Get sums for both tests
broadcast_sum=$(extract_join_sum "WindowTest.BroadcastJoinerBasic" "Join count: [0-9]+")
handshake_sum=$(extract_join_sum "WindowTest.HandshakeJoiner" "join count: [0-9]+")

# Check if we got valid numbers
if ! [[ "$broadcast_sum" =~ ^[0-9]+$ ]] || ! [[ "$handshake_sum" =~ ^[0-9]+$ ]]; then
    echo "Error: Failed to extract valid join counts"
    exit 1
fi

# Compare the sums
if [ "$broadcast_sum" -eq "$handshake_sum" ]; then
    echo "Success: Both join sums equal $broadcast_sum"
    exit 0
else
    echo "Mismatch: BroadcastJoinerBasic=$broadcast_sum vs HandshakeJoiner=$handshake_sum"
    exit 1
fi
