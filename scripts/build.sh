#!/bin/bash
# Build the GSD daemon

GSD_HOME=$(dirname "$(dirname "$(readlink -f "$0")")")
cd "$GSD_HOME/daemon" || exit 1

echo "Building GSD daemon..."
cargo build --release

if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

echo "GSD daemon built successfully!"
echo "Binary location: $GSD_HOME/daemon/target/release/gsd-daemon"
