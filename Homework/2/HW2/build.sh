#!/usr/bin/env bash

# Remove all files and directories in the 'bin' folder except for 'knownhosts.json'
find bin -mindepth 1 -not -name 'knownhosts.json' -exec rm -rf {} +

# Create the 'bin' directory and copy files into it
mkdir -p bin
cp -R src/* bin/
cp run.sh bin/

echo "Done!"

exit 0
