#!/usr/bin/env python

# clearempty.py - Koen Vermeer <k.vermeer@eyehospital.nl>
# Inspired by rollup.py by Arno Hautala <arno@alum.wpi.edu>
#   This work is licensed under a Creative Commons Attribution-ShareAlike 3.0 Unported License.
#   (CC BY-SA-3.0) http://creativecommons.org/licenses/by-sa/3.0/

# This script removes empty snapshots, based on their 'used' property.
# Note that one snapshot's 'used' value may change when another snapshot is
# destroyed. This script iteratively destroys the oldest empty snapshot. It
# does not remove the latest snapshot of each dataset, manual snapshots and
# any snapshot with the 'freenas:type' property set.

import subprocess
import argparse
import sys
from collections import defaultdict

parser = argparse.ArgumentParser(description='Removes empty auto snapshots.')
parser.add_argument('datasets', nargs='+', help='the root dataset(s) from which to remove snapshots')
parser.add_argument('--test', '-t', action="store_true", default=False, help='only display the snapshots that would be deleted, without actually deleting them. Note that due to dependencies between snapshots, this may not match what would really happen.')
parser.add_argument('--recursive', '-r', action="store_true", default=False, help='recursively removes snapshots from nested datasets')

args = parser.parse_args()

deleted = defaultdict(lambda : defaultdict(lambda : defaultdict(int)))

snapshot_was_deleted = True

while snapshot_was_deleted:
    snapshot_was_deleted = False
    snapshots = defaultdict(lambda : defaultdict(lambda : defaultdict(int)))

    # Get properties of all snapshots of the selected datasets
    for dataset in args.datasets:
        zfs_snapshots = subprocess.check_output(["zfs", "get", "-Hrpo name,property,value", "type,creation,used,freenas:state", dataset])

        for snapshot in zfs_snapshots.splitlines():
            name,property,value = snapshot.split('\t',3)

            # enforce that this is a snapshot (presence of '@')
            if "@" not in name:
                continue

            # if the rollup isn't recursive, skip any snapshots from child datasets
            if not args.recursive and not name.startswith(dataset+"@"):
                continue
            
            dataset,snapshot = name.split('@',2)
            
            snapshots[dataset][snapshot][property] = value

    # Ignore non-snapshots and not-auto-snapshots
    # Remove already destroyed snapshots
    for dataset in snapshots.keys():
        for snapshot in snapshots[dataset].keys():
            if not snapshot.startswith("auto-") \
                or snapshots[dataset][snapshot]['type'] != "snapshot" \
                or snapshots[dataset][snapshot]['used'] != '0' \
                or snapshot in deleted[dataset].keys():
                del snapshots[dataset][snapshot]

        snapshot = max(snapshots[dataset], key=lambda snapshot: snapshots[dataset][snapshot]['creation'])
        del snapshots[dataset][snapshot]

        # Stop if no snapshots are in the list
        if not snapshots[dataset]:
            del snapshots[dataset]
            continue

        snapshot = max(snapshots[dataset], key=lambda snapshot: snapshots[dataset][snapshot]['creation'])
        if not args.test:
            # destroy the snapshot
            subprocess.call(["zfs", "destroy", dataset+"@"+snapshot])

        deleted[dataset][snapshot] = snapshots[dataset][snapshot]
        snapshot_was_deleted = True

for dataset in sorted(deleted.keys()):
    print dataset
    for snapshot in sorted(deleted[dataset].keys()):
        print "\t", snapshot, deleted[dataset][snapshot]['used']
