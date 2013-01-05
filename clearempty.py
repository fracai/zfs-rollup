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
from collections import defaultdict

parser = argparse.ArgumentParser(description='Removes empty auto snapshots.')
parser.add_argument('datasets', nargs='+', help='the root dataset(s) from which to remove snapshots')
parser.add_argument('--test', '-t', action="store_true", default=False, help='only display the snapshots that would be deleted, without actually deleting them. Note that due to dependencies between snapshots, this may not match what would really happen.')
parser.add_argument('--recursive', '-r', action="store_true", default=False, help='recursively removes snapshots from nested datasets')

args = parser.parse_args()

deleted_snapshots = []

snapshot_was_deleted = True

while snapshot_was_deleted:
    snapshot_was_deleted = False
    snapshots = defaultdict(lambda : defaultdict(int))

    # Get properties of all snapshots of the selected datasets
    for dataset in args.datasets:
        zfs_snapshots = subprocess.check_output(["zfs", "get", "-Hrpo name,property,value", "type,used,freenas:state", dataset])
        for snapshot in zfs_snapshots.splitlines():
            name,property,value = snapshot.split('\t',3)
            if not args.recursive and not name.startswith(dataset+"@"):
                continue
            snapshots[name][property] = value

    # Ignore non-snapshots and not-auto-snapshots
    for name in snapshots.keys():
        if not snapshots[name]['type'] == 'snapshot' \
            or not "@auto-" in name:
            del snapshots[name]

    # Remove already destroyed snapshots
    for name in deleted_snapshots:
        if name in snapshots:
            del snapshots[name]

    # Stop if no snapshots are in the list
    if not snapshots:
        break

    # Get snapshot dates
    for name in snapshots:
        dataset,sstime = name.split('@auto-')
        snapshots[name]['dataset'] = dataset
        snapshots[name]['sstime'] = sstime

    unique_datasets = set([snapshots[name]['dataset'] for name in snapshots])

    for dataset in unique_datasets:
        dataset_snapshot = {name:snapshots[name] for name in snapshots if snapshots[name]['dataset'] == dataset}
        # Remove newest snapshot from candidate list
        newest_name = max(dataset_snapshot, key=lambda name: dataset_snapshot[name]['sstime'])
        del dataset_snapshot[newest_name]

        # Ignore zero length or special freenas-flagged snapshots
        for name in dataset_snapshot.keys():
            if not dataset_snapshot[name]['used'] == '0' \
                or not dataset_snapshot[name]['freenas:state'] == '-':
                del dataset_snapshot[name]

        # Stop processing this dataset if no snapshots are in the list
        if not dataset_snapshot:
            continue

        snapshot_to_delete = min(dataset_snapshot,key=lambda name: dataset_snapshot[name]['sstime'])
        print "Destroying snapshot", snapshot_to_delete, "..."

        # Sanity checks
        if not "@auto-" in snapshot_to_delete:
            break
        if not dataset_snapshot[snapshot_to_delete]['used'] == '0':
            break

        if args.test:
            deleted_snapshots.append(snapshot_to_delete)
        else:
            subprocess.call(["zfs", "destroy", snapshot_to_delete])
        snapshot_was_deleted = True
