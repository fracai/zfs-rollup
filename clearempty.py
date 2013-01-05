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

# TODO: 
#   Fix recursive option.

import subprocess
import argparse
#import sys
from collections import defaultdict

parser = argparse.ArgumentParser(description='Removes empty auto snapshots.')
parser.add_argument('datasets', nargs='+', help='the root dataset(s) from which to remove snapshots')
parser.add_argument('--test', '-t', action="store_true", default=False, help='only display the snapshots that would be deleted, without actually deleting them. Note that due to dependencies between snapshots, this may not match what would really happen.')
#parser.add_argument('--verbose', '-v', action="store_true", default=False, help='display verbose information about which snapshots are kept, pruned, and why')
parser.add_argument('--recursive', '-r', action="store_true", default=False, help='recursively removes snapshots from nested datasets')

args = parser.parse_args()

#if args.test:
#    args.verbose = True

zfs_arguments = "-Hrpo"

deleted_snapshots = []

snapshotdeleted = True

while snapshotdeleted:
   snapshotdeleted = False
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
      snapshots_curds = {name:snapshots[name] for name in snapshots if snapshots[name]['dataset'] == dataset}
      # Remove newest snapshot from candidate list
      newest_name = max(snapshots_curds, key=lambda name: snapshots_curds[name]['sstime'])
      del snapshots_curds[newest_name]

      # Ignore zero length or special freenas-flagged snapshots
      for name in snapshots_curds.keys():
         if not snapshots_curds[name]['used'] == '0' \
            or not snapshots_curds[name]['freenas:state'] == '-':
            del snapshots_curds[name]

      # Stop processing this dataset if no snapshots are in the list
      if not snapshots_curds:
         continue

      nametodelete = min(snapshots_curds,key=lambda name: snapshots_curds[name]['sstime'])
      print "Destroying snapshot", nametodelete, "..."

      # Sanity checks
      if not "@auto-" in nametodelete:
         break
      if not snapshots_curds[nametodelete]['used'] == '0':
         break

      if args.test:
         deleted_snapshots.append(nametodelete)
      else:
         subprocess.call(["zfs", "destroy", nametodelete])
      snapshotdeleted = True

