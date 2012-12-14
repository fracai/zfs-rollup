#!/usr/bin/env python

# rollup.py - Arno Hautala <arno@alum.wpi.edu>
#   This work is licensed under a Creative Commons Attribution-ShareAlike 3.0 Unported License.
#   (CC BY-SA-3.0) http://creativecommons.org/licenses/by-sa/3.0/

# For the latest version, visit:
#   https://github.com/fracai/zfs-rollup
#   https://bitbucket.org/fracai/zfs-rollup

# A snapshot pruning script, similar in behavior to Apple's TimeMachine
# Keep hourly snapshots for the last day, daily for the last week, and weekly thereafter.
# Also prune empty snapshots (0 bytes used) that aren't the most recent for any dataset

# TODO: 
#   arguments for configuring snapshots to keep (# daily, # weekly, etc)
#   define arbitrary intervals (daily = 86400, quarter-hour = 900, etc)
#   configuration file support
#   rollup based on local time, not UTC
#     requires pytz, or manually determining and converting time offsets
#   arguments to select intervals, modify existing intervals, add new intervals

# TEST:
#   arbitrary intervals !!!! current code untested

import datetime
import calendar
import time
import subprocess
import argparse
import sys
from collections import defaultdict

intervals = {}
intervals['hourly']  = { 'max' : 24, 'abbreviation':'h', 'reference' : '%Y-%m-%d %H' }
intervals['daily']   = { 'max' :  7, 'abbreviation':'d', 'reference' : '%Y-%m-%d' }
intervals['weekly']  = { 'max' :  0, 'abbreviation':'w', 'reference' : '%Y-%W' }
intervals['monthly'] = { 'max' : 12, 'abbreviation':'m', 'reference' : '%Y-%m' }
intervals['yearly']  = { 'max' : 10, 'abbreviation':'y', 'reference' : '%Y' }

used_intervals = [ 'hourly', 'daily', 'weekly' ]

parser = argparse.ArgumentParser(description='Prune excess snapshots, keeping hourly for the last day, daily for the last week, and weekly thereafter.')
parser.add_argument('datasets', nargs='+', help='the root dataset(s) from which to prune snapshots')
parser.add_argument('--test', '-t', action="store_true", default=False, help='only display the snapshots that would be deleted, without actually deleting them')
parser.add_argument('--verbose', '-v', action="store_true", default=False, help='display verbose information about which snapshots are kept, pruned, and why')
parser.add_argument('--empty', '-z', action="store_true", default=False, help='prune empty snashots (will still retain the most recent even if empty)')
parser.add_argument('--recursive', '-r', action="store_true", default=False, help='recursively pruning snapshots from nested datasets')

args = parser.parse_args()

if args.test:
    args.verbose = True

now = datetime.datetime.utcnow()

one_hour = datetime.timedelta(hours = 1)
one_day  = datetime.timedelta(days  = 1)
one_week = datetime.timedelta(weeks = 1)

snapshots = defaultdict(lambda : defaultdict(lambda : defaultdict(int)))

for dataset in args.datasets:
    zfs_snapshots = subprocess.check_output(["zfs", "get", "-Hrpo", "name,property,value", "creation,used", dataset])

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

for dataset in sorted(snapshots.keys()):
    print dataset
    
    sorted_snapshots = sorted(snapshots[dataset].keys())
    most_recent = sorted_snapshots[-1]
    
    rollup_intervals = defaultdict(lambda : defaultdict(int))
    
    hours = []
    days = {}
    weeks = {}
    
    for snapshot in sorted_snapshots:
        # enforce that this is an automated snapshot (presence of 'auto')
        if "auto" not in snapshot:
            print "\tignoring:\t", "@"+snapshot
            #continue

        prune = True

        epoch = snapshots[dataset][snapshot]['creation']
        
        for interval in used_intervals:
            reference = time.strftime(intervals[interval]['reference'], time.gmtime(float(epoch)))
            
            if reference not in rollup_intervals[interval]:
                if intervals[interval]['max'] != 0 and len(rollup_intervals[interval]) > intervals[interval]['max']:
                    rollup_intervals[interval].pop(sorted(rollup_intervals[interval].keys())[0])
                rollup_intervals[interval][reference] = epoch
        
        
    for snapshot in sorted_snapshots:
        # enforce that this is an automated snapshot (presence of 'auto')
        if "auto" not in snapshot:
            print "\tignoring:\t", "@"+snapshot
            #continue
        
        prune = True
        
        epoch = snapshots[dataset][snapshot]['creation']
        
        for interval in used_intervals:
            reference = time.strftime(intervals[interval]['reference'], time.gmtime(float(epoch)))
            if reference in rollup_intervals[interval]:
                prune = False

        if prune or args.verbose:
            print "\t","pruning\t" if prune else " \t", "@"+snapshot, 
            if args.verbose:
                for interval in used_intervals:
                    reference = time.strftime(intervals[interval]['reference'], time.gmtime(float(epoch)))
                    if rollup_intervals[interval][reference] == epoch:
                        print intervals[interval]['abbreviation'],
                    else:
                        print '-',
                print snapshots[dataset][snapshot]['used']
            else:
                print
        
        if prune:
            if (not args.test):
                # destroy the snapshot
                subprocess.call(["zfs", "destroy", dataset+"@"+snapshot])
