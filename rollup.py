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
#   configuration file support (?)
#   rollup based on local time, not UTC
#     requires pytz, or manually determining and converting time offsets
#   improve documentation

# TEST:
#   arbitrary intervals (looks good so far)

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

modifiers = {
    'h' : 60,
    'd' : 60*24,
    'w' : 60*24*7,
    'm' : 60*24*28,
    'y' : 60*24*365,
}

used_intervals = {
    'hourly': intervals['hourly'],
    'daily' : intervals['daily'],
    'weekly': intervals['weekly']
}

parser = argparse.ArgumentParser(description='Prune excess snapshots, keeping hourly for the last day, daily for the last week, and weekly thereafter.')
parser.add_argument('datasets', nargs='+', help='the root dataset(s) from which to prune snapshots')
parser.add_argument('--test', '-t', action="store_true", default=False, help='only display the snapshots that would be deleted, without actually deleting them')
parser.add_argument('--verbose', '-v', action="store_true", default=False, help='display verbose information about which snapshots are kept, pruned, and why')
parser.add_argument('--empty', '-z', action="store_true", default=False, help='prune empty snashots (will still retain the most recent even if empty)')
parser.add_argument('--recursive', '-r', action="store_true", default=False, help='recursively pruning snapshots from nested datasets')
parser.add_argument("--intervals", "-i", 
    help="modify existing and define new snapshot intervals. either name existing intervals ("+", ".join(intervals.keys())+"), "+
    "modify the number of those to store (hourly:12), or define new intervals according to interval:count (2h:12). "+
    "Multiple intervals may be specified if comma seperated (hourly,daily:30,2h12)."
)

args = parser.parse_args()

if args.test:
    args.verbose = True

if args.intervals:
    used_intervals = {}
    
    for interval in args.intervals.split(','):
        if interval.count(':') == 1:
            period,count = interval.split(':')
            
            if period in intervals:
                used_intervals[period] = intervals[period]
                used_intervals[period]['max'] = count
                
            else:
                if period[-1] in modifiers:
                    used_intervals[interval] = { 'max' : count, 'interval' : int(period[:-1]) * modifiers[period[-1]] }
                    
                elif int(period):
                    used_intervals[interval] = { 'max' : count, 'interval' : int(period) }
                    
        elif interval.count(':') == 0 and interval in intervals:
            used_intervals[interval] = intervals[interval]

for interval in used_intervals:
    if 'abbreviation' not in used_intervals[interval]:
        used_intervals[interval]['abbreviation'] = interval

print used_intervals

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
    
    for snapshot in sorted_snapshots:
        # enforce that this is an automated snapshot (presence of 'auto')
        if "auto" not in snapshot:
            print "\tignoring:\t", "@"+snapshot
            continue

        prune = True

        epoch = snapshots[dataset][snapshot]['creation']
        
        for interval in used_intervals.keys():
            if 'reference' in used_intervals[interval]:
                reference = time.strftime(used_intervals[interval]['reference'], time.gmtime(float(epoch)))
                
                if reference not in rollup_intervals[interval]:
                    if used_intervals[interval]['max'] != 0 and len(rollup_intervals[interval]) > int(used_intervals[interval]['max']):
                        rollup_intervals[interval].pop(sorted(rollup_intervals[interval].keys())[0])
                    rollup_intervals[interval][reference] = epoch
            
            elif 'interval' in used_intervals[interval]:
                if used_intervals[interval]['max'] != 0 and len(rollup_intervals[interval]) > int(used_intervals[interval]['max']):
                    rollup_intervals[interval].pop(sorted(rollup_intervals[interval].keys())[0])
                
                if (not rollup_intervals[interval]) or int(sorted(rollup_intervals[interval].keys())[-1]) + (used_intervals[interval]['interval']*60*.9) < int(epoch):
                    rollup_intervals[interval][epoch] = epoch
        
    for snapshot in sorted_snapshots:
        # enforce that this is an automated snapshot (presence of 'auto')
        if "auto" not in snapshot:
            continue
        
        prune = True
        
        epoch = snapshots[dataset][snapshot]['creation']
        
        for interval in used_intervals.keys():
            if 'reference' in used_intervals[interval]:
                reference = time.strftime(used_intervals[interval]['reference'], time.gmtime(float(epoch)))
                if reference in rollup_intervals[interval] and rollup_intervals[interval][reference] == epoch:
                    prune = False
                    
            elif 'interval' in used_intervals[interval]:
                if epoch in rollup_intervals[interval]:
                    prune = False

        if prune or args.verbose:
            print "\t","pruning\t" if prune else " \t", "@"+snapshot, 
            if args.verbose:
                for interval in used_intervals.keys():
                    if 'reference' in used_intervals[interval]:
                        reference = time.strftime(used_intervals[interval]['reference'], time.gmtime(float(epoch)))
                        if reference in rollup_intervals[interval] and rollup_intervals[interval][reference] == epoch:
                            print used_intervals[interval]['abbreviation'],
                        else:
                            print '-',
                    if 'interval' in used_intervals[interval]:
                        if epoch in rollup_intervals[interval]:
                            print used_intervals[interval]['abbreviation'],
                        else:
                            print '-',
                print snapshots[dataset][snapshot]['used']
            else:
                print
        
        if prune:
            if (not args.test):
                # destroy the snapshot
                subprocess.call(["zfs", "destroy", dataset+"@"+snapshot])
