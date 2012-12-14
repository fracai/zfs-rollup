#!/usr/bin/env python

# rollup.py - Arno Hautala <arno@alum.wpi.edu>
#   This work is licensed under a Creative Commons Attribution-ShareAlike 3.0 Unported License.
#   (CC BY-SA-3.0) http://creativecommons.org/licenses/by-sa/3.0/

# A snapshot pruning script, similar in behavior to Apple's TimeMachine
# Keep hourly snapshots for the last day, daily for the last week, and weekly thereafter.
# Also prune empty snapshots (0 bytes used) that aren't the most recent for any dataset

# TODO: 
#   arguments for configuring snapshots to keep (# daily, # weekly, etc)
#   define arbitrary intervals (daily = 86400, quarter-hour = 900, etc)
#   configuration file support
#   different intervals for different dataset roots (? can also just run multiple rollups)
#   rollup based on local time, not UTC
#     requires pytz, or manually determining and converting time offsets

# TEST:
#   recursive argument
#   empty argument

import datetime
import calendar
import subprocess
import argparse
import sys
from collections import defaultdict

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
    zfs_arguments = "-Hrpo" if args.recursive else "-Hpo"
    zfs_snapshots = subprocess.check_output(["zfs", "get", zfs_arguments, "name,property,value", "creation,used", dataset])

    for snapshot in zfs_snapshots.splitlines():
        name,property,value = snapshot.split('\t',3)

        # enforce that this is a snapshot (presence of '@')
        if "@" not in name:
            continue
        
        dataset,snapshot = name.split('@',2)
        snapshots[dataset][snapshot][property] = value

for dataset in sorted(snapshots.keys()):
    print dataset
    
    sorted_snapshots = sorted(snapshots[dataset].keys())
    most_recent = sorted_snapshots[-1]
    
    hours = []
    days = {}
    weeks = {}
    
    for snapshot in sorted_snapshots:
        # enforce that this is an automated snapshot (presence of 'auto')
        if "auto" not in snapshot:
            print "\tignoring:\t", "@"+snapshot
            continue

        prune = True

        epoch = snapshots[dataset][snapshot]['creation']
        
        snaptime = datetime.datetime.utcfromtimestamp(float(epoch))
        snapdate = snaptime.date()
        snapweek = str(snapdate.isocalendar()[0])+"-"+str(snapdate.isocalendar()[1])

        if (not args.empty or snapshots[dataset][snapshot]['used'] != '0') or snapshot == most_recent:
            if snaptime >= now - one_day:
                hours.append(epoch)
                prune = False
            
            if snaptime >= now - one_week and snapdate not in days:
                days[snapdate] = epoch
                prune = False
            
            if snapweek not in weeks:
                weeks[snapweek] = epoch
                prune = False
        
        if prune or args.verbose:
            print "\t","pruning\t" if prune else " \t", "@"+snapshot, 
            if args.verbose:
                print 'h' if epoch in hours else '-', 
                print 'd' if snapdate in days and days[snapdate] == epoch else '-', 
                print 'w' if snapweek in weeks and weeks[snapweek] == epoch else '-',
                print snapshots[dataset][snapshot]['used']
            else:
                print
        
        if prune:
            if (not args.test):
                # destroy the snapshot
                subprocess.call(["zfs", "destroy", dataset+"@"+snapshot])
