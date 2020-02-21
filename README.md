# ZFS Utilities

A collection of scripts for working with ZFS Snapshots. Most assume ZFS as
hosted by FreeNAS (snapshot naming conventions, API support), but should be
useable on any ZFS system.

## ZFS Rollup
Similar in behavior to Apple's TimeMachine, the default
behavior is to keep hourly snapshots for the last day, daily for the last week,
and weekly thereafter. Extending beyond TimeMachine, the argument syntax allows
customizing the number of snapshots kept at each interval, as well as defining
additional buckets of arbitrary interval lengths.

## ClearEmpty
The goal here is to remove any snapshots that are of
zero size, meaning the snapshot holds no unique changes. If the blocks that
have changed in a snapshot are referenced by another snapshot, it will report
zero size. If a dataset contains multiple snapshots with no unique blocks,
the snapshots can be pruned. This will not free any blocks, but will declutter
the list of snapshots.

NOTE: It is possible for snapshots to report zero unique blocks if the changed
blocks are referenced by multiple snapshots. It is therefore important to only
remove one snapshot at a time from a given dataset and then scan for additional
empty snapshots.

## Snap Strip
By default, destroy all snapshots with an 'auto' prefix, except for the most
recent snapshot. Options are available for changing the prefix, dryrun, and
verbose output.

NOTE: The script currently never actually destroys any snapshots. If the
destroy commands look acceptable, you can pipe them to another shell to perform
the actions. This works with dryrun and verbose modes as well.

IE. `snap-strip.py tank tank/dataset | bash`

## TM Snap
See also README-tmsnap.md

This is a script to be run on a macOS machine. It will scan the system.log and 
act on messages from "backupd" to create a new snapshot on a FreeNAS machine 
that is providing the TimeMachine target.

It is not uncommon for TimeMachine to corrupt the TimeMachine volume
sparsebundle, especially when connected over a wireless network. Corruption
can also result from shutting down or sleeping the Mac. In most cases these
will present when TimeMachine checks the integrity of the volume and reports
that it needs to start over.

These issues can sometimes be resolved by stopping any current backups,
disabling the TimeMachine service to prevent another backup from starting, and
rolling back the TimeMachine dataset to an earlier snapshot before the issues
started. Rolling back like this isn't guaranteed to resolve issues, and may
lead to missing backups if the macOS machine does not detect that it needs 
perform a full scan prior to backing up. In the worst case, the dataset can be
rolled back to an empty state if such a snapshot exists.

NOTE: When rolling back the TimeMachine dataset to an empty state, it's likely
that TimeMachine will report that the volume identity has changed.

WARNING: It is also possible for FreeNAS errors to interfer with the AFP service
in a way that mimics sparsebundle errors. Before rolling back any datasets, try
restarting the AFP service on FreeNAS. Also look for any processes that are
in states of "Uninteruptible Sleep" and "pages locked into memory" (D,L
respectively). This may indicate a deadlock that will not be recovered by
restarting the AFP service and will require rebooting the FreeNAS machine.
