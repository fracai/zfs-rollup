TMSnap utilizes three components:
- tmsnap.py : The script that monitors system.log for TimeMachine activity and creates new ZFS snapshots on the FreeNAS host.
- tmsnap.json : A json configuration file that specifies the FreeNAS host, root password (required for accessing the FreeNAS API), and ZFS dataset. The FreeNAS username and TimeMachine log file locotaion are also listed, but these should not need to be modified.
- org.freenas.time-machine-snapshot.plist : A launchd plist for starting the TMSnap process and keeping it running.

TimeMachine is managed by the backupd process.
Status information about running backups are written to /var/log/system.log and prefixed with the local hostname, the date, and process identifier (backupd and PID).
Because system.log is owned by root and is not world readable, the tmsnap script needs to run as root as well.

The standard location for root owned launchd tasks is: /Library/LaunchDaemons
I have used the prefix 'fracai.zfs-rollup' for the launchd plist, but it can be customized as desired.
Be sure to change the value for the 'Label' key within the plist as well.

tmsnap.py uses a number of Python packages that may not be available with the default installation.
The easiest way to satisfy these is via a virtualenv.
However, the root environment will not contain this virtualenv in the PYTHONPATH.
That environment variable can be modified in the launchd plist, or the tmsnap.py modified to specify the #! as the python binary provided by the virtualenv.

tmsnap.json needs to be modified to specify the root password of the FreeNAS device that hosts your TimeMachine volume, the hostname of that FreeNAS device, and the dataset path that leads to the TimeMachine volume for the current Mac.
The configuration file also contains settings for the system.log location and the FreeNAS user, neither of which should need to be modified.

The Python script and configuration can be located anywhere as long as the launchd plist is modified accordingly.

With everything in place, the tmsnap process can be activiated via launchd with:
`launchctl load -w /Library/LaunchDaemons/fracai.zfs-rollup.time-machine-snapshot.plist`
