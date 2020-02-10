#!/opt/local/bin/python

import json
import requests
from multitail import multitail
import re, os
import argparse
import logging
from functools import reduce

TM_PREFIX = re.compile("com\.apple\.backupd\[\d+\]:[ \t]*")
TM_STARTED = re.compile("Starting (?:automatic|manual) backup")
TM_COMPLETE = re.compile("Created new backup: (\d{4}-\d{2}-\d{2}-\d{6})")
TM_EJECTED = "Ejected Time Machine network volume."
TM_ERROR = ("Backup failed", "Backup canceled", "Stopping backup")

def apicall(host, path, authdata, payload):
    url = 'http://'+host+'/'+path
    return requests.post(
        url,
        auth=authdata,
        headers={'Content-Type': 'application/json'},
        verify=False,
        data=json.dumps(payload),
        timeout=30
    )

def snapshot(host, auth, payload):
    return apicall(host, '/api/v1.0/storage/snapshot/', auth, payload)

parser = argparse.ArgumentParser(description='Monitor the system log for Time Machine events and create snapshots after successful backups')
parser.add_argument('--config', action='store', help='the configuration file')
parser.add_argument('--test', action='store_true', help="don't actually create a new snapshot, just print what would be done")
args = parser.parse_args()

# default logger to console
logging.basicConfig(
    format='%(asctime)s %(filename)s [%(process)d]: %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.debug("loading configuration")

if not args.config:
    logging.error("configuration file not specified")
    parser.print_usage()
    sys.exit(1)
if not os.path.isfile(args.config):
    logging.error("configuration file does not exist: '"+args.config+"'")
    parser.print_usage()
    sys.exit(2)

logging.getLogger('').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.getLogger('multitail').setLevel(logging.ERROR)

json_data=open(args.config)
config = json.load(json_data)
json_data.close()

authorization = (config['username'], config['password'])

latest_completed_backup = None

logging.debug("starting up")

for fn, line in multitail([config['log_path']]):
    match = TM_PREFIX.search(line)
    if not match:
        continue
    if TM_STARTED.search(line):
        latest_completed_backup = None
        logging.info("started new backup")
        continue
    if reduce((lambda x,y: x or y), [x in line for x in TM_ERROR]):
        logging.error("backup error: "+line[match.end():])
        latest_completed_backup = None
        continue
    match = TM_COMPLETE.search(line)
    if match:
        latest_completed_backup = match.group(1)
        continue
    if TM_EJECTED in line and latest_completed_backup:
        shot = {}
        shot['dataset'] = config['dataset']
        shot['name'] = 'tm-'+latest_completed_backup
        logging.info("snapshotting: '"+latest_completed_backup+"'")
        if args.test:
            logging.warn("skipping snapshot during test")
            continue
        result = snapshot(config['host'], authorization, shot)
        if 201 == result.status_code:
            logging.info("snapshot successful: '"+latest_completed_backup+"'")
        else:
            logging.error("snapshot failed, response code: '"+str(result.status_code)+"'")
