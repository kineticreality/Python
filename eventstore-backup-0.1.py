# Eventstore Backup
# Utilizing ZFS Snapshot functionality to backup the Eventstore Data Volume.
# Author: Mike Reid 
# Version 0.1

#!/usr/bin/python

import os
import sys
import argparse
import subprocess
import time
import commands
import logging
import requests

# Logging Configuration
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

timestamp = time.strftime("%Y-%m-%d-%H%M") # Timestamp format for backup files.
backupPath = "/backup" # Path to backup directory and/or disk mount.
response = requests.get('http://169.254.169.254/latest/meta-data/instance-id') # Gets EC2 instance Id.
instance_id = response.text # EC2 Instance Id.
snapshotTimestamp = str(instance_id) + "-" + str(timestamp) # Creates name for backup archive file.


# runCommand function, to run OS commands and correctly return stdout.
# Called by the zfsSend function.
def runCommand(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, bufsize=1, shell=True)
    # Grab stdout line by line as it becomes available.  This will loop until
    # p terminates.
    while p.poll() is None:
        l = p.stdout.readline() # This blocks until it receives a newline.
        sys.stdout.write(l)
    # When the subprocess terminates there might be unconsumed output
    # that still needs to be processed.
    sys.stdout.write(p.stdout.read())
    return p.returncode


def zfsSend(snapshot,backupType):
        if backupType == str("full"):
           logging.debug("Performing full snapshot for " + str(instance_id))
           runCommand("zfs snapshot -r eventstore@"+ str(snapshot) + "-" + str(backupType))
           runCommand("zfs send eventstore@" + str(snapshot) + "-" + str(backupType) + " | gzip >" + str(backupPath) + "/" + str(snapshot) + "-full" + ".gz")
           sys.exit(0)
        elif backupType == str("incremental"):
             logging.debug("Performing incremental snapshot for " + str(instance_id))
             lastfullBackup = subprocess.check_output("zfs list -H -t snapshot | awk '{print $1}' | grep full | tail -1", shell=True).strip()
             logging.debug("Last full snapshot is " + str(lastfullBackup) )
             runCommand("zfs snapshot -r eventstore@"+ str(snapshot) + "-" + str(backupType))
             logging.debug("Creating incremental snapshot " + str(snapshot) + "-" + str(backupType))
             lastincBackup = subprocess.check_output("zfs list -H -t snapshot | awk '{print $1}' | grep incremental | tail -1", shell=True).strip()
             runCommand("zfs send -i " + str(lastfullBackup) + " " + str(lastincBackup) + " | gzip > " + str(backupPath) + "/" + str(snapshot) + "-inc" + ".gz")
             sys.exit(0)
        elif backupType == str("differential"):
             logging.debug("Performing differential snapshot for" )
        else:
           sys.exit(1)



parser = argparse.ArgumentParser(description='Creates ZFS Snapshots of Eventstore Data')
parser.add_argument('--full', action='store_true', help='Creates a full snapshot of the data store.')
parser.add_argument('--incremental', action='store_true', help='Creates a incremental snapshot of the data store.')
parser.add_argument('--differential', action='store_true', help='Creates a differential snapshot of the data store.')

args = parser.parse_args()



if __name__ == "__main__":
    os.chdir(backupPath)
    if (args.full):
        backupType = "full"
        print str(snapshotTimestamp)
        zfsSend(snapshotTimestamp,backupType)
       
    elif (args.incremental):
        backupType = "incremental"
        print str(snapshotTimestamp)
        zfsSend(snapshotTimestamp,backupType)

    elif (args.differential):
        backupType = "differential"
        print str(snapshotTimestamp)
        zfsSend(snapshotTimestamp,backupType)
    else:
        logging.debug("No valid parameter detected. Please choose a backup function.")
        logging.debug("InstanceId = " + str(instance_id))
        sys.exit(1)
    sys.exit(0)




