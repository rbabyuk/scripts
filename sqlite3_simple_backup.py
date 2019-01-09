#!/home/pi/sqlite3/bin/python
import sqlite3
import time
import os
import sys
import inspect
import shutil
import yaml #pyyaml module
import gzip
import dropbox
import logging
# TODO:
# - implement retention of local files for example keep 5 file it total and remove oldest
# - retain old backups from dropbox: after 10 days remove all daiy backups and leave just 1
# - implement poor's man monitoring: send email in case of failures
# - implement autodeployment of this script with all dependencies(do not use pre deployed virtual env in shebang)
# - write readme and how to restore backup


def gzipFile(in_file, out_file):
    """
    compress local file in gzip format
    """
    with open(in_file, 'rb') as f_in, gzip.open(out_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
        logging.info("Successfully created gzip archive: {}".format(out_file))

def backupSqlite3DB(db_file, backup_path):
    """
    Takes backup: lock db -> take backup -> unlock db
    """
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    # Lock database before making a backup
    cursor.execute('begin immediate')
    
    # Make new backup file and archive it
    gzipFile(db_file, backup_path)
    
    # Unlock database
    connection.rollback()
    connection.close()


def dropboxUpload(token, local_file, remote_file):
    """
    uploads local file to dropbox account
    used OAuth2 token preconfigured at dropbox account
    """
    dbx = dropbox.Dropbox(token)
    with open(local_file, 'rb') as f:
        dbx.files_upload(f.read(), '/' + remote_file, mute=True)
    logging.info("Successfully uploded {} to dropbox".format(remote_file))

if __name__ == "__main__":
    # sript directory
    script_cwd = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    # log file sits in the same dir as the script
    script_log = os.path.join(script_cwd, 'sqlite3_backup.log')
    # Configure logger
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S", filename=script_log, level=logging.DEBUG)

    properties_file = os.path.join(script_cwd, 'properties.yaml')

    try:
        with open (properties_file, 'rb') as props:
            properties = yaml.load(props)
    except IOError as error:
        logging.error(error)
        logging.error("Properties file: {} cannot be found. Please create one in the same dir where you keep this sript".format(properties_file))
        sys.exit(2)
    """
    token: dropbox access token
    db_path: full path to sqlite3 db
    db_file: db file name
    backup_fname: resulting backup file name
    backup_dir: where we store backup files
    backup_fpath: full path of backup file
    """
    token         = properties["dropbox"]["token"]
    db_path       = properties["db_path"]
    db_file       = properties["db_file"]
    backup_fname  = "{0}{1}.gzip".format(db_file, time.strftime("-%Y%m%d-%H%M%S"))
    backup_dir    = os.path.join(db_path, 'backups')
    backup_fpath  = os.path.join(backup_dir, backup_fname)

    if not os.path.isdir(backup_dir):
        logging.info("Backup directory does not exist: {};\nCreating...".format(backup_dir))
        os.makedirs(backup_dir)
    backupSqlite3DB(db_file, backup_fpath)
    dropboxUpload(token, backup_fpath, backup_fname)
    logging.info("Backup update has been successful.")
