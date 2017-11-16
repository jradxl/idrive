#!/ffp/bin/python

"""
Run cmd:
/ffp/bin/python idrive_uploads.py --source 'NSA325' --user 'pivul@o2.pl'
--password-file=/ffp/idrive/acc_pwd --pvt-key=/ffp/idrive/enc_key
--files-from=/ffp/idrive/backup_list
"""

import os
import sys
from subprocess import Popen, PIPE
import logging
from logging.handlers import RotatingFileHandler
import argparse
from xml.etree import ElementTree as et
import errno

__author__ = 'kpiwk'


DEBUG = True

# The name of the idrive binary
IDRIVE_BIN = 'idevsutil'


def _create_logger(path=None, filename=None):
    if path is not None:
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise
    else:
        path = '.'

    if filename is None:
        filename = 'idrive.log'

    # Set log file name
    log_file = os.path.join(path, filename)

    # Set log level
    _log_level = logging.DEBUG  # logging.INFO, logging.ERROR
    # Set handler: rotating logs

    # Set format
    _format = '[%(asctime)s] %(levelname)s - %(message)s'
    _logformat = logging.Formatter(_format)

    # Set handler: rotating logs
    # rotate after 10MB, keep 9 rotated logs
    _handler = RotatingFileHandler(log_file,
                                   maxBytes=10485760,
                                   backupCount=9)
    _handler.setFormatter(_logformat)

    # Get logger
    log = logging.getLogger(__name__)
    log.setLevel(_log_level)
    log.addHandler(_handler)

    return log


def _exec_cmd(cmd=None, usr_input=None, log=None, debug=DEBUG):
    """
    Executes a command line command.

    :param cmd: Command to execute
    :param usr_input: (Optional) Input to pass to the executed command
    :param debug: Enables debug logging if True
    :return:
    """

    if log is None:
        log = _create_logger(path='{}/log'.format(
            os.path.join(os.path.dirname(__file__), '..')))

    if debug:
        log.debug("Executing command: {0}".format(cmd))

    proc = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    ret, err = proc.communicate(input=usr_input)

    if debug:
        if ret:
            log.debug("Command out: {0}".format(ret))
        if err:
            log.debug("Command err: {0}".format(err))

    # assert proc.returncode == 0, 'Aborting. Return code: {0}'.format(
    # proc.returncode)

    return ret, proc.returncode


def _exec_cmd_flush(cmd=None, usr_input=None, log=None, debug=DEBUG):
    """
    Executes a command line command.

    :param cmd: Command to execute
    :param usr_input: (Optional) Input to pass to the executed command
    :param debug: Enables debug logging if True
    :return:
    """

    if log is None:
        log = _create_logger(path='{}/log'.format(
            os.path.join(os.path.dirname(__file__), '..')))

    if debug:
        log.debug("Executing command: {0}".format(cmd))

    proc = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, stdin=PIPE)

    # Poll process for new output until finished
    while True:
        next_line = proc.stdout.readline()
        if next_line == '' and proc.poll() is not None:
            break
        # sys.stdout.write(next_line)
        # sys.stdout.flush()
        log.debug(next_line)

    ret, err = proc.communicate(input=usr_input)

    if debug:
        if ret:
            log.debug("Command out: {0}".format(ret))
        if err:
            log.debug("Command err: {0}".format(err))

    # assert proc.returncode == 0, 'Aborting. Return code: {0}'.format(
    # proc.returncode)

    return ret, proc.returncode


def _flush_print(text=None, sub=None):
    # Empty buffer - flush text to stdout
    if sub:
        print text % sub
    else:
        print text
    sys.stdout.flush()


def _parse_args():
    """
    Parses command line arguments.
    --address=ADDRESS                      bind address for outgoing socket to
                                           daemon
    --auth-list                            list files/folders level by level
    --bw-file=PATH_OF_BW_FILE              read bandwidth throttle value from
                                           FILE
    --bwlimit=KBPS                         limit I/O bandwidth; Kilo Bytes per
                                           second
    --config-account                       configure your account with
                                           encryption
    --copy-within                          copy file(s)/folder(s) from one
                                           location to other, within the
                                           account on server
    --create-dir=DIR_PATH                  create a directory at server side
    --delete-items                         delete file(s)/folder(s)
    --deletefrom-trash                     permanently delete file(s)/folder(s)
                                           from trash
    --enc-type=DEFAULT                     specify the type of encryption
                                           (--enc-type=DEFAULT or
                                           --enc-type=PRIVATE) that you wish
                                           to configure for your account
    --event-month=MM                       specify month for events
    --event-year=YYYY                      specify year for events
    --eventid=EVENT_ID                     get the details for the specified
                                           event id
    --get-quota                            get total quota, total space used
                                           etc. for a particular account
    --save-event=DIR_PATH                  destination directory for event file
                                           download
    --files-from=PATH_OF_FILE_LIST_FILE    read list of source-file names from
                                           FILE
-0, --from0                                all *-from/filter files are
                                           delimited by 0s
    --get-size                             display size of the folder
    --getServerAddress                     retrieve server address
-4, --ipv4                                 prefer IPv4
-6, --ipv6                                 prefer IPv6
    --items-status                         file(s)/folder(s) status
    --list-timeid                          list upload session time values
    --moveto-original                      move file(s)/folder(s) from trash
                                           to original location
    --mpc=0                                automatically reads your computer
                                           name for multiple computer backup
    --mpc=COMPUTER_NAME                    provide custom computer name for
                                           multiple computer backup
    --password-file=PATH_OF_PSWD_FILE      read password from
    --port=PORT                            specify double-colon alternate port
                                           number
    --properties                           file/folder properties
    --proxy=PROX_INFO                      connect server via proxy. Example
                                           --proxy=PROX_USR:PROX_PWD
                                           @PROX_IP:PROX_PORT
    --pvt-key=PATH_OF_PVT_KEY_FILE         read private encryption key from
                                           FILE
    --rename                               rename file/folder. specify old
                                           and new file path names using
                                           '--old-path=FILE_PATH
                                           --new-path=FILE_PATH parameters
    --old-path=FILE_PATH                   use this parameter along with
                                           '--rename' to specify the old file
                                           path name
    --new-path=FILE_PATH                   use this parameter along with
                                           '--rename' to specify the new file
                                           path name
-r, --recursive                            recurse into directories
-R, --relative                             use relative path names
    --search                               search for file(s)
    --sockopts=OPTIONS                     specify custom TCP options
    --temp=PATH_OF_TEMP_DIR                temp directory path
    --timeline                             records timeline information
-t, --times                                preserve times
    --timeid=DATE                          remember upload session with the
                                           time value reference.
                                           Example: --timeid='DD-MM-YYYY HH:MM'
    --trf-uprate                           calculate transfer rate for upload
    --trf-downrate                         calculate transfer rate for download
    --type                                 display the type
                                           (FULL/INCREMENTAL/SYNC) of
                                           upload/download
    --user=USERNAME                        specify the username
-v, --verbose                              increase verbosity
    --client-version                       display version number for client
    --server-version                       display version number for server
    --version-info                         file version details
    --validate                             validate your account

    :return: Parsed arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--source',
                        help='Source folder path. Most likely computer '
                             'name.')
    parser.add_argument('--target-path',
                        help='Full folder path to download location.')
    parser.add_argument('--password-file',
                        help='Full path to IDrive user\'s password file.',
                        type=str,
                        required=True)
    parser.add_argument('--user',
                        help='The name of the user. Most likely an email '
                             'address.',
                        type=str,
                        required=True)
    parser.add_argument('--pvt-key',
                        help='Full path to encryption key file.',
                        type=str,
                        required=True)
    parser.add_argument('--files-from',
                        help='Full file path to file download list.',
                        type=str,
                        required=True)
    args = parser.parse_args()
    return args


def run_download(idrive_root=None,
                 source=None,
                 target_path=None,
                 user_name=None,
                 pwd_file=None,
                 pvt_key=None,
                 files_from=None,
                 log=None):
    """
    Download the files from cloud.

    :param idrive_root
    :param source:
    :param target_path:
    :param user_name:
    :param pwd_file:
    :param pvt_key:
    :param files_from:
    :param log:
    :return:
    """

    # IDrive root is required
    if idrive_root is None:
        # "WARNING: IDrive root is missing. Use current dir as fallback."
        idrive_root = os.path.join(os.path.dirname(__file__), '..')

    if target_path is None:
        # if target path is not given, download to root
        target_path = '/mnt/HD_a2'

    # Create logger instance
    if log is None:
        log = _create_logger(path='{}/log'.format(idrive_root))

    log.info("Starting download.")

    # Get IDrive server name
    ret, ret_code = _exec_cmd(cmd='{root}/bin/{bin_name} '
                                  '--getServerAddress {user} '
                                  '--password-file={password}'
                                  ''.format(root=idrive_root,
                                            bin_name=IDRIVE_BIN,
                                            user=user_name,
                                            password=pwd_file),
                              log=log)

    # Continue if there's no error
    if ret_code == 0:
        # Read the xml response
        root = et.fromstring(ret)
        tree = dict()
        if root.tag == 'tree':
            tree.update(root.attrib)
            cmd_utility_server = tree.get('cmdUtilityServer')
            if cmd_utility_server:
                log.debug("Found IDrive server: {}".format(cmd_utility_server))
            else:
                log.error("Did not found IDrive server. Aborting.")
                cmd_utility_server = None
                ret_code = 1
        else:
            log.error("Did not found any proper XML response.")
            cmd_utility_server = None
            ret_code = 1

        # Continue if there's no error
        if ret_code == 0:
            # Now, as we have the server name, let's upload the files
            ret, ret_code = _exec_cmd_flush(
                cmd='{root}/bin/{bin_name} '
                    '--verbose '
                    '--xml-output '
                    '--password-file={password} '
                    '--pvt-key={encryption_key} '
                    '--files-from={file_list} '
                    '{user}@{server}::home/{path}/ '
                    '{target}'
                    ''.format(
                        root=idrive_root,
                        bin_name=IDRIVE_BIN,
                        password=pwd_file,
                        encryption_key=pvt_key,
                        file_list=files_from,
                        user=user_name,
                        server=cmd_utility_server,
                        path=source,
                        target=target_path),
                log=log)
            log.info("Download finished.")

    return ret_code


if __name__ == "__main__":
    def main():
        """
        This is the main function.

        :return:
        """

        # Get all arguments
        args = _parse_args()

        source = args.source
        user_name = args.user
        target = getattr(args, 'target_path')
        pwd_file = getattr(args, 'password_file')
        pvt_key = getattr(args, 'pvt_key')
        files_from = getattr(args, 'files_from')

        # Run backup function
        log = _create_logger(path=os.path.dirname(__file__),
                             filename='idrive_downloads.log')
        ret_code = run_download(idrive_root='/ffp/idrive',
                                source=source,
                                target_path=target,
                                user_name=user_name,
                                pwd_file=pwd_file,
                                pvt_key=pvt_key,
                                files_from=files_from,
                                log=log)
        log.info("Run download command returned: {}".format(ret_code))


    main()
