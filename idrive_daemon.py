import daemon
import daemon.pidfile
import signal

import time
import os
import sys
from timeit import default_timer as timer

import logging
from logging.handlers import RotatingFileHandler
import errno

from idrive_uploads import run_backup
from idrive_downloads import run_download

__author__ = 'kpiwk'

"""
Python daemon that runs backup script every N minutes.
"""

########################################
# User variables
########################################
_idrive_root = '/ffp/idrive/'
_destination = 'NSA325'
_user_name = 'pivul@o2.pl'
_pwd_file = '/ffp/idrive/cfg/acc_pwd'
_pvt_key = '/ffp/idrive/cfg/enc_key'
_files_from = '/ffp/idrive/cfg/backup_list'

_source = 'MI\ 5_861322038690984'
_target_path = '/mnt/HD_a2/photo/synced/Mi5'
_download_from = '/ffp/idrive/cfg/download_list'
########################################


def _create_logger(path=None, filename=None):
    """
    Configure logger instance.

    :param path: Log file parent dir
    :param filename: Log file name
    :return: Configured logger instance
    """
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

    # Set format
    _format = '[%(asctime)s] %(levelname)s - %(message)s'
    _log_format = logging.Formatter(_format)

    # Set handler: rotating logs
    _handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=9)  # rotate after 10MB, keep 9 rotated logs
    _handler.setFormatter(_log_format)

    # Get logger
    log_f = logging.getLogger(__name__)
    log_f.setLevel(_log_level)
    log_f.addHandler(_handler)

    return log_f

########################################
# Create new logger
########################################
log = _create_logger(os.path.join(_idrive_root, 'log'))


def daemon_terminate(signum, frame):
    """
    Signal handler. Logs signal and frame.

    :param signum:
    :param frame:
    :return:
    """
    log.info("IDrive daemon terminated. Received signal: {} at frame: {}".format(signum, frame))
    sys.exit(0)


def run():
    """
    Runs the daemon.

    :return:
    """
    print "Starting IDrive daemon..."

    ########################################
    # Sys variables
    ########################################
    interval = 60  # in minutes
    pid_file_path = os.path.join(_idrive_root, 'bin', 'idrive.pid')

    if os.path.exists(pid_file_path) and os.path.isfile(pid_file_path):
        log.error("Unable to start the daemon. PID file already exists in {}".format(pid_file_path))
        sys.exit(1)

    pid_file = daemon.pidfile.PIDLockFile(pid_file_path)

    # Preserve log handlers inside the daemon; otherwise they get closed
    handles = []
    for handler in log.handlers:
        handles.append(handler.stream.fileno())

    log.info("Starting IDrive daemon...")
    log.debug("Idrive root: {}".format(_idrive_root))
    log.debug("PID file: {}".format(pid_file))
    # Start the daemon
    context = daemon.DaemonContext(working_directory=_idrive_root, pidfile=pid_file, files_preserve=handles)
    context.signal_map = {
        signal.SIGHUP: daemon_terminate,  # kill -1 <pid> # safest
        signal.SIGTERM: daemon_terminate  # kill <pid> # moderate
    }

    log.debug("Context: {}".format(context))

    # Open the daemon
    with context:
        while True:
            # Run the backup command
            log.info("Starting backup.")
            up_start = timer()
            up_rc = run_backup(idrive_root=_idrive_root,
                               destination=_destination,
                               user_name=_user_name,
                               pwd_file=_pwd_file,
                               pvt_key=_pvt_key,
                               files_from=_files_from,
                               log=log)
            up_end = timer()
            if up_rc == 0:
                log.info("Backup time elapsed: {}".format(up_start - up_end))
            else:
                log.error("Backup command failed. Return code was: {}".format(up_rc))

            log.info("Starting download.")
            down_start = timer()
            down_rc = run_download(idrive_root=_idrive_root,
                                   source=_source,
                                   target_path=_target_path,
                                   user_name=_user_name,
                                   pwd_file=_pwd_file,
                                   pvt_key=_pvt_key,
                                   files_from=_download_from,
                                   log=log)
            down_end = timer()
            if down_rc == 0:
                log.info("Download time elapsed: {}".format(down_end - down_start))
            else:
                log.error("Download command failed. Return code was: {}".format(down_rc))

            log.info("[Summary] "
                     "Backup return code {}, elapsed time: {} seconds. "
                     "Download return code {}, elapsed time: {} seconds.".format(up_rc,
                                                                                 up_end - up_start,
                                                                                 down_rc,
                                                                                 down_end - down_start))
            # now sleep for n*60 seconds
            time.sleep(interval * 60)


if __name__ == "__main__":
    run()
