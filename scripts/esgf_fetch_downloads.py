#!/usr/bin/python

import logging
import sys
import argparse

from esgf_download.downloader import Downloader

def test_download():
    logging.basicConfig(stream=sys.stdout, level=4)
    downloader = Downloader('/home/data/projects/CMIP5_climdex/downloading/synchro_data_new2.db',
                                     '/home/data/climate/CMIP5/incoming/', 'bronaugh', 'pcic8UV8', 'pcmdi9.llnl.gov',
                                     initial_threads_per_host=10, max_total_threads=100)
    downloader.go_get_em()

def download(args):
    logging.basicConfig(stream=vars(args).pop('log_output', None), level=vars(args).pop('log_level', None).upper())
    logging.debug(vars(args))
    static_args = ['database', 'output_path', 'username', 'password', 'auth_server', 'initial_threads_per_host', 'max_total_threads']
    static_arg_vals = [vars(args).pop(k, None) for k in static_args]
    downloader = Downloader(*static_arg_vals, **vars(args))
    downloader.go_get_em()

if __name__ == '__main__':
    # Set up all arguments with grouped options for downloading data
    parser = argparse.ArgumentParser(description='ESGF Data Downloader')
    g0 = parser.add_argument_group('Data Downloader Top Level Options')
    g0.add_argument('-db', '--database',
                        required=True,
                        help='Path to database file. REQUIRED')
    g0.add_argument('-L', '--log-level',
                        default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='Logging level desired: "debug", "info", "warning", "error", or "critical"')
    g0.add_argument('-l', '--log-output',
                        default=sys.stdout,
                        help="Logger output destination, file or stream interpretable by the logger class. Defaults to stdout.")

    # ESGF Update Metadata Options
    g1 = parser.add_argument_group('ESGF Required Download Options')
    g1.add_argument('-o', '--output_path',
                    required=True,
                    help='Output directory')
    g1.add_argument('-u', '--username',
                    required=True,
                    help='Authentication username')
    g1.add_argument('-p', '--password',
                    required=True,
                    help='Authentication password')

    # Optional Data Download Options
    g2 = parser.add_argument_group('Additional download options')
    g2.add_argument('-a', '--auth_server',
                    default='pcmdi9.llnl.gov',
                    help='Server to authenticate against')
    g2.add_argument('-t', '--initial_threads_per_host',
                    type=int, default=5,
                    help='Threads per host')
    g2.add_argument('-T', '--max_total_threads',
                    type=int, default=50,
                    help='Max total threads')

    args = parser.parse_args()
    download(args)
