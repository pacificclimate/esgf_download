#!/usr/bin/python

import esgf_download
import logging
import sys
import argparse

def test_update():
    logging.basicConfig(stream=sys.stdout, level=4)
    esgf_download.metadata_update('/home/data/projects/CMIP5_climdex/downloading/synchro_data_new2.db',
                             search_host="http://pcmdi9.llnl.gov/esg-search/search",
                             project='CMIP5',
                             experiment=['historical', 'historicalNat', 'historicalExt', 'historicalGHG',
                                         'historicalMisc', 'rcp26', 'rcp45', 'rcp60', 'rcp85'],
                             variable=['tasmin', 'tasmax', 'pr'], time_frequency='day', realm='atmos')
    # esgf_download.metadata_update('/home/data/projects/CMIP5_climdex/downloading/test_synchro_data.db',
    #                          search_host="http://pcmdi9.llnl.gov/esg-search/search",
    #                          project='CMIP5',
    #                          experiment=['historical', 'historicalNat', 'historicalExt', 'historicalGHG',
    #                                      'historicalMisc', 'rcp26', 'rcp45', 'rcp60', 'rcp85'],
    #                          variable=['tasmin', 'tasmax', 'pr'], time_frequency='day', realm='atmos', model='MPI-ESM-LR')

def update_metadata(args):
    logging.basicConfig(stream=vars(args).pop('log_output', None), level=vars(args).pop('log_level', None).upper())
    static_args = ['database']
    static_arg_vals = [vars(args).pop(k, None) for k in static_args]
    esgf_download.metadata_update(*static_arg_vals, **vars(args))
    
if __name__ == '__main__':
    # Set up all arguments with grouped options for updating metadata
    parser = argparse.ArgumentParser(description='Update ESGF Metadata Updater')
    g0 = parser.add_argument_group('Metadata Update Top Level Options')
    g0.add_argument('-db', '--database',
                        required=True,
                        help="Path to database file. REQUIRED")
    g0.add_argument('-L', '--log-level',
                        default='info',
                        choices=['debug', 'info', 'warning', 'error', 'critical'],
                        help='Logging level desired: debug, info, warning, error, or critical')
    g0.add_argument('-l', '--log-output',
                        default=sys.stdout,
                        help="Logger output destination, file or stream interpretable by the logger class. Defaults to stdout.")

    # ESGF Update Metadata Options
    g1 = parser.add_argument_group('ESGF Update Options')
    g1.add_argument('-s', '--search-host',
                       default='http://pcmdi9.llnl.gov/esg-search/search',
                       help="Search host")
    g1.add_argument('-p', '--project',
                       required=True,
                       action='append', help='Project, eg "CMIP5"')
    g1.add_argument('-e', '--ensemble',
                       action='append', help='Ensemble, or run, eg "r1i1p1"')
    g1.add_argument('-x', '--experiment',
                       action='append', help='Experiment, eg "rcp45"')
    g1.add_argument('-i', '--institute',
                       action='append', help='Institute, eg "CCCMA"')
    g1.add_argument('-m', '--model',
                       action='append', help='Model, eg "CanCM4"')
    g1.add_argument('-t', '--time-frequency',
                       action='append', help='Time frequency, eg "day" or "mon"')
    g1.add_argument('-v', '--variable',
                       action='append', help='Variable, eg "tas", "tasmin", "tasmax", or "pr"')

    # Additional ESGF arguments
    g2 = parser.add_argument_group("Additional ESGF Options")
    g2.add_argument('--realm',
                       action='append', help='Realm, eg "atmos", "land"')
    g2.add_argument('--long-name',
                       dest='variable_long_name', action='append',
                       help='Varialbe long name, eg "Daily Maximum Near-Surface Air Temperature"')
    g2.add_argument('--cf-name',
                       dest='cf_standard_name', action='append',
                       help='CF Standard Name, eg "air_temperature"')
    g2.add_argument('--family',
                       dest='experiment_family', action='append',
                       help='Experiment Family, eg "ESM", "Historical"')
    g2.add_argument('--source-id',
                       action='append', help='Source id, eg "CloudSat"')
    g2.add_argument('--cmor-table',
                       action='append', help='CMOR Table, eg "atmos"')
    g2.add_argument('--product',
                       action='append', help='Product, eg "output1"')

    args = parser.parse_args()
    update_metadata(args)
