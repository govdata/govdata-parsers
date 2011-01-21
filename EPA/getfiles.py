#!/opt/bin/python
from os import system
from os.path import exists, basename
import sys
import commands
import tabular as tb

# base url for the zip file containing the csv file for all of the us
us_single_url = "http://epa.gov/enviro/html/frs_demo/geospatial_data/state_files/national_single.zip"
us_combined_url = "http://epa.gov/enviro/html/frs_demo/geospatial_data/state_files/national_combined.zip"
csv_files = ['NATIONAL_SINGLE.CSV',
             'NATIONAL_CONTACT_FILE.CSV',
             'NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV']

cmd = 'pwd'
(status,output)=commands.getstatusoutput(cmd)

filenames = os.listdir(output)

# get the files and unzip them

if 'national_single.zip' not in filenames:
    system('wget %s' % us_single_url)
    # unzip the file, 1st check CSV's not in there
    if csv_files[0] not in filenames:
        system('unzip national_single.zip')
else:
    print 'The single file ' + 'national_single.zip ' + 'was already downloaded'
    # check CSV's not in there
    if csv_files[0] not in filenames:
        system('unzip national_single.zip')
                
if 'national_comibned.zip' not in filenames:
    system('wget %s' % us_combined_url)
    # unzip the file, 1st check CSV's not in there
    system('unzip national_combined.zip')
    if csv_files[1] not in filenames or csv_files[2] not in filenames:
        system('unzip national_combined.zip')               
else:
    print 'The combined file ' + 'national_single.zip ' + 'was already downloaded'
    # check CSV's not in there
    if csv_files[1] not in filenames or csv_files[2] not in filenames:
        system('unzip national_combined.zip')
                
