#!/opt/bin/python
from os import system
from os.path import exists, basename
import sys
import tabular as tb

# Command line arguments for the current year to start scraping backwards from
# e.g. 2009 back to 1987 (the earliest year with TRI data)
CURRENTYEAR = int(sys.argv[1])
# and the version, or reporting year for the data
VERSION = int(sys.argv[2])
versionString = str(VERSION)[-2:]

# Base URL for scraping CSV files
BASEURL = 'http://www.epa.gov/tri/tridata/current_data/basic/TRI_%s_US_v%s.csv' # % (year, versionString)

# Metadata
name = 'EPA_TRI'
source = {
	"agency": {"shortName": "EPA", "name": "Environmental Protection Agency"}
	"subagency": {"shortName": "OEI", "name": "Office of Environmental Information"}
	"topic": {"name": "Toxic Release Inventory"}
	"subtopic": {"name": "Release Quantity Data"}
	"program": {"shortName": "TRI", "name": "Toxic Release Inventory Program Division"}
	"dataset": {"shortName": "rqd", "name": "Release Quantity Data"}
}
metadata = {
	'title' = 'Toxic Release Inventory Data',
	'description' = """Quantity data reported by Toxic Release Inventory (TRI) facilities, including the quantities of toxic chemicals released on-site, transferred off-site and summary data concerning releases, recycling, energy recovery and treatment as it appears in the Pollution Prevention portion of TRI’s form R""",
	'keywords' = ['TRI', 'toxic', 'release', 'chemicals', 'pollution'],
	'uniqueIndexes' = ['TRI Facility ID', 'CAS #/Compound ID', 'Year'],
	'sliceCols' = ['Primary IC', 'Chemical', 'Clean Air Act Chemical', 'Chemical Classification', 'Metal', 'Carcinogen', 'Parent Company Name'],
	'dateFormat' = 'YYYY',
	'columnGroups' = {
		'timeColumns' = ['Year'],
		'spaceColumns' = ['Location','Latitude','Longitude'],
		'labelColumns' = ['TRI Facility Name', 'Chemical', 'Year']
	}
}
# TODO: Insert metadata into MongoDB

# Methods for cleaning the data
# Combines SIC and NAICS codes
def combineIC(icData):
	if str(icData[0]) != 'nan':
		return (icData[0],'SIC')
	else:
		return (icData[1],'NAICS')

# Combine location data into a dictionary with keys from common/location.py
def buildLocation(locCodes, locData):
	location = {}
	for i in range(0,len(locData)):
		location[locCodes[i]] = locData[i]
	return location
# Original location field names in dataset
# and corresponding location.py space codes
locFields 	= ['Street Address','City','County','ST','ZIP']
locCodes	= ['a', 'W', 'c', 'S', 'p']

# Start of parser
# Retrieve CSV files for CURRENTYEAR to 1987
for year in range(CURRENTYEAR, 1986, -1):
	URL= BASEURL % (year, versionString)
	fname = basename(URL)

	# Don't wget if file already exists
	if not exists(fname):
		system('wget %s' % URL)

	# If file doesn't exist, tell user and skip this year
	if not exists(fname):
		print "Error: File for year %s has not been updated to version %s" % (year, versionString)
		continue

	# Import CSV using tabular, stripping header line with EPA update date
	F = open(fname,'r')
	header = F.readline()
	headersplit = header.split(',')
	updateDate = headersplit[-1]
	X = tb.tabarray(SVfile = fname,skiprows=0,names=headersplit[:-1])

	# Combine SIC and NAICS columns
	types = None
	for field in ['Primary %s', '%s 2', '%s 3', '%s 4', '%s 5', '%s 6']:
		combinedICs = [combineIC(icTuple)[0] for icTuple in X[[field % 'SIC', field % 'NAICS']]]
		X = X.addcols(combinedICs, field % 'IC')
		if not types:
			types = [combineIC(icTuple)[1] for icTuple in X[[field % 'SIC', field % 'NAICS']]] 
			X = X.addcols(types, 'IC Type')
		X = X.deletecols([field % 'SIC', field % 'NAICS'])

	# Combine location information into one 'Location' column
	# Leave Latitude/Longitude columns alone
	locations = [buildLocation(locCodes, locTuple) for locTuple in X[locFields]]
	X = X.addcols(locations, 'Location')
	X = X.deletecols(locFields)

	# Add a version column
	versions = [versionString] * len(X)
	X = X.addcols(versions, 'Version')

	# TODO: Insert data into MongoDB

	# TODO: Clean up downloaded CSV file
	#system('rm -f %s' % fname)
