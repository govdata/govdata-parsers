#!/opt/bin/python
import tabular as tb
import os
import BeautifulSoup
import re
import sets

# Base URL for scraping CSV files
BASEURL = 'http://www.data.gov/raw/2159/csv'
FILENAME = 'RISKFACTORSANDACCESSTOCARE.csv'

# Metadata
name = 'riskfactors'
source = {
	"agency": {"shortName": "DHHS", "name": "Department of Health and Human Services"},
	"subagency": {"shortName": "CDC", "name": "Centers for Disease Control and Prevention"},
	"topic": {"name": "Health and Nutrition"},
	#"subtopic": {"name": "Release Quantity Data"},
	"program": {"shortName": "CHSI", "name": "Community Health Status Indicators"},
	"dataset": {"shortName": "ohdc", "name": "Community Health Status Indicators (CHSI) to Combat Obesity, Heart Disease and Cancer"}
}
metadata = {
	'title' = 'Community Health Status Indicators (CHSI) to Combat Obesity, Heart Disease and Cancer',
	'description' = "Community Health Status Indicators (CHSI) to combat obesity, heart disease, and cancer are major components of the Community Health Data Initiative. This dataset provides key health indicators for local communities and encourages dialogue about actions that can be taken to improve community health (e.g., obesity, heart disease, cancer). The CHSI report and dataset was designed not only for public health professionals but also for members of the community who are interested in the health of their community. The CHSI report contains over 200 measures for each of the 3,141 United States counties. Although CHSI presents indicators like deaths due to heart disease and cancer, it is imperative to understand that behavioral factors such as obesity, tobacco use, diet, physical activity, alcohol and drug use, sexual behavior and others substantially contribute to these deaths.",
	'keywords' = ['Obesity','CHSI','health','data','community','indicators','interventions','performance','measurable','life expectancy','mortality','disease','prevalence','risk','factors','behaviors','socioeconomic','environments','access','cost','quality','warehouse','heart','cancer'],
	'uniqueIndexes' = ['Location'],
	'sliceCols' = [['Location']],
	'columnGroups' = {
		'spaceColumns' = ['Location'],
		'labelColumns' = ['Location']
	}
}
# TODO: Insert metadata into MongoDB

def download():
        os.system("wget " + BASEURL + " -O risk.html")
        S = BeautifulSoup.BeautifulSoup(open('risk.html'))
        for tag in S.findAll('a', href=True):
                if "download" in tag['href']:
                        #print tag['href']
                        url="http://data.gov"+tag['href']
        print url
        os.system("wget "+url+" -O files.zip")

#"{'s':'Alabama,'c':'Macon','f':{'s':'02','c':'013'}"

def unzip():
        os.system("unzip files.zip")

# Combine location data into a dictionary with keys from common/location.py
def buildLocation(locCodes, locData):
        location = {}
	locationF = {}
        #for i in range(0,len(locData)):
        #        location[locCodes[i]] = locData[i]
	location[locCodes[0]]=locData[0]
	location[locCodes[1]]=locData[1]
	
	locationF[locCodes[0]]=locData[2]
	locationF[locCodes[1]]=locData[3]
	location[locCodes[2]]=locationF
        return repr(location)

# Original location field names in dataset
# and corresponding location.py space codes
locFields       = ['CHSI_State_Name', 'CHSI_County_Name', 'State_FIPS_Code', 'County_FIPS_Code']
locCodes        = ['s','c','f']


# Start of parser
# Retrieve CSV file
def csvparser():
	# Import CSV using tabular, stripping header line with EPA update date
	F = open(FILENAME,'r')
	header = F.readline()
	headersplit = header.split(',')
	updateDate = headersplit[-1]
	X = tb.tabarray(SVfile = fname,skiprows=0,names=headersplit[:-1])

        # Combine location information into one 'Location' column
	locations = [buildLocation(locCodes, locTuple) for locTuple in X[locFields]]
        X = X.addcols(locations, 'Location')
        X = X.deletecols(locFields)

	# TODO: Insert data into MongoDB

	# TODO: Clean up downloaded CSV file
	#system('rm -f %s' % fname)
