#!/usr/bin/python

import tabular as tb
import os
import BeautifulSoup
import re
import sets

# Base URL for scraping CSV files
BASEURL = 'http://www.data.gov/raw/2159/csv'
FILENAME = 'RISKFACTORSANDACCESSTOCARE.csv'

# Metadata
name = 'Fluoride'
source = {
        "agency": {"shortName": "CDC", "name": "Centers for Disease Control"},
        "subagency": {"shortName": "NCCD", "name": "National Center for Chronic Disease Prevention and Health Promotion "},
        "topic": {"name": "Division of Oral Health"},
        #"subtopic": {"name": "Oral Health Data Systems"},
        "program": {"shortName": "MWF", "name": "My Water's Fluoride"},
        "dataset": {"shortName": "WFRS", "name": "Water Fluoridation Reporting System"}
}


#Need to change the metadata.
metadata = {
ral Health Data Systems
        'title' = 'Oral Health Data Systems',
        'description' = "Safe, effective prevention of tooth decay for people of all ages: Know if your water is optimally fluoridated.Community Health Status Indicators (CHSI) to combat obesity, heart disease, and cancer are major components of the Community Health Data Initiative. This dataset provides key health indicators for local communities and encourages dialogue about actions that can be taken to improve community health (e.g., obesity, heart disease, cancer). The CHSI report and dataset was designed not only for public health professionals but also for members of the community who are interested in the health of their community. The CHSI report contains over 200 measures for each of the 3,141 United States counties. Although CHSI presents indicators like deaths due to heart disease and cancer, it is imperative to understand that behavioral factors such as obesity, tobacco use, diet, physical activity, alcohol and drug use, sexual behavior and others substantially contribute to these deaths.",
        'keywords' = ['Obesity','CHSI','health','data','community','indicators','interventions','performance','measurable','life expectancy','mortality','disease','prevalence','risk','factors','behaviors','socioeconomic','environments','access','cost','quality','warehouse','heart','cancer'],
        'uniqueIndexes' = ['Location'],
        'sliceCols' = [['Location']],
        'columnGroups' = {
                'spaceColumns' = ['Location'],
                'labelColumns' = ['Location']
        }
}









#def get_county_water_system_lists():
    #for each state:
    
       #wget the http://apps.nccd.cdc.gov/MWF/CountyDataV.asp?State=STATEABBREV
       #parse out the counties; then get the water system list for the county
       #get the details for the aters 
       #reduced option down to 'xx', and stringifies the values
       #wget county water systems in each state using for loop

import os
import BeautifulSoup
def parser():
    os.system('wget "http://apps.nccd.cdc.gov/MWF/Index.asp" -O mainfile.html')
    Soup = BeautifulSoup.BeautifulSoup(open('mainfile.html'))
    options = Soup.findAll('option')
    Abbrev = [str(dict(option.attrs)['value']) for option in options[1:]]
    for abbr in Abbrev:
        #toget = '"http://apps.nccd.cdc.gov/MWF/CountyDataV.asp?State=' + abbr + '"'
        toget = '"http://apps.nccd.cdc.gov/MWF/SearchResultsV.asp?State=' + abbr +'&County=ALL&StartPG=1&EndPG=20"'
        os.system('wget ' + toget + ' -O ' + abbr + '_file.html')

from starflow import utils
import tabular as tb
def parser2():
    files_to_parse = [x for x in os.listdir('.') if x.endswith('_file.html')]
    
    for file in files_to_parse:
        print('parsing',file)
        #next step of getting data for each water system
        H20Systems = BeautifulSoup.BeautifulSoup(open(file))
        table = H20Systems.findAll('table')[3].findAll('table')[7]
        TR = table.findAll('tr',bgcolor='#F5F5F5')
        Links = [str(dict(tr.findAll('td')[1].findAll('a')[0].attrs)['href']) for tr in TR]
        Names = [utils.Contents(tr.findAll('td')[1]) for tr in TR]
        Number = [utils.Contents(tr.findAll('td')[2]).replace('&nbsp;',' ') for tr in TR]
        County = [utils.Contents(tr.findAll('td')[3]) for tr in TR]
        outname = file.split('_')[0] + '_file.tsv'

        tb.tabarray(columns = [Links,Names,Number,County],names=['Links','Names','Number','County']).saveSV(outname,metadata=True)

def get_lowest_level():
    files_to_get = [x for x in os.listdir('.') if x.endswith('_file.tsv')][1:]
    #files_to_get = ['MA_file.tsv']

    SERVERNAME = 'http://apps.nccd.cdc.gov/MWF'
    for file in files_to_get:
        print(file)
        X = tb.tabarray(SVfile = file)
        if len(X) > 1:
            state_name = file.split('_')[0]
            dirname = state_name + '_DETAILS'
            utils.MakeDir(dirname)
        
            for (link,id) in X[['Links','Number']]:
                number = id.split(' ')[-1]
            
                os.system('wget "' + SERVERNAME  + '/' + link + '" -O ' + dirname + '/' + number + '.html')

def parse_lowest_level():
    files_to_parse = utils.ListUnion([[os.path.join(x,y) for y in os.listdir(x)] for x in os.listdir('.') if x.endswith('DETAILS')])

    kvpairs = []
    for file in files_to_parse:
        print(file)
        Soup = BeautifulSoup.BeautifulSoup(open(file))
        bolds = Soup.findAll('b')
        bolds = [b for b in bolds if utils.Contents(b).endswith(':')]
        newkvpairs = [(utils.Contents(b).strip(': '),utils.Contents(b.findNext()).strip()) for b in bolds][:-1]
        if len(bolds) > 0:
            newkvpairs.append((utils.Contents(bolds[-1]).strip(': '),''.join([utils.Contents(x) if utils.Contents(x) != '' else '\n' for x in bolds[-1].findNext().contents])))
    
        kvpairs.append(newkvpairs)

    tb.tabarray(kvpairs = kvpairs).saveSV('final_results.tsv',metadata=True)

def parese_clean():
    X = tb.tabarray(SVfile = 'final_results.tsv')
    X['Fluoride concentration'][X['Fluoride concentration'] == ''] = '0.00'
    Fluoride = [float(x.strip(" mg/L")) for x in X["Fluoride concentration"]]
    X = X.addcols(Fluoride,names=['Fluoride concentration'])
    X.saveSV('clean_results.tsv',metadata=True)

    
