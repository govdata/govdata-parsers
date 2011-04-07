# -*- coding: utf-8 -*-

import sqlite3
import re
import os
import BeautifulSoup
from starflow.utils import Contents
import tabular as tb

# TODO
# decode
#    classification http://www.uspto.gov/web/patents/classification/uspc700/sched700.htm
#    http://www.uspto.gov/patents/resources/classification/numeric/can.jsp 
#    kind, apptype, asgtype, usreldoc kind - type in from paper
#    write metadata

def download_from_dvn():
    
    pass

def unzip_dvn():
    pass
    


def get_class_pages():
    os.system('wget http://www.uspto.gov/patents/resources/classification/numeric/can.jsp -O class_pages.html')
    Soup = BeautifulSoup.BeautifulSoup(open('class_pages.html'))
    P = Soup.findAll('p',align='left')
    A = ['http://uspto.gov/' + str(dict(p.findAll('a')[0].attrs)['href']) for p in P]
    Xlist = []
    for (a,p) in zip(A,P):
        filename = Contents(p).strip().replace(' ','_') + '.html'
        os.system('wget ' + a + ' -O ' + filename)
        CSoup = BeautifulSoup.BeautifulSoup(open(filename))
        T = CSoup.findAll('h1','page-title')[0].findNext('table')
        TR = T.findAll('tr')
        headers = [Contents(t) for t in TR[0].findAll('th')]
        rows = [[Contents(td).replace('&nbsp;','') for td in tr.findAll('td')] for tr in TR[2:]]         
        rows = [r for r in rows if r[0]]
        X = tb.tabarray(records = rows, names = headers)
        Xlist.append(X)
    Y = tb.tab_rowstack(Xlist)
    Cat = Y[['CLASS','CLASS TITLE']].aggregate(On=['CLASS'],AggFunc=lambda x : x[0])
    Cat.saveSV('catlevels.tsv')
    

def fix_cat(cat):
    return '0'*(3 - len(cat)) + cat.lower()
   
def get_subclass_pages():
    X = tb.tabarray(SVfile = 'catlevels.tsv')
    recs = []
    p = re.compile('Sub\d')
    f = lambda x : p.match(dict(x.attrs).get('class',''))
    for x in X[-10:]:
        subrecs = []
        cat = x['CLASS']
        fixed_cat = fix_cat(cat)
        title = x['CLASS TITLE']
        os.system('wget http://www.uspto.gov/web/patents/classification/uspc' + fixed_cat + '/sched' + fixed_cat + '.htm -O ' + cat + '.html')
        Soup = BeautifulSoup.BeautifulSoup(open(cat + '.html'))
        Crac = Soup.find(True,'CracHeader')
        For = Soup.find(True,'ForHeader')
        Dig = Soup.find(True,'DigHeader')
        if Crac:
            end = Crac
        elif For:
            end = For
        elif Dig:
            end = Dig
        else:
            end  = None
        if end:
            T = end.findAllPrevious('tr',valign='top')[:]
        else:
            T = Soup.findAll('tr',valign='top')[:]
        T.reverse()
        for (i,t) in enumerate(T): 
            try:
                subclass = Contents(t.find(f)).strip()
            except:
                pass
            else:
                try:
                    subtitle = Contents(t.find(True,"SubTtl")).strip()
                except:
                    pass
                else:
                    try:
                        indent = int(dict(t.find(True,"SubTtl").find("img").attrs)['src'].split('/')[-1].split('_')[0])
                    except AttributeError:
                        indent = 0
                    #print (cat,title,subclass,subtitle,indent)    
                    subrecs.append((cat,title,subclass,subtitle,indent))
        subrecs.reverse()
        recs.extend(subrecs)

    Y = tb.tabarray(records = recs, names=['Class','Title','Subclass','Subtitle','Indent'])
    Y.saveSV('classifications.tsv')


def create_metadata():
    """
      the metadata
      it has to define a source and some other things that we'll
      decied on later ...
      it will return a python dictionary whose keys are the fields
      and whose values are the metadata values for those fields
      {"title":"USPTO Data", "keywords":["patent",...], } &c
      
    """
    metadata = {}
    metadata['dateFormat'] = 'YYYYmmdd'
    columnGroups = {}
    columnGroups['timeColumns'] = []
    columnGroups['spaceColumns'] = []
    metadata['columnGroups'] = columnGroups
    metadata['source'] = [('agency', {'name': 'Department of Commerce', 'shortName': 'DOC'}),
                          ('subagency', {'name': 'United Patent and Trademark Office', 'shortName': 'USPTO'}),
                          ('dataset', {'name': 'Patent Grant Bibliographic Data', 'shortName': 'PGBD'})]
    metadata['uniqueIndexes'] = ['Patent']

    #Kind, ApplicationType, assignee.Assignee, assignee.AsgType, Title,
    #assignee.location, class, inventor.Nationality, inventor.location,
    #lawyer.OrgName, 'inventor.name','PatentType'
    metadata['sliceCols'] = [[]]
    metadata['sliceContents'] = ['Abstract']

    #other metadata...

    kind ={
    'A1' : "Utility Patent Grant issued prior to January 2, 2001 or Utility Patent Application published on or after January 2, 2001",
    'A2' : "Second or subsequent publication of a Utility Patent Application",
    'A9' : "Correction  published Utility Patent Application",
    'Bn' : "Reexamination Certificate issued prior to January 2, 2001.'n' represents a value 1 through 9.",
    'B1' : "Utility Patent Grant (no published application) issued on or after January 2, 2001.",
    'B2' : "Utility Patent Grant (with a published application) issued on or after January 2, 2001",
    'Cn' : "Reexamination Certificate issued on or after January 2, 2001.'n' Represents a value 1 through 9 denoting the publication level.",
    'E1' : "Reissue Patent",
    'H1' : "Statutory Invention Registration (SIR) Patent Documents. SIR documents began with the December 3, 1985 issue",
    'I1' : "'X' Patents issued from July 31, 1790 to July 13, 1836",
    'I2' : "'X' Reissue Patents issued from July 31, 1790 to July 4, 1836",
    'I3' : "Additional Improvements – Patents issued issued between 1838 and 1861.",
    'I4' : "Defensive Publication – Documents issued from Nov 5, 1968 through May 5, 1987",
    'I5' : "Trial Voluntary Protest Program (TVPP) Patent Documents",
    'NP' : "Non-Patent Literature",
    'P1' : "Plant Patent Grant issued prior to January 2, 2001",
    'P1' : "Plant Patent Application published on or after January 2, 2001",
    'P2' : "Plant Patent Grant (no published application) issued on or after January 2, 2001",
    'P3' : "Plant Patent Grant (with a published application) issued on or after January 2, 2001",
    'P4' : "Second or subsequent publication of a Plant Patent Application",
    'P9' : "Correction publication of a Plant Patent Application",
    'S1' : "Design Patent"}

    field_names={
    'Patent':"8 character alpha numeric identification code assigned by USPTO",
    'Assignee':"name of the owner of the patent",
    'ApplicationDate':"week of patent application",
    'ApplicationNum': "patent application number",
    'Claims': "number of claims on the patent application",
    'GrantDate': "patent grant date",
    'Kind': "patent kind code",
    'PatentType': "patent type - either utility or design",
    'Title': "patent title",
    'citation': "patents cited",
    'class': "primary and subclassification of the patent",
    'lawyer': "lawyers and law offices associated with the patent",
    'usreldoc': "related patent documents"}

    util_app_types={
        2: "Filed prior to January 1, 1948",
        3: "Filed January 1, 1948 through December 31, 1959",
        4: "Filed January 1, 1960 through December 31, 1969",
        5: "Filed January 1, 1970 through December 31, 1978",
        6: "Filed January 1, 1979 through December 31, 1986",
        7: "Filed January 1, 1987 through January 21, 1993",
        8: "Filed January 22, 1993 through January 20, 1998",
        9: "Filed January 21, 1998 through October 23, 2001",
        10: "Filed October 24, 2001 through November 30, 2004",
        11: "Filed December 1, 2004 through December 5, 2007",
        12: "Filed December 6, 2007 through Current"}
    design_app_types={
        7: "Filed prior to October 1, 1992",
        29: "Filed after October 1, 1992"}



    metadata['kind']= kind
    metadata['field_names']=field_names
    metadata['util_app_types']= util_app_types
    metadata['design_app_types']= design_app_types

    return metadata

class uspto_iterator(object):

    def __init__(self):
        self.cursors = {}

    def __iter__(self):
        return self

    def refresh(self):
        """
          open an iterative cursor to the 9 sqlite files
          and load into the metadata attribute the metadata
        """

        dbnames = ['patent', 'patdesc', 'inventor', 'assignee','class', 'lawyer', 'citation', 'sciref', 'usreldoc']
        self.dbnames = dbnames
        self.current_values = {}
        for dbname in dbnames:
            conn = sqlite3.connect(dbname + '.sqlite3')
            conn.row_factory = sqlite3.Row #plug in sqlite3.Row - mimic tuples but allows for mapping access
            c = conn.cursor()
            c.execute("SELECT * from %s order by patent" % dbname)
            self.cursors[dbname] = c
            self.current_values[dbname] = c.next()
            print self.current_values[dbname].keys() #returns a tuple of column names
         
            
        self.metadata = create_metadata()
        
        pass

    def next(self):

        """
         this actually returns the schema record
        """


        # NOTE: asgtype - not explained by uspto? 

        patent = self.current_values['patent'][0]
        
        rec = {}
        for dbname in self.dbnames:
            vals = []
            while True:
                val = self.current_values[dbname]
                if val[0] != patent:
                    break
                else:
                    processed_val = dict([(k,val[k]) for k in val.keys() if val[k] != ''])

                    if dbname != 'patent':
                        processed_val.pop('Patent')
                    vals.append(processed_val)
                    self.current_values[dbname] = self.cursors[dbname].next()
                
            
            if vals:     
                rec[dbname] = vals

        for k in ['Cit_Date', 'Cit_Name', 'Cit_Kind', 'Cit_Country', 'CitSeq']:
            # make sure that data are ordered by CitSeq
            for r in rec.get('citation',[]):
                r.pop(k, None)
        # make sure that data are order by their respective Seq numbers
        for r in rec.get('inventor',[]):
            r.pop('InvSeq')
        for r in rec.get('lawyer',[]):
            r.pop('LawSeq')
        for r in rec.get('assignee',[]):
            r.pop('AsgSeq')
        for r in rec.get('usreldoc', []):
            r.pop('OrderSeq')
            r.pop('Country')

        for r in rec.get('assignee',[]):
            location = {}
            if r.get('State'):
                location['S'] = r.pop('State')
            if r.get('City'):
                location['W'] = r.pop('City')
            if r.get('Country'):
                location['C'] = r.pop('Country')

            r['location'] = location

        for r in rec.get('inventor',[]):
            r['name'] = {}
            f = r.pop('Firstname',None)
            if f:
                r['name']['first'] = f
            l = r.pop('Lastname',None)
            if l:
                r['name']['last'] = l

        for r in rec.get('lawyer',[]):
            r['name'] = {}
            f = r.pop('Firstname',None)
            if f:
                r['name']['first'] = f
            l = r.pop('Lastname',None)
            if l:
                r['name']['last'] = l
                

        for r in rec.get('inventor',[]):
            location = {}
            if r.get('Street'):
                location['a'] = r.pop('Street')
            if r.get('State'):
                location['S'] = r.pop('State')
            if r.get('City'):
                location['W'] = r.pop('City')
            if r.get('Country'):
                location['C'] = r.pop('Country')
            if r.get('Zipcode'):
                location['p'] = r.pop('Zipcode')
            if r.get('Nationality'):
                if r.get('Nationality') != 'omitted':
                    r['Nationality'] = {'C':r['Nationality']}
                else:
                    r.pop('Nationality')

            r['location'] = location    

        for r in rec.get('lawyer', []):
            r.pop('LawCountry')

        for k in rec:
            if len(rec[k]) == 1:
                rec[k] = rec[k][0]

        for k in rec['patent']:
            rec[k] = rec['patent'][k]
        rec.pop('patent')
        rec['Kind']=self.metadata['kind'][rec['Kind']]

        descr = rec.pop('patdesc')
        rec.update(descr)
        
        rec.pop('AppYear')
        rec.pop('GYear')
        rec['AppDate'] = rec['AppDate'].replace('-','')
        rec['GDate'] = rec['GDate'].replace('-','')
        if rec['PatType']=='utility':
            rec['AppType'] = self.metadata['util_app_types'][rec['AppType']]
        elif rec['PatType']=='design':
            rec['AppType'] = self.metadata['design_app_types'][rec['AppType']]
        
        toReplace = [('ApplicationDate', 'AppDate'),
                     ('GrantDate', 'GDate'),
                     ('PatentType', 'PatType'),
                     ('ApplicationNumber', 'AppNum'),
                     ('ApplicationType', 'AppType')]
        for a,b in toReplace:
            rec[a] = rec.pop(b)
        
        
        return rec
   

PATENT_SCHEMA = """
CREATE TABLE patent (
                    Patent VARCHAR(8),      Kind VARCHAR(3),        Claims INTEGER,
                    AppType INTEGER,        AppNum VARCHAR(8),      
                    GDate INTEGER,          GYear INTEGER,
                    AppDate INTEGER,        AppYear INTEGER);
"""

CITATION_SCHEMA = """
CREATE TABLE citation (
                    Patent VARCHAR(8),      Cit_Date INTEGER,       Cit_Name VARCHAR(10),
                    Cit_Kind VARCHAR(1),    Cit_Country VARCHAR(2), Citation VARCHAR(8),
                    Category VARCHAR(15),   CitSeq INTEGER);
"""

CLASS_SCHEMA = """
CREATE TABLE class (
                    Patent VARCHAR(8),      Prim INTEGER,
                    Class VARCHAR(3),       SubClass VARCHAR(3));
"""

INVENTOR_SCHEMA = """
CREATE TABLE inventor (
                    Patent VARCHAR(8),      Firstname VARCHAR(15),  Lastname VARCHAR(15),
                    Street VARCHAR(15),     City VARCHAR(10),
                    State VARCHAR(2),       Country VARCHAR(12),
                    Zipcode VARCHAR(5),     Nationality VARCHAR(2), InvSeq INTEGER);
"""

PATDESC_SCHEMA = """
CREATE TABLE patdesc (
                    Patent VARCHAR(8),
                    Abstract VARCHAR(50),   Title VARCHAR(20));
"""

SCIREF_SCHEMA = """
CREATE TABLE sciref (
                    Patent VARCHAR(8),      Descrip VARCHAR(20),    CitSeq INTEGER);
"""

ASSIGNEE_SCHEMA = """
CREATE TABLE assignee (
                    Patent VARCHAR(8),      AsgType INTEGER,        Assignee VARCHAR(30),
                    City VARCHAR(10),       State VARCHAR(2),       Country VARCHAR(2),
                    Nationality VARCHAR(2), Residence VARCHAR(2),   AsgSeq INTEGER);
"""

LAWYER_SCHEMA = """
CREATE TABLE lawyer (
                    Patent VARCHAR(8),      Firstname VARCHAR(15),  Lastname VARCHAR(15),
                    LawCountry VARCHAR(2),  OrgName VARCHAR(20),    LawSeq INTEGER);
"""

USRELDOC_SCHEMA = """
CREATE TABLE usreldoc (
                    Patent VARCHAR(8),      DocType VARCHAR(10),    OrderSeq INTEGER,
                    Country VARCHAR(2),     RelPatent VARCHAR(8),   Kind VARCHAR(2),
                    RelDate INTEGER,        Status VARCHAR(10));
"""
        
    
