import sqlite3

def download_from_dvn():
    
    pass

def unzip_dvn():
    pass
    

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
         
            
        #self.metadata = create_metadata()
        
        pass

    def next(self):

        """
         this actually returns the schema record
        """

        #call the next methods as needed of the 9 cursors
        #some will get called multiple times for each single call
        #to the patent DB
        #transform the data into a record that can be inserted in to MongDB
        #return that value

        #coprime with patent:  citation,
        # TODO
        # decode classification, kind, apptype, asgtype, usreldoc kind
        # standardize code names? ie asgtype
        # understand usreldoc, assignee residence
        # write metadata

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

        descr = rec.pop('patdesc')
        rec.update(descr)
        
        rec.pop('AppYear')
        rec.pop('GYear')
        rec['AppDate'] = rec['AppDate'].replace('-','')
        rec['GDate'] = rec['GDate'].replace('-','')

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
        
    
