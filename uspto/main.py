def download_from_dvn():
    pass

def unzip_dvn():
    pass

def reorder():
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
    pass

class uspto_iterator(govdata.core.DataIterator):
    pass

    def __init__(self):
        pass

    def refresh(self):
        """
          open an iterative cursor to the 9 sqlite files
          and load into the metadata attribute the metadata
        """

        #open cursors
        #self.cursors =  dictionary of cursors

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
        
        pass
