from xml.dom import minidom
from types import *
import datetime, csv, os, re, sqlite3
import copy
##import numpy as np
##import scipy as sp
import unicodedata

def uniasc(x, form='NFKD', action='replace', debug=False):
    # unicode to ascii format
    if debug:
        print x
    return unicodedata.normalize(form, x).encode('ascii', action)

def ron_d(xml, itr=0, defList=[], cat="", debug=False):
    xmlcopy = []
    if itr==0:
        pass
    else:
        xmlist = copy.copy(defList)
        for x in xml.childNodes:
            if x.nodeName[0] != "#":
                if debug:
                    print x.nodeName
                if xmlist.count(cat+x.nodeName)==0 and \
                   len(re.findall("[A-Z0-9]", innerHTML(x), re.I))>0:
                    xmlist.append(cat+x.nodeName)
                xmlist.extend(ron_d(x, itr-1, cat=cat+x.nodeName+"|", debug=debug))
                
        xmlcopy = copy.copy(xmlist)
        for x in xmlist:
            if xmlcopy.count(x)>1:
                xmlcopy.remove(x)
        xmlcopy.sort()
    return xmlcopy 

def innerHTML(dom_element):
    #if blank return nothing as well!
    if dom_element == '':
        return ''
    else:
        rc = ""
        for node in dom_element.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc = rc + node.data
        return rc
    
def XMLstruct(strList, debug=False):
    xmlstruct = []
    for i,x in enumerate(strList):
        if debug and i%(max(1, len(strList)/20))==0:
            print i
        xmlstruct = ron_d(minidom.parseString(x), 10, defList=xmlstruct)
    return xmlstruct

class SQLPatent:

    def dbBuild(self, q, tbl, week=0, legacy=True):
##        if legacy:
##            table = os.path.isfile(tbl+"_l.sqlite3")
##            conn = sqlite3.connect(tbl+"_l.sqlite3")
##        else:
        table = os.path.isfile("%s.sqlite3" % tbl)
        conn = sqlite3.connect("%s.sqlite3" % tbl)            
        c = conn.cursor()
        ##c.execute("PRAGMA synchronous = 0")
        self.dbTbl(tbl=tbl, c=c, legacy=legacy)

        if c.execute("SELECT count(*) FROM gXML WHERE week=?", (week,)).fetchone()[0]==0:
            #INSERT STUFF
            if tbl=="assignee":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""" % tbl, q)
                # q is the list of lists
            elif tbl=="citation":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?)""" % tbl, q)
            elif tbl=="class":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?)""" % tbl, q)
            elif tbl=="inventor":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""" % tbl, q)
            elif tbl=="patent":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""" % tbl, q) #add one more ?
            elif tbl=="patdesc":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?)""" % tbl, q)
            elif tbl=="lawyer":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?)""" % tbl, q)
            elif tbl=="sciref":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?)""" % tbl, q)
            elif tbl=="usreldoc":
                c.executemany("""INSERT OR IGNORE INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?)""" % tbl, q)
            c.execute("INSERT INTO gXML VALUES (?)", (week,))

        conn.commit()
        c=None
        conn=None

    def dbFinal(self, tbl, legacy=True):
        conn = sqlite3.connect("%s.sqlite3" % tbl)            
        c = conn.cursor()
        if tbl=="assignee":
##            c.execute("CREATE INDEX IF NOT EXISTS idx_pata ON %s (Patent, AsgSeq)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_asg    ON %s (Assignee)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_asgtyp ON %s (AsgType)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_stt ON %s (State)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_cty ON %s (Country)" % tbl)
        elif tbl=="citation":
##            print "[citation] to be continued..."
            c.execute("CREATE INDEX IF NOT EXISTS idx_patc ON %s (Patent, Citation)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_cit ON %s (Citation)" % tbl)
        elif tbl=="class":
##            c.execute("CREATE INDEX IF NOT EXISTS idx_patcs ON %s (Patent, Class, SubClass)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_prim ON %s (Prim)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_cls  ON %s (Class)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_scls ON %s (SubClass)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_cscl ON %s (Class,SubClass)" % tbl)
        elif tbl=="inventor":
##            c.execute("CREATE INDEX IF NOT EXISTS idx_pati ON %s (Patent, InvSeq)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_stt ON %s (State)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_cty ON %s (Country)" % tbl)
        elif tbl=="patent":
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_ayr ON %s (AppYear)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_gyr ON %s (GYear)" % tbl)
        elif tbl=="patdesc":
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
        elif tbl=="lawyer":
            c.execute("CREATE INDEX IF NOT EXISTS idx_patl ON %s (Patent, LawSeq)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
        elif tbl=="sciref":
##            c.execute("CREATE INDEX IF NOT EXISTS idx_patc ON %s (Patent, CitSeq)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
        elif tbl=="usreldoc":
##            c.execute("CREATE INDEX IF NOT EXISTS idx_pator ON %s (Patent, OrderSeq, RelPatent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_patent ON %s (Patent)" % tbl)
            c.execute("CREATE INDEX IF NOT EXISTS idx_relpat ON %s (RelPatent)" % tbl)
        c.close()
        conn.close()

    def dbTbl(self, tbl, c, legacy=True):
        c.execute("CREATE TABLE IF NOT EXISTS gXML ( week VARCHAR )")
        if tbl=="assignee":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS assignee (
                    Patent VARCHAR(8),      AsgType INTEGER,        Assignee VARCHAR(30),
                    City VARCHAR(10),       State VARCHAR(2),       Country VARCHAR(2),
                    Nationality VARCHAR(2), Residence VARCHAR(2),   AsgSeq INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS uqAsg ON assignee (Patent, AsgSeq);
                DROP INDEX IF EXISTS idx_pata;
                DROP INDEX IF EXISTS idx_patent;
                DROP INDEX IF EXISTS idx_asgtyp;
                DROP INDEX IF EXISTS idx_stt;
                DROP INDEX IF EXISTS idx_cty;
                """)
        elif tbl=="citation":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS citation (
                    Patent VARCHAR(8),      Cit_Date INTEGER,       Cit_Name VARCHAR(10),
                    Cit_Kind VARCHAR(1),    Cit_Country VARCHAR(2), Citation VARCHAR(8),
                    Category VARCHAR(15),   CitSeq INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS uqCit ON citation (Patent, CitSeq);
                DROP INDEX IF EXISTS idx_patc;
                DROP INDEX IF EXISTS idx_patent;
                DROP INDEX IF EXISTS idx_cit;
                """)
        elif tbl=="class":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS class (
                    Patent VARCHAR(8),      Prim INTEGER,
                    Class VARCHAR(3),       SubClass VARCHAR(3));
                CREATE UNIQUE INDEX IF NOT EXISTS uqClass ON class (Patent, Class, SubClass);
                DROP INDEX IF EXISTS idx_patcs;
                DROP INDEX IF EXISTS idx_patent;
                DROP INDEX IF EXISTS idx_prim;
                """)
        elif tbl=="inventor":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS inventor (
                    Patent VARCHAR(8),      Firstname VARCHAR(15),  Lastname VARCHAR(15),
                    Street VARCHAR(15),     City VARCHAR(10),
                    State VARCHAR(2),       Country VARCHAR(12),
                    Zipcode VARCHAR(5),     Nationality VARCHAR(2), InvSeq INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS uqInv ON inventor (Patent, InvSeq);
                DROP INDEX IF EXISTS idx_pati;
                DROP INDEX IF EXISTS idx_patent;
                DROP INDEX IF EXISTS idx_stt;
                DROP INDEX IF EXISTS idx_cty;
                """)
        elif tbl=="patent": #add PatType VARCHAR(15)
            c.executescript("""
                CREATE TABLE IF NOT EXISTS patent (
                    Patent VARCHAR(8),      Kind VARCHAR(3),        Claims INTEGER,
                    AppType INTEGER,        AppNum VARCHAR(8),      
                    GDate INTEGER,          GYear INTEGER,
                    AppDate INTEGER,        AppYear INTEGER, PatType VARCHAR(15) );
                CREATE UNIQUE INDEX IF NOT EXISTS uqPat on patent (Patent);
                DROP INDEX IF EXISTS idx_patent;
                DROP INDEX IF EXISTS idx_ayr;
                DROP INDEX IF EXISTS idx_gyr;
                """)
        elif tbl=="patdesc":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS patdesc (
                    Patent VARCHAR(8),
                    Abstract VARCHAR(50),   Title VARCHAR(20));
                CREATE UNIQUE INDEX IF NOT EXISTS uqPatDesc ON patdesc (Patent);
                DROP INDEX IF EXISTS idx_patent;
                """)
        elif tbl=="lawyer":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS lawyer (
                    Patent VARCHAR(8),      Firstname VARCHAR(15),  Lastname VARCHAR(15),
                    LawCountry VARCHAR(2),  OrgName VARCHAR(20),    LawSeq INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS uqLawyer ON lawyer (Patent, LawSeq);
                DROP INDEX IF EXISTS idx_patl;
                DROP INDEX IF EXISTS idx_patent;
                """)
        elif tbl=="sciref":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS sciref (
                    Patent VARCHAR(8),      Descrip VARCHAR(20),    CitSeq INTEGER);
                CREATE UNIQUE INDEX IF NOT EXISTS uqSciref ON sciref (Patent, CitSeq);
                DROP INDEX IF EXISTS idx_patc;
                DROP INDEX IF EXISTS idx_patent;
                """)
        elif tbl=="usreldoc":
            c.executescript("""
                CREATE TABLE IF NOT EXISTS usreldoc (
                    Patent VARCHAR(8),      DocType VARCHAR(10),    OrderSeq INTEGER,
                    Country VARCHAR(2),     RelPatent VARCHAR(8),   Kind VARCHAR(2),
                    RelDate INTEGER,        Status VARCHAR(10));
                CREATE UNIQUE INDEX IF NOT EXISTS uqUSRelDoc ON usreldoc (Patent, OrderSeq);
                DROP INDEX IF EXISTS idx_pator;
                DROP INDEX IF EXISTS idx_patent;
                """)
            
    def tblBuild(self, patents, tbl, legacy=True):
        q = [] # creating the list of lists
        for x in patents:
            if tbl=="assignee":
                for i,y in enumerate(x.asg_list):
                    if y[0]==0:
                        q.extend([[x.patent, y[2], y[1], y[4], y[5], y[6], y[7], y[8], i]])
                    else:
                        q.extend([[x.patent, "00", "%s, %s" % (y[2], y[1]), y[4], y[5], y[6], y[7], y[8], i]])
            elif tbl=="citation":
                cit_list = [y for y in x.cit_list if y[1]!=""]
                for i,y in enumerate(cit_list):
                    q.extend([[x.patent, y[3], y[5], y[4], y[1], y[2], y[0], i]])
            elif tbl=="class":
                for i,y in enumerate(x.classes):
                    q.extend([[x.patent, (i==0)*1, y[0], y[1]]])
            elif tbl=="inventor":
                for i,y in enumerate(x.inv_list):
                    q.extend([[x.patent, y[1], y[0], y[2], y[3], y[4], y[5], y[6], y[8], i]])
            elif tbl=="patent":
                q.extend([[x.patent, x.kind, x.clm_num, x.code_app, x.patent_app, x.date_grant, x.date_grant[:4], x.date_app, x.date_app[:4], x.pat_type]]) #add x.pat_type
            elif tbl=="patdesc":
                q.extend([[x.patent, x.abstract, x.invention_title]])
            elif tbl=="lawyer":
                for i,y in enumerate(x.law_list):
                    q.extend([[x.patent, y[1], y[0], y[2], y[3], i]])
            elif tbl=="sciref":
                cit_list = [y for y in x.cit_list if y[1]==""]
                for i,y in enumerate(cit_list):
                    q.extend([[x.patent, y[-1], i]])
            elif tbl=="usreldoc":
                for i,y in enumerate(x.rel_list):
                    if y[1]==1:
                        q.extend([[x.patent, y[0], y[1], y[3], y[2], y[4], y[5], y[6]]])
                    else:
                        q.extend([[x.patent, y[0], y[1], y[3], y[2], y[4], "", ""]])
                        
        return q            
            
class XMLPatent:
    def __init__(self, XMLString, debug=False):
        #XMLString conversion so tags are all lower
        XMLString = re.sub(r"<[/]?[A-Za-z-]+?[ >]", lambda x: x.group().lower(), XMLString)
##        XMLString = re.sub(r"(?<![</A-Za-z-])[/]?[A-Za-z-]+?>", lambda x: "<"+x.group().lower(), XMLString)        
        xmldoc = minidom.parseString(XMLString)
        #patent related detail
        #  patent number, kind, date_grant, date_app, country, pat_type
        if debug:
            print "  - country, patent, kind, date_grant"
        self.country, self.patent, self.kind, self.date_grant = self.__tagNme__(xmldoc, ["publication-reference", ["country", "doc-number", "kind", "date"]])
        if debug:
            print "  - pat_type"
        self.pat_type = self.__tagNme__(xmldoc, ["application-reference"], iHTML=False)[0].attributes["appl-type"].value
        if debug:
            print "  - date_app, country_app, patent_app"
        self.date_app, self.country_app, self.patent_app = self.__tagNme__(xmldoc, ["application-reference", ["date", "country", "doc-number"]])
        if debug:
            print "  - code_app"
        self.code_app = self.__tagNme__(xmldoc, ["us-application-series-code"])
        if debug:
            print "  - clm_num"
        self.clm_num = self.__tagNme__(xmldoc, ["number-of-claims"])
        if debug:
            print "  - classes"
        self.classes = [[x[:3].replace(' ',''), x[3:].replace(' ','')] for x in self.__tagNme__(xmldoc, ["classification-national", ["main-classification", "further-classification"]], idx=1, listType=True)]

        if debug:
            print "  - abstract"
        self.abstract = self.__allHTML__(xmldoc, ["abstract", "p"])
        if debug:
            print "  - invention_title"
        self.invention_title = self.__allHTML__(xmldoc, ["invention-title"])

        if debug:
            print "  - asg_list"
        self.asg_list = self.__asg_detail__(self.__tagNme__(xmldoc, ["assignees", "assignee"], iHTML=False))
        if debug:
            print "  - cit_list"
        self.cit_list = self.__cit_detail__(self.__tagNme__(xmldoc, ["references-cited", "citation"], iHTML=False))
        if debug:
            print "  - rel_list"
        self.rel_list = self.__rel_detail__(self.__tagNme__(xmldoc, ["us-related-documents"], iHTML=False))

        if debug:
            print "  - inv_list"
        self.inv_list = self.__tagSplit__(xmldoc, ["parties", "applicant"], [["addressbook", ["last-name", "first-name"]], ["addressbook", "address", ["street", "city", "state", "country", "postcode"]], [["nationality", "residence"], "country"]], blank=True)
        if debug:
            print "  - law_list"
        self.law_list = self.__tagSplit__(xmldoc, ["parties", "agents", "agent"], [["addressbook", ["last-name", "first-name", "country", "orgname"]]], blank=True)

    def __allHTML__(self, xmldoc, tagList):
        for x in self.__tagNme__(xmldoc, tagList, iHTML=False):
            return re.sub(r"<[/]?%s( .*?)?>" % (tagList[-1]), "", x.toxml())
        return ""

    def __innerHTML__(self, dom_element):
        #if blank return nothing as well!
        if dom_element == '':
            return ''
        else:
            rc = ""
            for node in dom_element.childNodes:
                if node.nodeType == node.TEXT_NODE:
                    rc = rc + node.data
            return rc       

    def __tagSplit__(self, xmldoc, xmlList, tagList, baseList=[], idx=0, blank=False, iHTML=True, debug=False):
        d_list = []
        for x in self.__tagNme__(xmldoc, tagList=xmlList, iHTML=False):
            record = copy.copy(baseList)
            for y in tagList:
                if debug:
                    print "-------------------"
                    print x.toxml()
                    print y
                record.extend(self.__tagNme__(x, tagList=y, blank=blank, iHTML=iHTML, debug=debug, idx=idx))
            d_list.append(record)
        return d_list        

    def __tagNme__(self, xmldoc, tagList, idx=0, listType=False, blank=False, iHTML=True, debug=False):
        xmldoc = [xmldoc]
        for i,x in enumerate(tagList):
            if type(x) is not ListType:
                x = [x]
            xmlnext = []
            for y in x:
                for z in xmldoc:
                    if z != '':
                        if i==0 and idx!=0:
                            if len(z.getElementsByTagName(y))>0:
                                xmlnext.append(z.getElementsByTagName(y)[idx-1])
                            else:
                                xmlnext.append('')
                        else:
                            blFlag = False
                            for za in z.getElementsByTagName(y):
                                blFlag = True
                                xmlnext.append(za)
                            if blFlag==False and blank==True:
                                xmlnext.append('')
                    else:
                        xmlnext.append('')
            xmldoc = xmlnext

        if debug:
            if len(xmldoc)==1 and iHTML:
                print self.__innerHTML__(xmldoc[0])
            elif iHTML:
                print [self.__innerHTML__(x) for x in xmldoc]
            else:
                print xmldoc        

        if len(xmldoc)==1 and iHTML:
            if listType:
                return [self.__innerHTML__(xmldoc[0])]
            else:
                return self.__innerHTML__(xmldoc[0])                
        elif iHTML:
            return [self.__innerHTML__(x) for x in xmldoc]
        else:
            return xmldoc        

    def __asg_detail__(self, xmldoc):
        d_list = []
        for x in xmldoc:
            record = []
            if len(x.getElementsByTagName("first-name"))>0:
                record = [1]
                record.extend(self.__tagNme__(x, [["last-name", "first-name"]]))
            else:
                record = [0]
                record.extend(self.__tagNme__(x, [["orgname", "role"]]))
            record.extend(self.__tagNme__(x, ["addressbook", "address", ["street", "city", "state", "country", "postcode"]], blank=True))
            record.extend(self.__tagNme__(x, [["nationality", "residence"], "country"], blank=True))
            d_list.append(record)
        return d_list        

    def __cit_detail__(self, xmldoc):
        d_list = []
        for x in xmldoc:
            #this means patcit is part of the XML
            record = [self.__tagNme__(x, ["category"])]
            if len(x.getElementsByTagName("patcit"))>0:
                record.extend(self.__tagNme__(x, ["patcit", ["country", "doc-number", "date", "kind", "name"]], blank=True))
                record.extend([""])
            elif len(x.getElementsByTagName("othercit"))>0:
                record.extend(["", "", "", "", ""])
                record.extend([self.__allHTML__(x, ["othercit"])])
                #probably should grab date information
            else:
                print x.toxml()
            d_list.append(record)
        return d_list        

    def __rel_detail__(self, xmldoc, debug=False):
        d_list = []
        for x in xmldoc:
            for y in ["continuation-in-part", "continuation", "division", "reissue"]:
                if len(x.getElementsByTagName(y))>0:
                    d_list.extend(self.__tagSplit__(x, ["relation", "child-doc"], [[["doc-number", "country", "kind"]]], baseList=[y, -1], blank=True))
                    d_list.extend(self.__tagSplit__(x, ["relation", "parent-doc"], [[["doc-number", "country", "kind", "date", "parent-status"]]], baseList=[y, 1], blank=True, idx=1))
                    d_list.extend(self.__tagSplit__(x, ["relation", "parent-doc", "parent-grant-document"], [[["doc-number", "country", "kind", "date", "parent-status"]]], baseList=[y, 1], blank=True))
                    d_list.extend(self.__tagSplit__(x, ["relation", "parent-doc", "parent-pct-document"],  [[["doc-number", "country", "kind", "date", "parent-status"]]], baseList=[y, 1], blank=True))
            for y in ["related-publication", "us-provisional-application"]:
                if len(x.getElementsByTagName(y))>0:
                    d_list.extend(self.__tagSplit__(x, ["document-id"], [[["doc-number", "country", "kind"]]], baseList=[y, 0], blank=True))
            if debug:
                print "-------------------"
                for x in d_list:
                    print x
        return d_list

    def __repr__(self):
        return \
"""country = %s, patent = %s, pat_type = %s,
date_grant = %s, date_app = %s,
         abstract = %s
  invention_title = %s
len(classes, asg, cit, ret, inv, law) = %s""" % (self.country, self.patent, self.pat_type, self.date_grant, self.date_app,
       self.abstract[:50], self.invention_title[:50],
       str([len(self.classes),  len(self.asg_list), len(self.cit_list),
            len(self.rel_list), len(self.inv_list), len(self.law_list)]))
