from patXML import SQLPatent
from patXML import XMLPatent
from patXML import uniasc
from fwork  import *
import os, datetime, re

flder='xml'
t1 = datetime.datetime.now()

#get a listing of all files within the directory that follow the naming pattern
files = [x for x in os.listdir(flder)
         if re.match(r"ip[a-z]{2}[0-9]{6,8}[.]xml", x, re.I)!=None]
print "Total files: %d" % (len(files))

tblList = ["assignee", "citation", "class", "inventor", "patent", "patdesc", "lawyer", "sciref", "usreldoc"]
for filenum, filename in enumerate(files):    
    print " > Regular Expression: %s" % filename
    XMLs = re.findall(
            r"""
                ([<][?]xml[ ]version.*?[>]       #all XML starts with ?xml
                .*?
                [<][/]us[-]patent[-]grant[>])    #and here is the end tag
             """,
            open(flder+"/"+files[filenum]).read(), re.I + re.S + re.X)
    print "   - Total Patents: %d" % (len(XMLs))

    xmllist = []
    for i, x in enumerate(XMLs):
        try:
            xmllist.append(XMLPatent(x))
        except:
            print "  - Error: %s (%d)  %s" % (filename, i, x[175:200])
    print "   - number of patents:", len(xmllist), datetime.datetime.now()-t1

    for x in tblList:
        SQLPatent().dbBuild(q=SQLPatent().tblBuild(xmllist, tbl=x), tbl=x, week=filename)
    print "   -", datetime.datetime.now()-t1


for x in tblList:
    SQLPatent().dbFinal(tbl=x)
