import sys

import SQLite
import datetime
from senAdd import *
import locFunc 
import orgClean

debug = False

t1 = datetime.datetime.now()
print "Start", t1
##### Run B2_LocationMatch.py
import B2_LocationMatch
print "   - Loc Merge", "\n   -", datetime.datetime.now()-t1

##
## ###########################
#####                       ###
####     A S S I G N E E     ##
#####                       ###
## ###########################
##
### Create copy of assignee table, add column for assigneeAsc
s = SQLite.SQLite(db = 'assignee.sqlite3', tbl = 'assignee_1')
s.conn.create_function("ascit", 1, ascit)
s.conn.create_function("cc", 3, locFunc.cityctry)
s.attach(database = 'NBER_asg')
s.c.execute("DROP TABLE IF EXISTS assignee_1")
s.replicate(tableTo = 'assignee_1', table = 'assignee')
#s.addSQL(data='assignee', insert="IGNORE")
s.c.execute("INSERT INTO assignee_1 SELECT * FROM assignee %s" % (debug and "LIMIT 2500" or ""))
s.add('assigneeAsc', 'VARCHAR(30)')
s.c.execute("UPDATE assignee_1 SET assigneeAsc = ascit(assignee);")
s.commit()
print "DONE: assignee_1 table created in assignee.sqlite3 with new column assigneeAsc", "\n   -", datetime.datetime.now()-t1
s.merge(key=[['AsgNum', 'pdpass']], on=[['assigneeAsc', 'assignee']], keyType=['INTEGER'], tableFrom='main', db='db')
s.c.execute("UPDATE assignee_1 SET AsgNum=NULL WHERE AsgNum<0")
print"DONE: NBER pdpass added to assignee_1 in column AsgNum", "\n   -", datetime.datetime.now()-t1
s.commit()

### Run orgClean.py and generate grp
org = orgClean.orgClean(db = 'assignee.sqlite3', fld = 'assigneeAsc', table = 'assignee_1', other = "")
org.disambig()

print"DONE: orgClean"
print "   -", datetime.datetime.now()-t1

# Copy assignee num from grp to assignee table
s.merge(key=[['AsgNum', 'AsgNum2']], on=['AssigneeAsc'], tableFrom='grp')
print "DONE: Replaced Asgnum!", "\n   -", datetime.datetime.now()-t1
s.c.execute("""update assignee_1 set City = cc(city, country, 'city'), Country = cc(city, country, 'ctry');""")
s.attach('hashTbl.sqlite3')
s.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'], on=['City', 'State', 'Country'], tableFrom='locMerge', db='db')
s.commit()
print "DONE: Asg Locationize!", "\n   -", datetime.datetime.now()-t1
s.close()

 ###########################
###                       ###
##     I N V E N T O R     ##
###                       ###
 ###########################

## Clean inventor: ascit(Firstname, Lastname, Street)
## Create new table inventor_1 to hold prepped data

i = SQLite.SQLite(db = 'inventor.sqlite3', tbl = 'inventor_1')
i.conn.create_function("ascit", 1, ascit)
i.conn.create_function("cc", 3, locFunc.cityctry)
i.c.execute('drop table if exists inventor_1')
i.replicate(tableTo = 'inventor_1', table = 'inventor')
i.c.execute('insert or ignore into inventor_1 select * from inventor  %s' % (debug and "LIMIT 2500" or ""))

i.c.execute("""update inventor_1 set firstname = ascit(firstname), lastname = ascit(lastname), street = ascit(street), City = cc(city, country, 'city'), Country = cc(city, country, 'ctry');""")
i.commit()

i.attach('hashTbl.sqlite3')
i.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'], on=['City', 'State', 'Country'], tableFrom='locMerge', db='db')
i.merge(key=['NCity', 'NState', 'NCountry', 'NZipcode', 'NLat', 'NLong'], on=['City', 'State', 'Country', 'Zipcode'], tableFrom='locMerge', db='db')
i.commit()
i.close()
print "DONE: Inv Locationize!", "\n   -", datetime.datetime.now()-t1

 ###########################
###                       ###
##        C L A S S        ##
###                       ###
 ###########################

# Clean up classes
# see CleanDataSet.py --> classes()
from CleanDataset import *
classes()
print "DONE: Classes!", "\n   -", datetime.datetime.now()-t1

 ###########################
###                       ###
##       P A T E N T       ##
###                       ###
 ###########################

p = SQLite.SQLite(db = 'patent.sqlite3', tbl = 'patent')
p.conn.create_function('dVert', 1, dateVert)
p.c.execute("""update patent set AppDate=dVert(AppDate), GDate=dVert(GDate);""")
p.commit()
p.close()
print "DONE: Patent Date!", "\n   -", datetime.datetime.now()-t1
