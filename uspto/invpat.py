import SQLite
import datetime


t1 = datetime.datetime.now()
print "Start", t1

##Create invpat
ip = SQLite.SQLite(db = 'invpat.sqlite3', tbl = 'invpat')
ip.c.execute("DROP TABLE IF EXISTS invpat")
ip.c.execute("""CREATE TABLE invpat(Firstname TEXT, Lastname TEXT, Street TEXT,
            City TEXT, State TEXT, Country TEXT, Zipcode TEXT, Lat REAL,
            Lng REAL, InvSeq INT, Patent TEXT, AppYear TEXT, GYear INT,
            AppDate TEXT, Assignee TEXT, AsgNum INT, Class TEXT, Invnum,
            Invnum_N TEXT);""")

##From inventor.sqlite3: Firstname, Lastname, Street, City, State, Country, Zipcode, Lat, Lng, InvSeq
ip.attach('inventor.sqlite3')
ip.c.execute("""INSERT INTO invpat (Firstname, Lastname, Street, City, State, Country, Zipcode, Lat, Lng, Patent, InvSeq)
                SELECT Firstname, Lastname, Street, NCity, NState, NCountry, NZipcode, NLat, NLong, Patent, InvSeq FROM db.inventor_1""")
ip.detach()

##From patent.sqlite3: Patent, AppYear, GYear, AppDate
ip.attach('patent.sqlite3')
ip.merge(key = ['AppYear', 'GYear', 'AppDate'], on= ['Patent'], tableFrom = 'patent', db = 'db')
ip.detach()

##From assignee.sqlite3: Assignee, AsgNum
ip.attach('assignee.sqlite3')
ip.merge(key = [['Assignee', 'assigneeAsc'], 'AsgNum'], on = ['Patent'], tableFrom = 'assignee_1', db = 'db')
ip.detach()

##From class: class
ip.attach('class_1.sqlite3')
ip.merge(key = [['Class', 'ClassSub']], on = ['Patent'], tableFrom = 'class_1', db = 'db')
ip.detach()
ip.commit()

##Generate invnum
ip.c.execute("UPDATE invpat SET invnum = patent || '-' || invseq")
ip.c.execute("UPDATE invpat SET Invnum_N = Invnum")
ip.commit()

##Index invpat
ip.c.execute("CREATE INDEX asg on invpat (Assignee);")
ip.c.execute("CREATE INDEX asg2 on invpat (AsgNum);")
ip.c.execute("CREATE INDEX gyr on invpat (Gyear);")
ip.c.execute("CREATE INDEX iNidx  ON invpat (Invnum_N);")
ip.c.execute("CREATE INDEX locc on invpat (City);")
ip.c.execute("CREATE INDEX loccs on invpat (City, State);")
ip.c.execute("CREATE INDEX locs on invpat (State);")
ip.c.execute("CREATE INDEX pdx ON invpat (Patent);")
ip.c.execute("CREATE INDEX pidx ON invpat (Patent, InvSeq);")

ip.commit()

ip.close()

print "Finish", datetime.datetime.now()-t1

