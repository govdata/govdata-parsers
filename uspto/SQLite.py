def dbAdd(db):
    """
    I tire of this code...
    """
    return db!=None and "%s." % db or ""

class SQLite:
    """
    The following extends sqlite3, a commonly used library for the Patent Team
     - remember, in classes "self" refers to the class and is not really a paramater
     - table is an optional parameter for many of these functions.
       If the variable is not set, it defaults to the table set during initialization
    """
    def __init__(self, db=":memory:", tbl=None, table=None):
        """
        Initiate the database.
        Specify the location of the database and the table.
         - Automatically opens the connection
         - tbl and table are the same!
        """
        if tbl==None and table==None:
            tbl = "main"
        else:
            tbl = (tbl!=None) and tbl or table
        self.tbl = tbl
        self.db = db
        self.open()
    def optimize(self):
        """
        Some basic optimization stuff w/r http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html
        """
        self.c.executescript("""
            PRAGMA cache_size=2000000;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=2;
            """)
    def getTbl(self, table=None):
        if table==None:
            return self.tbl
        else:
            return table
    def chgTbl(self, table):
        """
        Not too complex.  Allows you to change the default table
        """
        self.tbl = table
    def commit(self):
        """
        Simply because .conn.commit() is too much work for my simple mind.
        This is often necessary after execute statements.
        Commit the data into the table/database.
        """
        self.conn.commit()
    def open(self):
        """
        Opens the connection.
        """
        import sqlite3
        self.conn = sqlite3.connect(self.db)
        self.c = self.conn.cursor()
    def close(self):
        """
        Close the connection. (The database is locked as the sqlite3 API is used)
        """
        self.commit()
        self.c.close()
        self.conn.close()
    def attach(self, database, name="db"):
        """
        Attaches a database and defaults it as db.
        """
        try:
            self.c.execute("ATTACH DATABASE '%s' AS %s" % (database, name))
        except:
            self.c.execute("DETACH DATABASE %s" % (name))
            self.c.execute("ATTACH DATABASE '%s' AS %s" % (database, name))            
    def detach(self, name="db"):
        """
        Detaches a database (default, db).
        """
        self.c.execute("DETACH DATABASE %s" % (name))
    def vacuum(self, name="db"):
        """
        Vacuum the database.
        (When indices and table are added and dropped, the database stays the same size.)
        """
        self.c.execute("vacuum")
        self.commit()
    def csvInput(self, fname):
        def decode(lst):
            import unicodedata
            try:
                return [x.decode("iso-8859-1") for x in lst]
            except:
                print lst
                return lst
        import csv
        f = open(fname, "rb")
        t = [decode(x) for x in csv.reader(f)]
        f.close()
        return t
    def tables(self, lookup=None, db=None):
        """
        Returns a list of table names that exist within the database.
        If lookup is specified, does that table exist within the list of tables?
        """
        retList = [x[0].lower() for x in self.c.execute("SELECT tbl_name FROM %ssqlite_master WHERE type='table' ORDER BY tbl_name" % dbAdd(db))]
        if lookup==None:
            return retList
        else:
            return lookup.lower() in retList
    def indexes(self, lookup=None, db=None, search=None):
        """
        Returns a list of index names that exist within the database
        If lookup is specified, does that index exist within the list of indexes?
        If search is specified, returns a list of indices that match the criteria
        """
        import re
        retList = [x[0].lower() for x in self.c.execute("SELECT name FROM %ssqlite_master WHERE type='index' ORDER BY name" % dbAdd(db))]

        if search!=None: #returns a range of numbers related to the search term
            nums = [(lambda x:len(x)>0 and int(x[0]) or 1)(re.findall('[0-9]+', x)) for x in retList if x.find(search)>=0]
            if len(nums)==0:
                return [0, 0]
            else:
                return [min(nums), max(nums)]
        elif lookup!=None:
            return lookup.lower() in retList
        else:
            return retList
        
    def count(self, table=None):
        """
        I like these basic reports where I am curious about the size of the default table.
        And for kicks- why not throw in the current time.
        """
        import datetime
        table = self.getTbl(table)
        if self.tables(lookup=table):
            cnt = self.c.execute("SELECT count(*) FROM %s" % table).fetchone()[0]
            print datetime.datetime.now(), cnt
            return cnt
        else:
            print datetime.datetime.now()
            return 0
    def addSQL(self, data, db=None, table=None, header=False, field=True, insert=""):
        """
        This serves three functions depending the type of data (flat CSV, pure data, existing table)
        If data is a link to a database -- load the data into CSV
        
         - If data = table name, use the data as a base
           - If table doesn't exist, replicate ELSE Insert
         - Else
           - If data = filename (CSV) ... Generate table using quickSQL (header toggle is for this one)
         - ElseIf data = data, sounds good...!
           - Insert data

        Field=True, defaults that field names must match 1-1
        """
        import types
        table = self.getTbl(table)
        if type(data)==types.StringType and not self.tables(db=db, lookup=table):
            data = self.csvInput(data)
        if insert!="":
            insert = "OR %s" % insert

        if type(data)==types.StringType and self.tables(db=db, lookup=data):
            if self.tables(db=db, lookup=table):
                self.replicate(tableTo=table, table=data, db=db)
            if field:
                fieldTo = set(self.columns(table=table, output=False, lower=True))
                fieldFr = set(self.columns(table=data, db=db, output=False, lower=True))
                colList = ", ".join(list(fieldTo & fieldFr))
                self.c.execute("INSERT %s INTO %s (%s) SELECT %s FROM %s%s" % (insert, table, colList, colList, dbAdd(db), data))
            else:
                self.c.execute("INSERT %s INTO %s SELECT * FROM %s%s" % (insert, table, dbAdd(db), data))
                
        #if table exists ... just add data into it
        elif self.tables(db=db, lookup=table):
            self.c.executemany("INSERT %s INTO %s VALUES (%s)" % (insert, table, ", ".join(["?"]*len(data[0]))), data[int(header):])
        else:
            self.quickSQL(data, table=table, header=header)
            #need to make this so the variables are more flexible
        self.conn.commit()
    def quickSQL(self, data, table=None, override=False, header=False, typescan=50, typeList=[]):
        import re, types
        table = self.getTbl(table)
        if override:
            self.c.execute("DROP TABLE IF EXISTS %s" % table)
        elif self.tables(db=None, lookup=table):
            return

        if header:
            headLst = []
            for x in data[0]:
                headLst.append(re.sub("[()!@#$%^&*'-]+", "", x).replace(" ", "_").replace("?", ""))
                if headLst[-1] in headLst[:-1]:
                    headLst[-1]+=str(headLst[:-1].count(headLst[-1])+1)
        tList = []
        for i,x in enumerate(data[1]):
            if str(typeList).upper().find("%s " % data[0][i].upper())<0:
                cType = {types.StringType:"VARCHAR", types.UnicodeType:"VARCHAR", types.IntType:"INTEGER", types.FloatType: "REAL", types.NoneType: "VARCHAR"}[type(x)]
                if type(typescan)==types.IntType and cType=="VARCHAR":
                    least = 2
                    ints = 1
                    for j in range(1, min(typescan+1, len(data))):
                        if type(data[j][i])==types.StringType or type(data[j][i])==types.UnicodeType:
                            if re.sub(r"[-,.]", "", data[j][i]).isdigit():
                                if len(re.findall(r"[.]", data[j][i]))==0:   pass
                                elif len(re.findall(r"[.]", data[j][i]))==1: ints = 0
                                else: least = 0; break
                            else: least = 0; break
                    cType = {0:"VARCHAR", 1:"INTEGER", 2:"REAL"}[max(least-ints, 0)]
                if header:
                    tList.append("%s %s" % (headLst[i], cType))
                else:
                    tList.append("v%d %s" % (i, cType))
            else:
                tList.extend([y for y in typeList if y.upper().find("%s " % data[0][i].upper())==0])

        #print("CREATE TABLE IF NOT EXISTS %s (%s)" % (table, ", ".join(tList)))
        self.c.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (table, ", ".join(tList)))
        if header==False:
            self.c.executemany("INSERT INTO %s VALUES (%s)" % (table, ", ".join(["?"]*len(data[0]))), data)
        else:
            self.c.executemany("INSERT INTO %s VALUES (%s)" % (table, ", ".join(["?"]*len(data[0]))), data[1:])
        self.conn.commit()            
    def columns(self, table=None, output=True, db=None, lower=False):
        table = self.getTbl(table)
        if output:
            for x in self.c.execute("PRAGMA %sTABLE_INFO(%s)" % (dbAdd(db), table)):
                print x
        else:
            return [lower and x[1].lower() or x[1] for x in self.c.execute("PRAGMA %sTABLE_INFO(%s)" % (dbAdd(db), table)).fetchall()]
    def drop(self, keys, table=None): #drop columns -- doesn't exist so lame!
        import types
        table = self.getTbl(table)
        if type(keys)!=types.ListType:
            keys = [keys]
        cols = ", ".join([x for x in self.columns(output=False) if x.lower() not in [y.lower() for y in keys]])
        self.c.executescript("""
            DROP TABLE IF EXISTS %s_backup;
            ALTER TABLE %s RENAME TO %s_backup;
            """ % (table, table, table))
        self.c.execute("CREATE TABLE %s (%s)" % (table, ", ".join([" ".join([x[1], x[2]]) for x in self.c.execute("PRAGMA TABLE_INFO(%s_backup)" % table) if x[1].lower() not in [y.lower() for y in keys]])))
        self.replicate(tableTo=table, table="%s_backup" % table)
        self.c.execute("INSERT INTO %s SELECT %s FROM %s_backup" % (table, cols, table))
        self.c.execute("DROP TABLE %s_backup" % (table))
    def add(self, key, typ, table=None):
        table = self.getTbl(table)
        try:
            self.c.execute("ALTER TABLE %s ADD COLUMN %s %s" % (table, key, typ))
        except:
            x=0
    
    def delete(self, table=None): #delete table
        table = self.getTbl(table)
        self.c.execute("DROP TABLE IF EXISTS %s" % table)
        self.conn.commit()
    def replicate(self, tableTo=None, table=None, db=None):
        """
        Replicates the basic structure of another table
        """
        import re
        #replicate the structure of a table
        table = self.getTbl(table)
        if db==None:
            if tableTo==None: #THIS ALLOWS US TO AUTOMATICALLY ADD TABLES
                tableTo = [(lambda x:len(x)>0 and int(x[0]) or 1)(re.findall('[0-9]+', x)) for x in self.tables() if x.find(table.lower())>=0]
                tableTo = len(tableTo)==0 and table or "%s%d" % (table, max(tableTo)+1)
        else:
            tableTo = table
        sqls = self.c.execute("""
          SELECT  sql, name, type
            FROM  %ssqlite_master
           WHERE  tbl_name='%s';""" % (dbAdd(db), table)).fetchall()

        idxC = 0
        idxA = self.baseIndex()
        idxR = self.indexes(search='idx_idx')
        def cleanTbl(wrd):
            wrd = re.sub(re.compile('create table ["\']?%s["\']?' % table, re.I), 'create table %s' % tableTo, wrd)
            return wrd
        def cleanIdx(wrd, name, newname):
            wrd = re.sub(re.compile(' on ["\']?%s["\']?' % table, re.I), ' on %s' % tableTo, wrd)
            wrd = re.sub(re.compile('INDEX %s ON' % name, re.I), 'INDEX %s ON' % newname, wrd)
            return self.baseIndex(idx=wrd) not in idxA and wrd or "";
        for x in sqls:
            try:
                if x[2]=='table':
                    self.c.execute(cleanTbl(x[0]))
                elif 'index':
                    idxC+=1
                    if idxC>=idxR[0] and idxC<=idxR[1]:
                        idxC = idxR[1]+1
                    self.c.execute(cleanIdx(x[0], x[1], "idx_idx%d" % idxC))
            except:
                y=0
    def baseIndex(self, idx=None, db=None):
        """
        Boils down a Index to its most basic form.
        Throw in an idx (string) to process that specific SQL.
        """
        import re
        if idx==None:
            sqls = self.c.execute("SELECT sql FROM %ssqlite_master WHERE type='index';" % (dbAdd(db))).fetchall()
        else:
            sqls = [[idx,]]
        #simplify the list
        idxLst = [re.sub("  +", " ", re.sub(", ", ",", re.sub(re.compile('INDEX .*? ON', re.I), 'INDEX ON', x[0]))).lower() for x in sqls if x[0]!=None]
        #reorders the keys (so sequentiality matters!)
        idxLst = [re.sub("[(].*?[)]", "(%s)" % ",".join(sorted(re.findall("[(](.*?)[)]", x)[0].split(','))), x) for x in idxLst] 
        if idx==None:
            return idxLst
        else:
            return idxLst[0]
        
    def index(self, keys, index=None, table=None, db=None, unique=False):
        """
        Hey Amy!  Look, documentation
        Index is for index name

        Indicates if Index is created with Index name or None
        """
        import re
        table = self.getTbl(table)
        if index==None: 
            index = [(lambda x:len(x)>0 and int(x[0]) or 1)(re.findall('[0-9]+', x)) for x in self.indexes(db=db) if x.find('idx_idx')>=0]
            index = len(index)==0 and "idx_idx" or "idx_idx%d" % (max(index)+1)

        #only create indexes if its necessary!  (it doens't already exist)
        idxA = self.baseIndex()
        idxSQL = "CREATE %sINDEX %s%s ON %s (%s)" % (unique and "UNIQUE " or "", dbAdd(db), index, table, ",".join(keys))
        if self.baseIndex(idx=idxSQL, db=db) not in idxA:
            self.c.execute(idxSQL)
            return "%s%s" % (dbAdd(db), index)
        else:
            return None

    #----- MERGE -----#

    def merge(self, key, on, tableFrom, keyType=None, table=None, db=None):
        """
        Matches the on variables from two tables and updates the key values

        Example of usage: (its on the table perspective, so that's first)
        On and Keys take an iterable with values of string or list:

        ie.
        key = ["ed", ["eric", "amy"]]
        on = ["ron", ["ron1", "amy"]]
        keyType = ['VARCHAR', 'VARCHAR'] #if nothing will just be blanks

        All together:

        .add('ed', 'VARCHAR')
        .add('eric', 'VARCHAR')

        c.executemany("UPDATE table SET ed=?, eric=? WHERE ron=? AND ron1=?",
            c.execute("SELECT b.ed, b.amy, b.ron, b.amy
                         FROM table AS a INNER JOIN tableFrom AS b
                           ON a.ron=b.ron AND a.ron1=b.amy").fetchall())       
        """
        import types, datetime
        table = self.getTbl(table)
        key = [type(x)==types.StringType and [x,x] or x for x in key]
        on = [type(x)==types.StringType and [x,x] or x for x in on]

        for i,x in enumerate(key):
            self.add(x[0], keyType!=None and keyType[i] or "", table=table)

        def huggleMe(lst, idx=0, head="", tail="", inner=", "):
            return head+("%s%s%s" % (tail, inner, head)).join([x[idx] for x in lst])+tail

        idxT = self.index(keys=[x[0] for x in on], table=table)
        idxF = self.index(keys=[x[1] for x in on], table=tableFrom, db=db)

        self.c.executescript("""
            DROP TABLE IF EXISTS TblA;
            DROP TABLE IF EXISTS TblB;
            CREATE TEMPORARY TABLE TblA AS SELECT %s FROM %s GROUP BY %s;
            CREATE TEMPORARY TABLE TblB AS SELECT %s, %s FROM %s%s GROUP BY %s;
            """ % (huggleMe(on), table, huggleMe(on),
                   huggleMe(key, idx=1), huggleMe(on, idx=1), dbAdd(db), tableFrom, huggleMe(on, idx=1)))
        self.index(keys=[x[0] for x in on], table="TblA", index='idx_temp_TblA')
        self.index(keys=[x[1] for x in on], table="TblB", index='idx_temp_TblB')
        
        sqlS = "UPDATE %s SET %s WHERE %s" % (table, huggleMe(key, tail="=?"), huggleMe(on, tail="=?", inner=" AND "))
##        sqlV = "SELECT %s, %s FROM %s AS a INNER JOIN %s%s AS b ON %s" % (
##            huggleMe(key, idx=1, head="b."), huggleMe(on, idx=1, head="b."),
##            table, dbAdd(db), tableFrom,
##            " AND ".join(["a."+"=b.".join(x) for x in on]))
        sqlV = "SELECT %s, %s FROM TblA AS a INNER JOIN TblB AS b ON %s" % (
            huggleMe(key, idx=1, head="b."), huggleMe(on, idx=1, head="b."),
            " AND ".join(["a."+"=b.".join(x) for x in on]))
        vals = self.c.execute(sqlV).fetchall()
        if len(vals)>0:
            self.c.executemany(sqlS, vals)

        #remove indices that we just temporarily created
        for x in [idxT, idxF]:
            if x!=None:
                self.c.execute("DROP INDEX %s" % x)
        
    #----- OUTPUTS -----#

    def csv_output(self, fname="default.csv", table=None):
        """
            Exports data into a CSV which is defaulted to "default.csv"
        """
        import unicodedata
        def asc(val):
            return [unicodedata.normalize('NFKD', unicode(x)).encode('ascii', 'ignore') for x in val]

        import csv
        table = self.getTbl(table)
        f = open(fname, "wb")
        writer = csv.writer(f, lineterminator="\n")
        writer.writerows([self.columns(table, output=False)])
        writer.writerows([asc(x) for x in self.c.execute("SELECT * FROM %s" % table).fetchall()])
        writer = None
        f.close()

    def mysql_output(self, cfg={'host':'localhost', 'passwd':'root', 'user':'root', 'db':'RD'}, textList=[], intList=[], varList=[], tableTo=None, table=None, full=True):
        """
        Output table into MySQL database.
        Auto converts fields TEXT, VARCHAR, and [Blank] to VARCHAR(255)
        Add additional text field names by using the textList
            (useful for the incorrect fields) tableTo is the MySQL table
        Add additional integer field names by using the intList
        Add additional varList.  This allows you to specify whatever you want.
            Format: [["name", "format"]]
        
        Full = True (input data)
        """
        textList = [x.lower() for x in textList]
        intList = [x.lower() for x in intList]

        if varList!=[]:
            varList = zip(*varList)
            varList[0] = [x.lower() for x in varList[0]]
        
        import MySQLdb, re, types, unicodedata
        table = self.getTbl(table)
        if tableTo == None:
            tableTo = table
        def field(name, type):
            name = name.lower()
            type = type.lower()

            if varList!=[] and name in varList[0]:
                return varList[1][varList[0].index(name)]
            if name in textList:
                return "VARCHAR(64)";
            elif name in intList:
                return "INTEGER";
            elif type.find("varchar")>=0 or type=="text" or type=="":
                return "VARCHAR(64)";
            elif type.find("int")>=0:
                return "INTEGER";
            elif type.find("real")>=0:
                return "REAL";
            else:
                return type
        
        mconn = MySQLdb.connect(host=cfg['host'], user=cfg['user'], passwd=cfg['passwd'], db=cfg['db'])
        mc = mconn.cursor()
        #get column and types for fields
        cols = ["`%s` %s" % (x[1], field(x[1], x[2])) for x in self.c.execute("PRAGMA TABLE_INFO(%s)" % table)]
        sql = "CREATE TABLE %s (%s);" % (tableTo, ", ".join(cols))

        try:
            mc.execute(sql)
        except:
            y=0
        indexes = [x[0] for x in self.c.execute("SELECT sql FROM sqlite_master WHERE type='index' and tbl_name='%s'" % table)]
        for idx in indexes:
            idx = idx.lower()
            idx = idx.replace('on %s (' % table.lower(),
                              'on %s (' % tableTo)
            try:
                #print idx
                mc.execute(idx)
            except:
                y=0
        if full:
            vals = [x for x in self.c.execute("SELECT * FROM %s" % table)]
            for i,val in enumerate(vals):
                #this is done to normalize the data
                insert = [(type(x)==types.UnicodeType or type(x)==types.StringType) and
                          unicodedata.normalize('NFKD', unicode(x)).encode('ascii', 'ignore') or x for x in val]
                try:
                    mc.execute("INSERT IGNORE INTO %s VALUES (%s)" % (tableTo, ", ".join(["%s"]*len(cols))), insert)
                except:
                    print i+1,val
        
        mc.close()
        mconn.close()

    """
    EXPERIMENTAL FUNCTIONS
    """
    # IGRAPH / VISUALIZATION RELATED FUNCTIONS, very very preliminary

    def igraph(self, where, table=None,
                 vx="Invnum_N", ed="Patent", order="AppYear",
                 va=", Lastname||', '||Firstname AS Name, City||'-'||State||'-'||Country AS Loc, Assignee, AsgNum",
                 ea=", a.AppYear AS AppYear", eg=', a.AppYear'):
        import math, datetime, senGraph
        table = self.getTbl(table)
        tab = senGraph.senTab()
        self.c.executescript("""
            DROP TABLE IF EXISTS G0;
            DROP TABLE IF EXISTS vx0;
            DROP TABLE IF EXISTS ed0;
            CREATE TEMP TABLE G0 AS
                SELECT * FROM %s WHERE %s ORDER BY %s;
            CREATE INDEX G_id ON G0 (%s);
            CREATE INDEX G_ed ON G0 (%s, %s);
            CREATE TEMPORARY TABLE vx0 AS
                SELECT %s, count(*) AS Patents %s FROM G0
                 GROUP BY %s;
            CREATE INDEX vx_id ON vx0 (%s);
            CREATE TEMPORARY TABLE ed0 AS
                SELECT  a.%s, b.%s, a.%s AS hId, b.%s AS tId, count(*) AS Weight %s
                  FROM  G0 AS a INNER JOIN G0 AS b
                    ON  a.%s=b.%s AND a.%s<b.%s
              GROUP BY  a.%s, b.%s %s;
            """ % (table, where, order, ed, vx, ed, vx, va, vx, vx,
                   vx, vx, vx, vx, ea, ed, ed, vx, vx, vx, vx, eg))

        tab.vList = self.c.execute("SELECT * FROM vx0").fetchall()
        tab.vlst = self.columns(table="vx0", output=False)[1:]
        tab.eList = self.c.execute("SELECT * FROM ed0").fetchall()
        tab.elst = self.columns(table="ed0", output=False)[2:]
        s = senGraph.senGraph(tab, "vertex")
        return s        
