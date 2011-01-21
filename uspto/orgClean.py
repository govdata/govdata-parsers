import sys

sys.path.append("/home/ron/PythonBase")
from SQLite import *
from senAdd import *
import datetime
import scipy
import scipy.stats


#SET UP DATABASE - Prep a 'AdsgNum' in the beginning
class orgClean():
    def __init__(self,
                 db="asg2.sqlite3", fld="Assignee", uqKey="AsgNum",
                 other="NCity, NState, NCountry,", table="Assignee_2"):

        self.fld = fld
        self.uqKey = uqKey
        self.other = other
        self.table = table
        self.s = SQLite(db)

    def disambig(self):
        import datetime
        s = self.s
        s.open()

        fld = self.fld
        uqKey = self.uqKey
        other = self.other
        table = self.table
        
        s.conn.create_function("jarow", 2, jarow)
        s.c.executescript("""
            
                DROP TABLE IF EXISTS grp;
                DROP TABLE IF EXISTS wrd;
            
            CREATE TABLE IF NOT EXISTS grp AS
                SELECT  0 AS Mtch,
                        %s, '' AS %s2,
                        '' AS %s3, '' AS Block1, '' AS Block2, '' AS Block3,
                        min(%s) AS %s,
                        '' AS %s2,
                        '' AS nFreqS, '' AS nFreqB,
                        %s count(*) AS cnt
                  FROM  %s
              GROUP BY  %s
                HAVING  %s!=""
              ORDER BY  cnt DESC;

            CREATE UNIQUE INDEX IF NOT EXISTS aGasg ON grp (%s);
            CREATE INDEX IF NOT EXISTS aGaN2 ON grp (%s2);
            CREATE INDEX IF NOT EXISTS Bl1 ON grp (Block1);
            CREATE INDEX IF NOT EXISTS Bl2 ON grp (Block2);
            CREATE TABLE IF NOT EXISTS wrd (
                word TEXT, word1 TEXT, word_N TEXT, count INTEGER, countF INTEGER);
            """ % (fld, fld, fld, uqKey, uqKey, uqKey, other, table, fld, fld, fld, uqKey))

        if s.c.execute("SELECT count(*) FROM wrd").fetchone()[0]==0:
            print datetime.datetime.now()
            sing = s.c.execute("SELECT %s, cnt FROM grp" % fld).fetchall()

            singDct = {}
            for x in sing:
                for y in x[0].split(" "):
                    if y not in singDct:
                        singDct[y] = [y, y[:2], '', 0, 0]
                    singDct[y][3] += 1
                    singDct[y][4] += x[1]

            s.c.executemany("INSERT INTO wrd VALUES (?, ?, ?, ?, ?)", singDct.values())
            s.c.executescript("""
                CREATE INDEX IF NOT EXISTS aWw  ON wrd (word);
                CREATE INDEX IF NOT EXISTS aWw3 ON wrd (word1);
                """)
            s.conn.commit()

            #CREATE HASH TABLE FOR BLANKS
            cnts = [x[0] for x in s.c.execute("SELECT count FROM wrd").fetchall()]
            dev3 = scipy.std(cnts)*3+scipy.mean(cnts)

            s.c.executescript("""
                CREATE TEMPORARY TABLE wrdTop AS
                    SELECT * FROM wrd WHERE count>%f;
                CREATE INDEX aWTw3 ON wrdTop (word1);
                CREATE TEMPORARY TABLE IF NOT EXISTS wrdMtch AS
                    SELECT  *
                      FROM (SELECT  a.word AS wordA, b.word as wordB, b.count, jarow(a.word, b.word) jaro
                              FROM  wrd AS a
                        INNER JOIN  wrdTop AS b
                                ON  a.word1=b.word1 and a.count<b.count and a.count<%f
                                    and length(a.word)>=5
                             WHERE  jaro>0.95
                          ORDER BY  jaro)
                  GROUP BY  wordA;
                """ % (dev3, dev3))

            #JUST TOP ONES
            blankDct = dict([x for x in s.c.execute("SELECT wordA, count FROM wrdMtch")])
            for x in s.c.execute("SELECT word, count FROM wrdTop"):
                blankDct[x[0]] = x[1]

            #ALL
            allDct = dict([[x[0], x[1:]] for x in s.c.execute("SELECT word, count, countF FROM wrd")])
            # count = number of times word occurs in each unique assignee occurence
            # countF = sum of total frequency of occurrence (i.e. sum of frequencies of each assignee containing word
            print datetime.datetime.now()

            #replace word, but make if everything gets killed.. use the low frequency item
            def wordPlace(word):
                newWord = []
                lowFreq = None
                lowWord = ""
                words = True
                for x in word.split(" "):
                    if x.isdigit():
                        newWord.append(x)
                    elif x not in blankDct:
                        words = False
                        newWord.append(x)
                    elif lowFreq==None or blankDct[x]<lowFreq:
                        lowFreq = blankDct[x]
                        lowWord = x
                    #if blank keep lowest freq

                if words:
                    newWord.append(lowWord)
                wordStr = " ".join(newWord)
                if wordStr>10:
                    return wordStr
                else:
                    return word

            def backWrds(word, trimword, minlength=25, minwords=2):
                if len(word)>=minlength and len(word.split(" "))>=minwords:
                    return word[::-1][:minlength]
                else:
                    return word
                
            def wordOrder(word):
                newWord = []
                for x in word.split(" "):
                    if x in blankDct:
                        newWord.append([blankDct[x], x])
                    elif x in allDct:
                        newWord.append([allDct[x][0], x])
                newWord.sort()
                for i in range(0, len(newWord)/2):
                    if newWord[-1][0] > dev3:
                        newWord.pop()
                wordStr = " ".join([x[1] for x in newWord])
                if wordStr>10:
                    return wordStr
                else:
                    return word

            def GenAsg(asg2, asg, row):
                #If asg2 exists, return
                if asg2!="":
                    return asg2
                #If asg exists per NBER return theirs
                elif asg!=None and asg!="":
                    return "A%0.12d" % int(asg)
                #Harvard Generated
                else:
                    return "H%0.12d" % int(row)

            def letters(word, lets=2, mult=2):
                newWord = [x for x in word.split(" ") if len(x)>=mult*lets]
                if len(newWord)>=8/lets:
                    return "".join([x[:lets] for x in newWord[:8/lets]])
                else:
                    return word

            def wordFreq(word, idx=0):
                return " ".join(str(allDct[x][idx]) for x in word.split(" "))

            s.conn.create_function("bWrds", 2, backWrds)
            s.conn.create_function("wFreq", 2, wordFreq)
            s.conn.create_function("letty",  1, letters)
            s.conn.create_function("wordP",  1, wordPlace)
            s.conn.create_function("wordO",  1, wordOrder)
            s.conn.create_function("GenAsg", 3, GenAsg)
            s.conn.create_function("blank",  1, lambda x: re.sub(" +", "", x))
            s.c.executescript("""
                UPDATE  grp
                   SET  %s2=wordP(%s),
                        %s3=wordO(%s),
                        Block1=blank(%s),
                        Block2=letty(%s),
                        nFreqS=wFreq(%s, 0),
                        nFreqB=wFreq(%s, 1);
                UPDATE  grp
                   SET  Block3=bWrds(%s, %s2),
                        %s2=GenAsg(%s2, %s, ROWID);
                CREATE INDEX IF NOT EXISTS Bl1c ON grp (Block1, %s2);
                CREATE INDEX IF NOT EXISTS Bl2c ON grp (Block2, %s2);
                CREATE INDEX IF NOT EXISTS Bl3c ON grp (%s2, %s2);
                CREATE INDEX IF NOT EXISTS Bl4c ON grp (%s3, %s2);
                CREATE INDEX IF NOT EXISTS B_3c ON grp (Block3, %s2);
                """ % (fld, fld, fld, fld, fld, fld, fld, fld, fld, fld,
                       uqKey, uqKey, uqKey, uqKey, uqKey,
                       fld, uqKey, fld, uqKey, uqKey))

        #GROUP ORDER MAKES A DIFFERENCE -- LAST ONE IS CAPTURED
        print datetime.datetime.now()

        def repMatch(string):
            #NEED TO HAVE Jaro1, Jaro2
            while True:
                s.c.executescript("""
                    DROP TABLE IF EXISTS temp;
                    DROP TABLE IF EXISTS temp2;
                    CREATE TEMPORARY TABLE IF NOT EXISTS temp  AS %s;
                    CREATE TEMPORARY TABLE IF NOT EXISTS temp2 AS
                        SELECT *, Max(Jaro1, Jaro2) AS Jaro FROM temp;
                    CREATE INDEX IF NOT EXISTS tAN2  ON temp2 (AAsgNum2);
                    CREATE INDEX IF NOT EXISTS tAN2j ON temp2 (AAsgNum2, Jaro);
                    """ % string)
                AsgRep = s.c.execute("SELECT BAsgNum2, AAsgNum2 FROM temp2 GROUP BY AAsgNum2 ORDER BY AAsgNum2 DESC, Jaro").fetchall()
                s.c.executemany("UPDATE grp SET %s2=? WHERE %s2=?" % (uqKey, uqKey), AsgRep)
                print datetime.datetime.now(), len(AsgRep)
                if len(AsgRep)==0:
                    break
                s.conn.commit()

        #Exact Match for common words
        repMatch("""
                SELECT  a.%s2 AS AAsgNum2, b.%s2 AS BAsgNum2,
                        1 AS Jaro1, 1 AS Jaro2
                  FROM  grp AS a
            INNER JOIN  grp AS b
                    ON  a.%s3=b.%s3 AND
                        a.%s2 > b.%s2""" % (uqKey, uqKey, fld, fld, uqKey, uqKey))
        #Close Match for removed words
        repMatch("""
                SELECT  jarow(a.%s,  b.%s)  AS Jaro1,
                        jarow(a.%s3, b.%s3) AS Jaro2,
                        a.%s2 AS AAsgNum2, b.%s2 AS BAsgNum2
                  FROM  grp AS a
            INNER JOIN  grp AS b
                    ON  a.%s2=b.%s2 AND
                        a.%s2 > b.%s2
                 WHERE  Jaro1 > 0.95 OR  Jaro2 > 0.95 OR
                       (Jaro1 > 0.90 AND Jaro2 > 0.90)""" % (fld, fld, fld, fld, uqKey, uqKey, fld, fld, uqKey, uqKey))
        #Blanking
        repMatch("""
                SELECT  jarow(a.%s,  b.%s)  AS Jaro1,
                        jarow(a.%s3, b.%s3) AS Jaro2,
                        a.%s2 AS AAsgNum2, b.%s2 AS BAsgNum2
                  FROM  grp AS a
            INNER JOIN  grp AS b
                    ON  a.Block1=b.Block1 AND
                        a.%s2 > b.%s2
                 WHERE  Jaro1 > 0.95 OR  Jaro2 > 0.95 OR
                       (Jaro1 > 0.90 AND Jaro2 > 0.90)""" % (fld, fld, fld, fld, uqKey, uqKey, uqKey, uqKey))
        #First two letters of multiple words
        repMatch("""
                SELECT  jarow(a.%s,  b.%s)  AS Jaro1,
                        jarow(a.%s3, b.%s3) AS Jaro2,
                        a.%s2 AS AAsgNum2, b.%s2 AS BAsgNum2
                  FROM  grp AS a
            INNER JOIN  grp AS b
                    ON  a.Block2=b.Block2 AND
                        a.%s2 > b.%s2
                 WHERE  Jaro1 > 0.95 OR  Jaro2 > 0.95 OR
                       (Jaro1 > 0.90 AND Jaro2 > 0.90)""" % (fld, fld, fld, fld, uqKey, uqKey, uqKey, uqKey))
        #Reverse the first word to see if words overlap
        s.conn.create_function("backC", 2, lambda x,y: max(x.find(' '+y), y.find(' '+x)))
        repMatch("""
                SELECT  jarow(a.%s,  b.%s)  AS Jaro1,
                        jarow(a.%s3, b.%s3) AS Jaro2,
                        a.%s2 AS AAsgNum2, b.%s2 AS BAsgNum2
                  FROM  grp AS a
            INNER JOIN  grp AS b
                    ON  a.Block3=b.Block3 AND
                        a.%s2 > b.%s2
                 WHERE  Jaro1 > 0.95 OR  Jaro2 > 0.95 OR
                       (Jaro1 > 0.90 AND Jaro2 > 0.90)""" % (fld, fld, fld, fld, uqKey, uqKey, uqKey, uqKey))
        s.conn.commit()
        s.close()

    def top(self, cnt=20, fname=None):
        s = self.s
        s.open()
        if fname!=None:
            f = open(fname, "wb")
        top = s.c.execute("SELECT %s2,count(*) AS cnt FROM grp GROUP BY %s2 ORDER BY cnt DESC" % (self.uqKey, self.uqKey)).fetchall()[:cnt]
        for x in top:
            for y in s.c.execute("SELECT cnt,%s FROM grp WHERE %s2='%s' GROUP BY %s ORDER BY cnt DESC" % (self.fld, self.uqKey, x[0], self.fld)):
                if fname!=None:
                    f.write(str(y)+"\n")
                else:
                    print y
            if fname!=None:
                f.write("-------------------------\n")
            else:
                print "-------------------------"
        if fname!=None:
            f.close()    
        s.close()

    def merge(self, keys, db=None, tbl="main"):
        s = self.s
        s.open()
        if len(keys[0])<13:
            keys = ["%s%0.12d" % (x[0], int(x[1:])) for x in keys]
            
        k1 = min(keys)
        for k in keys:
            s.c.execute("UPDATE grp SET %s2='%s' WHERE %s2='%s'" % (self.uqKey, k1, self.uqKey, k))
        s.conn.commit()
        s.close()
        if db!=None:
            t = SQLite(db)
            for k in keys:
                t.c.execute("UPDATE %s SET %s='%s' WHERE %s='%s'" % (tbl, self.uqKey, k1, self.uqKey, k))
            t.conn.commit()
            t.close()
    
    def find(self, key, mode=0):
        s = self.s
        s.open()
        if mode==0: #search for key
            for x in s.c.execute("SELECT %s2,count(*),sum(cnt) AS cnt,%s FROM grp WHERE %s LIKE '%%%s%%' GROUP BY %s2 ORDER BY cnt DESC" % (self.uqKey, self.fld, self.fld, key, self.uqKey)):
                print x
        elif mode==1: #search for specifc
            if len(key)<13:
                key = "%s%0.12d" % (key[0], int(key[1:]))
            for x in s.c.execute("SELECT cnt,%s FROM grp WHERE %s2='%s' GROUP BY %s ORDER BY cnt DESC" % (self.fld, self.uqKey, key, self.fld)):
                print x
        s.close()

    def setKey(self, db, table="main"):
        s = self.s
        s.open()
        OrgDct = dict(s.c.execute("SELECT %s, %s2 FROM grp" % (self.fld, self.uqKey)).fetchall())
        s.close()
        t = SQLite(db)
        def OrgDctIt(x):
            if x in OrgDct:
                return OrgDct[x]
            else:
                return ""
        t.conn.create_function("OrgDct", 1, OrgDctIt)
        t.c.execute("UPDATE %s SET %s=OrgDct(%s)" % (table, self.uqKey, self.fld))
        t.conn.commit()
        t.close()
        
    def setNames(self):
        s = self.s
        s.open()
        self.name = dict(s.c.execute("SELECT %s2, %s FROM grp ORDER BY cnt" % (self.uqKey, self.fld)).fetchall())
        self.name[''] = ''
        s.close()

    def fetchNames(self, key):
        try:
            x = self.name
        except:
            self.setNames()
        if len(key)<13 and len(key)>1:
            key = "%s%0.12d" % (key[0], int(key[1:]))
        return key in self.name and self.name[key] or ""
        
###Normal Run
##import orgClean
##org = orgClean.orgClean()
##org.disambig()
##org.top(20, "asg2.txt")
##org.setKey("invpat.sqlite3", "invpat")




##s = SQLite("asg2.sqlite3")
##grp = s.c.execute("SELECT asgnum2,assignee FROM grp").fetchall()
##t = SQLite("invpat.sqlite3")
##t.open()
##t.c.executemany("UPDATE invpat SET asgnum=? WHERE assignee=?", grp)
##t.close()


