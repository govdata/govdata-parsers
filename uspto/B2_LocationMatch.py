#Non_US http://earth-info.nga.mil/gns/html/gis_countryfiles.htm
#US     http://geonames.usgs.gov/domestic/download_data.htm

##NEED TO DO...
##    CREATE INDEX IF NOT EXISTS idx_ctc0 ON gnsloc (SORT_NAME, CC1);

import datetime, csv, os, re, sqlite3
from fwork import *

def sep_wrd(word, seq):
    if seq==-1:
        return word
    else:
        p = re.compile(" *?[,|] *")
        ln = p.split(word)
        if len(ln)> seq:
            return ln[seq]
        else:
            return ""

conn = sqlite3.connect("hashTbl.sqlite3")
c = conn.cursor()

conn.create_function("blk_split", 1, lambda x: re.sub(" ", "", x))
conn.create_function("sep_cnt",   1, lambda x: len(re.findall("[,|]", x)))
conn.create_function("jarow",     2, jarow)
conn.create_function("cityctry",  3, cityctry)
conn.create_function("sep_wrd",   2, sep_wrd)
conn.create_function("rev_wrd",   2, lambda x,y:x.upper()[::-1][:y])

c.executescript("""
    PRAGMA CACHE_SIZE=20000;

    ATTACH DATABASE 'assignee.sqlite3' AS asg;
    ATTACH DATABASE 'inventor.sqlite3' AS inv;
    ATTACH DATABASE 'loctbl.sqlite3'   AS loc;
    """)

c.executescript("""
 /* DROP TABLE IF EXISTS loc; */
    CREATE TABLE IF NOT EXISTS loc (
        Cnt INTEGER,
        City VARCHAR(10),   State VARCHAR(2),
        Country VARCHAR(2), Zipcode VARCHAR(5),
        City3 VARCHAR,

        NCity VARCHAR(10),  NState VARCHAR(2),
        NCountry VARCHAR(2), 
        UNIQUE(City,State,Country,Zipcode));
    DROP INDEX IF EXISTS loc_idxCC;
    DROP INDEX IF EXISTS loc_idx;
    DROP INDEX IF EXISTS loc_idxCS;
    DROP INDEX IF EXISTS loc_ixnCC;
    DROP INDEX IF EXISTS loc_ixn;
    DROP INDEX IF EXISTS loc_ixnCS;
    DROP INDEX IF EXISTS loc3_idxCC;
    """)

if not(tblExist(c, "locMerge")):
    print datetime.datetime.now()
    c.executescript("""
        CREATE TEMPORARY TABLE temp AS 
            SELECT  Upper(City) as CityX, Upper(State) as StateX,
                    Upper(Country) as CountryX, count(*) as Cnt
              FROM  asg.assignee
             WHERE  City!=""
          GROUP BY  CityX, StateX, CountryX;
        CREATE TEMPORARY TABLE temp2 AS
            SELECT  sum(Cnt) as Cnt,
                    cityctry(CityX, CountryX, 'city') as CityY, StateX as StateY,
                    cityctry(CityX, CountryX, 'ctry') as CtryY, '' as ZipcodeY
              FROM  temp
             WHERE  CityY!=""
          GROUP BY  CityY, StateY, CtryY;
        INSERT OR REPLACE INTO loc 
            SELECT  a.*, SUBSTR(CityY,1,3), b.NewCity, b.NewState, b.NewCountry
              FROM  temp2 AS a
         LEFT JOIN  loc.typos AS b
                ON  a.CityY=b.City AND a.StateY=b.State AND a.CtryY=b.Country;
        DROP TABLE  temp;
        DROP TABLE  temp2;                
        """)

    print datetime.datetime.now()
    c.executescript("""
        CREATE TEMPORARY TABLE temp AS 
            SELECT  Upper(City) as CityX, Upper(State) as StateX,
                    Upper(Country) as CountryX, Zipcode, count(*) as Cnt
              FROM  inv.inventor
             WHERE  City!="" OR (City="" AND Zipcode!="")
          GROUP BY  CityX, StateX, CountryX, Zipcode;
        CREATE TEMPORARY TABLE temp2 AS
            SELECT  sum(Cnt) as Cnt,
                    cityctry(CityX, CountryX, 'city') as CityY, StateX as StateY,
                    cityctry(CityX, CountryX, 'ctry') as CtryY, Zipcode as ZipcodeY
              FROM  temp
             WHERE  CityY!=""
          GROUP BY  CityY, StateY, CtryY, ZipcodeY;
        INSERT OR REPLACE INTO loc 
            SELECT  a.*, SUBSTR(CityY,1,3), b.NewCity, b.NewState, b.NewCountry
              FROM  temp2 AS a
         LEFT JOIN  loc.typos AS b
                ON  a.CityY=b.City AND a.StateY=b.State AND a.CtryY=b.Country;
        DROP TABLE  temp;
        DROP TABLE  temp2;                
        """)

    c.executescript("""
        CREATE INDEX IF NOT EXISTS loc_idCC3 ON loc (City3,State,Country);
        CREATE INDEX IF NOT EXISTS loc_idxCC ON loc (City,Country);
        CREATE INDEX IF NOT EXISTS loc_idx   ON loc (City,State,Country,Zipcode);
        CREATE INDEX IF NOT EXISTS loc_idxCS ON loc (City,State);
        CREATE INDEX IF NOT EXISTS loc_ixnCC ON loc (NCity,NCountry);
        CREATE INDEX IF NOT EXISTS loc_ixn   ON loc (NCity,NState,NCountry);
        CREATE INDEX IF NOT EXISTS loc_ixnCS ON loc (NCity,NState);
        """)

    print datetime.datetime.now()

c.executescript("""
    CREATE TABLE IF NOT EXISTS usloc AS
        SELECT  Zipcode, Lat, Long, Upper(City) as City,
                BLK_SPLIT(Upper(City)) as BlkCity,
                SUBSTR(UPPER(BLK_SPLIT(City)),1,3) as City3,
                REV_WRD(BLK_SPLIT(City), 4) as City4R,
                Upper(State) as State, "US" as Country
          FROM  loc.usloc
      GROUP BY  City, State;
    CREATE INDEX If NOT EXISTS usloc_idxZ  on usloc (Zipcode);
    CREATE INDEX If NOT EXISTS usloc_idxCS on usloc (City, State);
    CREATE INDEX If NOT EXISTS usloc_idBCS on usloc (BlkCity, State);
    CREATE INDEX If NOT EXISTS usloc_idC3S on usloc (City3, State);
    CREATE INDEX If NOT EXISTS usloc_idC4R on usloc (City4R, State);
    DETACH DATABASE asg;
    DETACH DATABASE inv;
    /*DETACH DATABASE loc;
    CREATE TEMPORARY TABLE gnsloc AS
        SELECT  '' AS zipcode, lat, long,
                UPPER(full_name_nd) AS city, "" AS State, cc1 AS country
          FROM  loc.gnsloc;
    CREATE INDEX gnsloc_idxCC on gnsloc (City, Country)
    */;
    """)
print datetime.datetime.now()

c.executescript("""
    CREATE TABLE IF NOT EXISTS locMerge (
        Mtch INTEGER,
        Val FLOAT,          Cnt INTEGER,
        
        City VARCHAR,       State VARCHAR,
        Country VARCHAR,    Zipcode VARCHAR,

        NCity VARCHAR,     NState VARCHAR,
        NCountry VARCHAR,  NZipcode VARCHAR,
        NLat FLOAT,        NLong FLOAT,
        City3 VARCHAR,
        UNIQUE(City, State, Country, Zipcode));
    CREATE INDEX IF NOT EXISTS okM_idxCC ON locMerge (City,Country);
    CREATE INDEX IF NOT EXISTS okM_idx   ON locMerge (City,State,Country,Zipcode);
    CREATE INDEX IF NOT EXISTS okM_idxCS ON locMerge (City,State);
    CREATE INDEX IF NOT EXISTS okM_idx3  ON locMerge (City3,State,Country);
    """)

def replace_loc(script):
    #ALLOWS US TO REPLACE THE PREV LOC DATASET
    c.executescript("""
        DROP TABLE IF EXISTS temp1;
        CREATE TEMPORARY TABLE temp1 AS %s;
        CREATE INDEX IF NOT EXISTS tmp1_idx ON temp1 (CityA, StateA, CountryA, ZipcodeA);;
        """ % script)
    field = ["[%s]" % x[1] for x in c.execute("PRAGMA TABLE_INFO(temp1)")][2:6]
    var_f = ",".join(field)

    if c.execute("SELECT count(*) FROM temp1").fetchone()[0]>0:
        c.executescript("""
            CREATE TEMPORARY TABLE temp2 AS
                SELECT  count(*) as cnt, CityA, StateA, CountryA, ZipcodeA
                  FROM  temp1
              GROUP BY  CityA, StateA, CountryA, ZipcodeA;
            CREATE INDEX IF NOT EXISTS t1_idx ON temp1 (CityA, StateA, CountryA, ZipcodeA);
            CREATE INDEX IF NOT EXISTS t2_idx ON temp2 (CityA, StateA, CountryA, ZipcodeA); 
            INSERT OR REPLACE INTO locMerge
                SELECT  b.cnt, a.*, SUBSTR(a.CityA,1,3)
                  FROM  temp1 AS a
            INNER JOIN  temp2 AS b
                    ON  a.CityA=b.CityA AND a.StateA=b.StateA AND a.CountryA=b.CountryA AND a.ZipcodeA=b.ZipcodeA;
            CREATE TEMPORARY TABLE temp3 AS
                SELECT  a.*
                  FROM  LOC AS a LEFT JOIN locMerge AS b
                    ON  a.City=b.City AND a.State=b.State AND a.Country=b.Country AND a.Zipcode=b.Zipcode
                 WHERE  b.Zipcode IS NULL;
            DROP TABLE IF EXISTS loc;
            CREATE TABLE loc AS SELECT * FROM temp3;
            CREATE INDEX IF NOT EXISTS loc_idxCC ON loc (City, Country);
            CREATE INDEX IF NOT EXISTS loc_idx   ON loc (City, State, Country, Zipcode);
            CREATE INDEX IF NOT EXISTS loc_idxCS ON loc (City, State);
            DROP TABLE IF EXISTS temp2;
            DROP TABLE IF EXISTS temp3;
              """)
        VarX = c.execute("select count(*) from loc").fetchone()[0]
        VarY = c.execute("select count(*) from locMerge").fetchone()[0]
        print " - Loc =", VarX, " OkM =", VarY, " Total =", VarX+VarY, "  ", datetime.datetime.now()
    conn.commit()

print "Loc =", c.execute("select count(*) from loc").fetchone()[0]
for scnt in range(-1, c.execute("select max(sep_cnt(city)) from loc").fetchone()[0]+1):
    sep = scnt
    print "------", scnt, "------"
    
    ##DOMESTIC
    replace_loc("""
        SELECT  11,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.city, b.state, 'US', b.zipcode, b.lat, b.long
          FROM  loc AS a INNER JOIN usloc AS b
            ON  SEP_WRD(CityA, %d)=b.city AND StateA=b.state AND CountryA='US'
         WHERE  SEP_CNT(CityA)>=%d AND CityA!="";
        """ % (sep, scnt))

    ##DOMESTIC (Blk Remove)
    replace_loc("""
        SELECT  11,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.city, b.state, 'US', b.zipcode, b.lat, b.long
          FROM  loc AS a INNER JOIN usloc AS b
            ON  BLK_SPLIT(SEP_WRD(a.City, %d))=b.blkcity AND a.state=b.state AND a.country='US'
         WHERE  SEP_CNT(a.City)>=%d AND a.City!=""
        """ % (sep, scnt))

    ##DOMESTIC FIRST3 (JARO WINKLER)
    replace_loc("""
        SELECT  (10+jarow(BLK_SPLIT(SEP_WRD(a.City, %d)), b.BlkCity)) AS Jaro,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.city, b.state, 'US', b.zipcode, b.lat, b.long
          FROM  loc AS a INNER JOIN usloc AS b
            ON  SUBSTR(BLK_SPLIT(SEP_WRD(a.City, %d)),1,3)=b.City3 AND a.state=b.state AND a.country='US'
         WHERE  jaro>%s AND SEP_CNT(a.City)>=%d AND a.City!=""
      ORDER BY  a.City, a.State, jaro
        """ % (sep, sep, "10.92", scnt))

    ##DOMESTIC LAST4 (JARO WINKLER)
    replace_loc("""
        SELECT  (10+jarow(BLK_SPLIT(SEP_WRD(a.City, %d)), b.BlkCity)) AS Jaro,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.city, b.state, 'US', b.zipcode, b.lat, b.long
          FROM  loc AS a INNER JOIN usloc AS b
            ON  REV_WRD(BLK_SPLIT(SEP_WRD(a.City, %d)),4)=b.City4R AND a.state=b.state AND a.country='US'
         WHERE  jaro>%s AND SEP_CNT(a.City)>=%d AND a.City!=""
      ORDER BY  a.City, a.State, jaro
        """ % (sep, sep, "10.90", scnt))

    #------------------------------------------#

    ##FOREIGN COUNTRY (Full Name 1)
    replace_loc("""
        SELECT  21,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.full_name_nd, "", b.cc1, "", b.lat, b.long
          FROM  loc AS a INNER JOIN loc.gnsloc AS b
            ON  SEP_WRD(a.City, %d)=b.full_name AND a.country=b.cc1
         WHERE  SEP_CNT(a.City)>=%d AND a.City!=""
        """ % (sep, scnt))

    ##FOREIGN COUNTRY (Full Name 2)
    replace_loc("""
        SELECT  21,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.full_name_nd, "", b.cc1, "", b.lat, b.long
          FROM  loc AS a INNER JOIN loc.gnsloc AS b
            ON  SEP_WRD(a.City, %d)=b.full_name_nd AND a.country=b.cc1
         WHERE  SEP_CNT(a.City)>=%d AND a.City!="";
        """ % (sep, scnt))

    ##FOREIGN COUNTRY (Short Form)
    replace_loc("""
        SELECT  21,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.full_name_nd, "", b.cc1, "", b.lat, b.long
          FROM  loc AS a INNER JOIN loc.gnsloc AS b
            ON  SEP_WRD(a.City, %d)=b.short_form AND a.country=b.cc1
         WHERE  SEP_CNT(a.City)>=%d AND a.City!="";
        """ % (sep, scnt))

    ##FOREIGN COUNTRY (Blk Split)
    replace_loc("""
        SELECT  21,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.full_name_nd, "", b.cc1, "", b.lat, b.long
          FROM  loc AS a INNER JOIN loc.gnsloc AS b
            ON  BLK_SPLIT(SEP_WRD(a.City, %d))=b.sort_name AND a.country=b.cc1
         WHERE  SEP_CNT(a.City)>=%d AND a.City!="";
        """ % (sep, scnt))

    ##FOREIGN COUNTRY FIRST3 (JARO WINKLER)
    replace_loc("""
        SELECT  (20+jarow(BLK_SPLIT(SEP_WRD(a.City, %d)), b.sort_name)) AS Jaro,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.full_name_nd, "", b.cc1, "", b.lat, b.long
          FROM  loc AS a INNER JOIN loc.gnsloc AS b
            ON  SUBSTR(BLK_SPLIT(SEP_WRD(a.City, %d)),1,3)=b.sort_name3 AND a.country=b.cc1
         WHERE  jaro>%s AND SEP_CNT(a.City)>=%d AND a.City!=""
      ORDER BY  a.City, a.Country, jaro;
        """ % (sep, sep, "20.92", scnt))

    ##FOREIGN COUNTRY LAST4 (JARO WINKLER)
    replace_loc("""
        SELECT  (20+jarow(BLK_SPLIT(SEP_WRD(a.City, %d)), b.sort_name)) AS Jaro,
                a.cnt as cnt, 
                a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
                b.full_name_nd, "", b.cc1, "", b.lat, b.long
          FROM  loc AS a INNER JOIN loc.gnsloc AS b
            ON  REV_WRD(BLK_SPLIT(SEP_WRD(a.City, %d)),4)=b.sort_name4R AND a.country=b.cc1
         WHERE  jaro>%s AND SEP_CNT(a.City)>=%d AND a.City!=""
      ORDER BY  a.City, a.Country, jaro;
        """ % (sep, sep, "20.90", scnt))

####    ##DOMESTIC (State miscode to Country)
####    replace_loc("""
####        SELECT  31,
####                a.cnt, a.city, a.state, a.country, a.zipcode,
####                b.city, b.state, 'US', b.zipcode, b.lat, b.long
####          FROM  loc AS a INNER JOIN usloc AS b
####            ON  SEP_WRD(a.City, %d)=b.city AND a.country=b.state
####         WHERE  SEP_CNT(a.City)>=%d AND a.City!="";
####        """ % (sep, scnt))

print "------ F ------"
##DOMESTIC (2nd LAYER)
replace_loc("""
    SELECT  15,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.city, b.state, 'US', b.zipcode, b.lat, b.long
      FROM  (SELECT  *
               FROM  loc
              WHERE  NCity IS NOT NULL) AS a
INNER JOIN  usloc AS b
        ON  a.NCity=b.city AND a.NState=b.state AND a.NCountry='US';
    """)

##DOMESTIC FIRST3 (2nd, JARO WINKLER)
replace_loc("""
    SELECT  14+jarow(BLK_SPLIT(a.NCity), b.BlkCity) AS Jaro,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.city, b.state, 'US', b.zipcode, b.lat, b.long
      FROM  (SELECT  *
               FROM  loc
              WHERE  NCity IS NOT NULL) AS a
INNER JOIN  usloc AS b
        ON  SUBSTR(BLK_SPLIT(a.NCity),1,3)=b.City3 AND a.Nstate=b.state AND a.Ncountry='US'
     WHERE  jaro>%s
  ORDER BY  a.NCity, a.NState, jaro
    """ % "14.95")

##FOREIGN FULL NAME (2nd LAYER)
replace_loc("""
    SELECT  25,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.full_name_nd, '' as state, b.cc1, '' as zip, b.lat, b.long
      FROM  (SELECT  *
               FROM  loc
              WHERE  NCity IS NOT NULL) AS a
INNER JOIN  loc.gnsloc AS b
        ON  a.NCity=b.full_name AND a.NCountry=b.cc1;
    """)

##FOREIGN FULL ND (2nd LAYER)
replace_loc("""
    SELECT  25,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.full_name_nd, '' as state, b.cc1, '' as zip, b.lat, b.long
      FROM  (SELECT  *
               FROM  loc
              WHERE  NCity IS NOT NULL) AS a
INNER JOIN  loc.gnsloc AS b
        ON  a.NCity=b.full_name_nd AND a.NCountry=b.cc1;
    """)

##FOREIGN NO SPACE (2nd LAYER)
replace_loc("""
    SELECT  25,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.full_name_nd, '' as state, b.cc1, '' as zip, b.lat, b.long
      FROM  (SELECT  *
               FROM  loc
              WHERE  NCity IS NOT NULL) AS a
INNER JOIN  loc.gnsloc AS b
        ON  BLK_SPLIT(a.NCity)=b.sort_name AND a.NCountry=b.cc1;
    """)

##FOREIGN COUNTRY FIRST3 (2nd, JARO WINKLER)
replace_loc("""
    SELECT  24+jarow(BLK_SPLIT(a.NCity), b.sort_name) AS Jaro,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.full_name_nd, '' as state, b.cc1, '' as zip, b.lat, b.long
      FROM  (SELECT  *
               FROM  loc
              WHERE  NCity IS NOT NULL) AS a
INNER JOIN  loc.gnsloc AS b
        ON  SUBSTR(BLK_SPLIT(a.NCity),1,3)=b.sort_name3 AND a.Ncountry=b.cc1
     WHERE  jaro>%s 
  ORDER BY  a.NCity, a.NCountry, jaro;
    """ % "24.95")

##DOMESTIC ZIPCODE
replace_loc("""
    SELECT  31,
            a.cnt as cnt, 
            a.city as CityA, a.state as StateA, a.country as CountryA, a.zipcode as ZipcodeA,
            b.City, b.State, 'US', b.zipcode, b.lat, b.long
      FROM  (SELECT  *, (SEP_WRD(zipcode,0)+0) as Zip2
               FROM  loc
              WHERE  Zipcode!='' AND Country='US') AS a
              INNER JOIN usloc AS b
        ON  a.Zip2=b.Zipcode;
    """)

##MISSING JARO (FIRST 3)
#replace_loc("""
#    SELECT  30+jarow(a.City, b.City) AS Jaro,
#            a.cnt, a.city, a.state, a.country, a.zipcode,
#            b.ncity, b.nstate, b.ncountry, b.nzipcode, b.nlat, b.nlong
#      FROM  loc AS a INNER JOIN locMerge AS b
#        ON  a.City3=b.City3 AND a.state=b.state AND a.country=b.country
#     WHERE  jaro>%s AND a.City!=""
#  ORDER BY  a.City, a.State, a.Country, jaro;
#    """ % ("30.95"))

conn.commit()
c.close()
conn.close()
