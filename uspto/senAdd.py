import csv, re, sqlite3, types

#colors = [['Orange Red', '#FF4500'], ['Gold', '#FFD700'], ['Orange', '#FFA500'], ['Tomato', '#FF6347'], ['Deep Pink', '#FF1493'], ['Magenta', '#FF00FF'], ['Salmon', '#FA8072'], ['Sandy Brown', '#F4A460'], ['Khaki', '#F0E68C'], ['Light Cyan', '#E0FFFF'], ['Yellow Green', '#9ACD32'], ['Pale Green', '#98FB98'], ['Dark Violet', '#9400D3'], ['Medium Purple', '#9370DB'], ['Sky Blue', '#87CEEB'], ['Crimson', '#DC143C'], ['Olive', '#808000'], ['Slate Gray', '#708090'], ['Dim Gray', '#696969'], ['Dark Olive Green', '#556B2F'], ['Steel Blue', '#4682B4'], ['Aqua', '#00FFFF'], ['Lime', '#00FF00'], ['Deep Sky Blue', '#00BFFF'], ['Teal', '#008080'], ['Dark Green', '#006400'], ['Medium Blue', '#0000CD'], ['Spring Green', '#00FF7F'], ['Violet', '#EE82EE'], ['Light Blue', '#ADD8E6'], ['Light Grey', '#D3D3D3'], ['Light Pink', '#FFB6C1'], ['Light Salmon', '#FFA07A'], ['Light Steel Blue', '#B0C4DE']]
colors = [['Orange Red', '#FF4500'], ['Gold', '#FFD700'], ['Orange', '#FFA500'], ['Tomato', '#FF6347'], ['Deep Pink', '#FF1493'], ['Magenta', '#FF00FF'], ['Salmon', '#FA8072'], ['Sandy Brown', '#F4A460'], ['Khaki', '#F0E68C'], ['Yellow Green', '#9ACD32'], ['Pale Green', '#98FB98'], ['Dark Violet', '#9400D3'], ['Medium Purple', '#9370DB'], ['Crimson', '#DC143C'], ['Olive', '#808000'], ['Slate Gray', '#708090'], ['Dim Gray', '#696969'], ['Dark Olive Green', '#556B2F'], ['Steel Blue', '#4682B4'], ['Lime', '#00FF00'], ['Deep Sky Blue', '#00BFFF'], ['Teal', '#008080'], ['Dark Green', '#006400'], ['Medium Blue', '#0000CD'], ['Spring Green', '#00FF7F'], ['Light Grey', '#D3D3D3'], ['Light Pink', '#FFB6C1'], ['Light Salmon', '#FFA07A']]

def dateVert(text):
    text = str(text);
    #date structure like 1-Jan-00
    date = re.findall("([0-9]{1,2})[-]([A-Z]{3})[-]([0-9]{2})", text, re.I)
    if len(date)==1:
        mon = {"jan":"01", "feb":"02", "mar":"03", "apr":"04", "may":"05", "jun":"06", "jul":"07", "aug":"08", "sep":"09", "oct":"10", "nov":"11", "dec":"12"}
        yr = ((int)(date[0][2])>=50) and "19" or "20"; #assuming 50-99 is 1900s
        return "%s%s%s%0.2d" % (yr, date[0][2], mon[date[0][1].lower()], int(date[0][0]))
    else:    
        #date structure like 1/10/2001
        date = re.findall("(1[0-2]|0?[0-9])[/]([0-3]?[0-9])[/](1[7-9][0-9]{2}|20[0-9]{2})", text)
        if len(date)>0:
            date = [int(x) for x in date[0]]
        else:
            #date structure like 20010110
            if text.isdigit() and len(text)==8 :
                date = [int(text[4:6]), int(text[6:8]), int(text[0:4])]
        if len(date)==3:
            return "%0.4d-%0.2d-%0.2d" % (date[2], date[0], date[1])
        else:
            return text

def patSimple(patent):
    patent = str(patent)    
    import re
    pat = re.sub("[^A-Za-z0-9]", "", patent)
    letters = re.findall("[A-Za-z]", pat)
    letters = "".join(letters)
    digits = pat.replace(letters, "")
    if digits.isdigit():
        return '%s%d' % (letters, (int)(digits))
    else:
        return pat

def pat8char(patent):
    patent = str(patent)    
    import re
    pat = re.sub("[^A-Za-z0-9]", "", patent)
    letters = re.findall("[A-Za-z]", pat)
    letters = "".join(letters)
    digits = pat.replace(letters, "")
    if digits.isdigit():
        return '%s%s' % (letters, ("%%0.%dd" % (8-len(letters))) % ((int)(digits)))
    else:
        return pat

def patVert(patent, country):
    import re
    patent = str(patent)
    if country=="US":
        if patent.find("/")>0:
            return patent #application
        else:
            if len(patent)==8:
                return patent
            else:
                patTyp = re.findall("[A-Z]+", patent)
                patNum = re.findall("[0-9]+", patent)
                if len(patTyp)==0:
                    patTyp = [""]
                return "%s%s" % (patTyp[0], "%s%s" % ("0"*(8-len(patTyp[0])-len(patNum[0])), patNum[0]))
    else:
        return patent #foreign

def patType(text):
    text = str(text)
    typ = text[0]
    return typ.isdigit() and "U" or typ

def colGrad(col1="#000000", col2="#FF0000", pct=50):
    r = hex((int(col2[1:3], 16) - int(col1[1:3], 16))*pct/100 + int(col1[1:3], 16))[-2:]
    g = hex((int(col2[3:5], 16) - int(col1[3:5], 16))*pct/100 + int(col1[3:5], 16))[-2:]
    b = hex((int(col2[5:7], 16) - int(col1[5:7], 16))*pct/100 + int(col1[5:7], 16))[-2:]
    return ("#%s%s%s" % (r, g, b)).replace("x", "0")

def csvInput(fname, header=False):
    f = [x for x in csv.reader(open(fname, "rb"))]
    if header:
        return dict([[f[0][i], x] for i,x in enumerate(zip(*f[1:]))])
    else:
        return f
def csvOutput(data, fname="default.csv"):
    writer = csv.writer(open(fname, "wb"), lineterminator="\n")
    writer.writerows(data)
    writer = None

def ascit(x, strict=True):
    import unicodedata
    x = x.upper()
    #Solves that {UMLAUT OVER (A)}
    x = re.sub(r"[{].*?[(].*?[)].*?[}]", lambda(x):re.findall("[(](.*?)[)]", x.group())[0], x)
    #remove space(s) + punctuation
    x = re.sub(r" *?[,|-] *?", lambda(x):re.findall(r"[,|-]", x.group())[0], x)
    if strict:
        #remove stuff in between (), {}
        x = re.sub(r"[(].*?[)]|[{].*?[}]", "", x)
        #remove periods, ampersand, etc
        ##x = re.sub(r"[!@#$%^&*.,(){}]", "", x)
        x = re.sub(r"[^A-Za-z0-9 ]", " ", x)
    x = re.sub(r"  +", " ", x)
    
    #remove duplicates
    x = re.sub(r"[ ,|-]{2,}", lambda(x):re.findall(r"[ ,|-]", x.group())[0], x)
    #remove all unicode
    x = unicodedata.normalize('NFKD', unicode(x)).encode('ascii', 'ignore')
    return x.strip()

def uni2asc(x):
    import unicodedata
    return unicodedata.normalize('NFKD', unicode(x)).encode('ascii', 'ignore')


def jarow(s1,s2):
    try:
        s1 = s1.upper()
        s2 = s2.upper()
        if s1==s2:
            return 1.0
        if s1=="" or s2=="":
            return 0.0
        short, long = len(s1)>len(s2) and [s2, s1] or [s1, s2]
        for l in range(0, min(5, len(short))):
          if short[l] != long[l]:
            break
        mtch = ""
        mtch2=[]
        dist = len(long)/2-1
        m = 0.0
        for i, x in enumerate(long):
          jx, jy = (lambda x,y: x==y and (x,y+1) or (x,y))(max(0, i-dist), min(len(short), i+dist))
          for j in range(jx, jy):
            if j<len(short) and x == short[j]:
              m+=1
              mtch+=x
              mtch2.extend([[j,x]])
              short=short[:j]+"*"+short[min(len(short), j+1):]
              break
        mtch2 = "".join(x[1] for x in sorted(mtch2))
        t = 0.0
        for i in range(0, len(mtch)):      
          if mtch[i]!=mtch2[i]:
            t += 0.5
        
        d = 0.1 
        # this is the jaro-distance 
        if m==0:
          d_j = 0
        else:
          d_j = 1/3.0 * ((m/len(short)) + (m/len(long)) + ((m - t)/m))
        return d_j + (l * d * (1 - d_j))
    except:
        return 0
        
#SPECIFICALLY FOR SEN
def invComp(s1, s2):
    sX = re.split(r" ?[.,] ?", s1)
    sY = re.split(r" ?[.,] ?", s2)
    if len(sX)==2 and len(sY)==2:
        if sX[1].find(sY[1])>-1 or sY[1].find(sX[1])>-1:
            llen = [min(len(sX[i]), len(sY[i])) for i in range(0,2)]
            return sum([jarow(sX[i], sY[i])*llen[i] for i in range(0,2)])/sum(llen)
        else:
            return 0.0
    else:
        return jarow(s1, s2)

def invXMtch(st):
    sts = st.split("|")
    ok = False
    for i in range(0,len(sts)-1):
        for j in range(i+1, len(sts)):
            if invComp(sts[i], sts[j])==0.0:
                ok = True
                break
    return ok
            
#---------------------

class uQvals:
    def __init__(self):
        self.dList=[]
    def step(self, value):
        self.dList.append(value.upper())
    def finalize(self):
        out = list(set(self.dList))
        return len(out)>1 and "|".join(out) or ""

def quickSQL(c, data, table="", header=False):
    if table=="":
        table = "debug%d" % len([x[0] for x in c.execute("SELECT tbl_name FROM sqlite_master WHERE type='table' order by tbl_name") if len(re.findall(r"debug[0-9]+", x[0]))>0])
    vTp = {types.StringType:"VARCHAR", types.UnicodeType:"VARCHAR", types.IntType:"INTEGER", types.FloatType: "REAL"}
    if header==False:
        c.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (table, ", ".join(["v%d %s" % (i, vTp[type(x)]) for i,x in enumerate(data[0])])))
        c.executemany("INSERT INTO %s VALUES (%s)" % (table, ", ".join(["?"]*len(data[0]))), data)
    else:
        c.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (table, ", ".join(["%s %s" % (data[0][i], vTp[type(x)]) for i,x in enumerate(data[1])])))
        c.executemany("INSERT INTO %s VALUES (%s)" % (table, ", ".join(["?"]*len(data[0]))), data[1:])

def flatten(l, ltypes=(list, tuple)):
    ltype = type(l)
    l = list(l)
    i = 0
    while i < len(l):
        while isinstance(l[i], ltypes):
            if not l[i]:
                l.pop(i)
                i -= 1
                break
            else:
                l[i:i + 1] = l[i]
        i += 1
    return ltype(l)

def freq(ln):
    return sorted([[x, ln.count(x)] for x in set(ln)], reverse=True, key=lambda(x): x[1])
