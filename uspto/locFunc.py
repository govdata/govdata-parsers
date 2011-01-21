import re, types, unicodedata

def jarow(s1,s2):
    if s1==s2:
        return 1.0
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

def uniasc(x):
    if type(x)==types.IntType or type(x)==types.FloatType:
        return x
    else:
        x = x.upper()
        #Solves that {UMLAUT OVER (A)}
        x = re.sub(r"[{].*?[(].*?[)].*?[}]", lambda(x):re.findall("[(](.*?)[)]", x.group())[0], x)
        #remove international zips (eg. A-12358, A12358, 12358A)
        x = re.sub(r"\b[A-Z]{1,5}[-]{0,2}[0-9]+\b|\b[0-9]+?[-]{0,2}[A-Z]{1,5}\b", "", x)
        x = re.sub(r"\b[A-Z]{1,5} ?[-]{1,2} ?[0-9]+\b|\b[0-9]+? ?[-]{1,2} ?[A-Z]{1,5}\b", "", x)
        #remove all numbers
        x = re.sub(r"[0-9]", "", x)
        #remove TOWN OF, ALL OF, etc
        x = re.sub(r"\b[A-Z]+?[,]? +?[I]?OF ", "", x)
        #remove stuff in parathesis
        x = re.sub(r"[(].*?[)]", "", x)
        #remove basic period punctuation, replace with nothing
        x = re.sub(r"[.()`'#\"&]|[-]{2,}|[/][-]", "", x)
        #remove space(s) + punctuation
        x = re.sub(r" *?[,|-] *?", lambda(x):re.findall(r"[,|-]", x.group())[0], x)
        #remove duplicates
        x = re.sub(r"[ ,|-]{2,}", lambda(x):re.findall(r"[ ,|-]", x.group())[0], x)
        #remove leading [,|-]
        x = re.sub(r"^[,|-]", "", x)
        #remove trailing [,|-]
        x = (lambda(x):len(x)>len(re.sub(r"[,|-]", " ", x).strip()))(x) and x[:-1] or x
        #remove all unicode
        x = unicodedata.normalize('NFKD', unicode(x)).encode('ascii', 'ignore')
        return x

def cityctry(city, ctry, ret="city"):

##    z = city
    ctry = ctry.upper()
    city = uniasc(city.upper())

##    if z != city:
##        print z, city, ctry
    
    if ret=="ctry":
        ##CZECH REPUBLIC
        if ctry=="CS":
            ctry = "CZ"
        ##CHINA
        elif ctry=="CN":
            if re.search(r"\b(HONG KONG|HK)\b", city)!=None:
                ctry="HK"
            elif re.search(r"\b(TAIWAN|TW)\b", city)!=None:
                ctry="TW"
        ##RUSSIA
        elif ctry=="SU":
            ctry = "RU"
        ##USA
        elif ctry=="USA":
            ctry = "US"
        ##YUGOSLAVIA
        elif ctry=="YI" and re.search(r"\bBELGRAD\b", city)!=None:
            ctry = "YU"
        elif ctry=="HR" and re.search(r"\bZAGREB\b", city)!=None:
            ctry = "YU"
        elif ctry=="SI" and re.search(r"\bLJUBLJANA\b", city)!=None:
            ctry = "YU"
        return ctry.strip()

    elif ret=="city":
        prov = []
        ##DIFFERENT COUNTRIES
        if ctry in ("BR", "CN", "JP", "RU"):
            city = re.sub(r"\bCITY[ ,]\b", "", city)
        if ctry != "US":
            city = re.sub(r"\bST\b", "SAINT", city)

        ##ARGENTINA
        if ctry=="AR":
            city = re.sub("(CIUDAD|PROVINCIA)( DE)?|[A-Z ]+? DE ", "", city)
        ##AUSTRALIA
        elif ctry=="AU":
            city = re.sub(r"\bMTN\b", "MOUNTAIN", city)
            city = re.sub(r"\bMT\b", "MOUNT", city)
            ##6 provinces and 2 territories
            prov = [["^A ?C ?T|[, ]A ?C ?T|AUSTRALIAN( CAPITAL TERRITORY)?", "AUSTRALIAN CAPITAL TERRITORY"], ["^N ?T|[, ]N ?T|NORTHERN TERRITORY", "NORTHERN TERRITORY"], ["^N ?S ?W|[ ,]N ?S ?W|NEW SOUTH WALES", "NEW SOUTH WALES"], ["^Q ?L ?D|[, ]Q ?L ?D|QUEENSLAND", "QUEENSLAND"], ["^S ?A|[, ]S ?A|SOUTH AUSTRALIA", "SOUTH AUSTRALIA"], ["^W ?A|[ ,]W ?A|WESTERN AUSTRALIA", "WESTERN AUSTRALIA"], ["^V ?I ?C|[, ]V ?I ?C|V ?I ?C(TORIA)?", "VICTORIA"], ["^T ?A ?S|[, ]T ?A ?S|T ?A ?S(MANIA)?", "TASMANIA"]]
        ##AUSTRIA
        elif ctry=="AT":
            #is this a good one to keep?
            city = re.sub(r"(, ?|[/])OSTERREICH", "", city)
            city = re.sub(r"[/]| ?AN DER ?", ",", city)
        ##BELGIUM
        elif ctry=="BE":
            city = re.sub(r"[/]| ?SUR ?", ",", city)
        ##CANADA
        elif ctry=="CA":
            #13 provinces
            prov = [["^A ?B|[, ]A ?B|ALBERTA", "ALBERTA"], ["^M ?B|[, ]M ?B|MANITOBA", "MANITOBA"], ["^N ?B|[, ]N ?B|NEW BRUNSWICK", "NEW BRUNSWICK"], ["^N ?L|[, ]N ?L|NEWFOUNDLAND", "NEWFOUNDLAND"], ["^N ?T|[, ]N ?T|NORTHWEST TERR(ITORIES)?", "NORTHWEST TERRITORIES"], ["^N ?S|[, ]N ?S|NOVA SCOTIA", "NOVA SCOTIA"], ["^N ?U|[, ]N ?U|NUNAVUT", "NUNAVUT"], ["^O ?N ?T|[, ]O ?N ?T|ONTARIO", "ONTARIO"], ["^P ?Q|[, ]P ?Q|QBC|QUEBC|QUE|QUEBEC ?(CITY)?", "QUEBEC"], ["^B ?C|[, ]B ?C|BRITISH COLUMBIA", "BRITISH COLUMBIA"], ["^S ?K|[, ]S ?K|SASKATCHEWAN", "SASKATCHEWAN"], ["^P ?E|[, ]P ?E|PRINCE ED(WARD)?( ISLAND)?", "PRINCE EDWARD ISLAND NATIONAL PARK"], ["^Y ?K|[, ]Y ?K|YUKON", "YUKON TERRITORY"]]
        ##DENMARK
        elif ctry=="DK":
            if re.search(r"AARHUS", city):
                city = "AARHUS"
            isCity = re.findall(r"\b(AARHUS|COPPENHAGEN)\b", city)
            if len(isCity)>0:
                city = isCity[0]
        ##FINLAND
        ##FRANCE
        elif ctry=="FR":
            city = re.sub(r"[/]| ?SUR ?", ",", city)
            city = re.sub(r"\b(CEDEX|ALL)\b", "", city)    
        ##GERMANY
        elif ctry=="DE":
            city = re.sub(r"[/]| ?AN DER ?", ",", city)
            if re.search(r"\b(ITZEHOE|AUE|EUE)\b", city)==None:
                city = re.sub(r"AE|OE|UE", lambda(x):x.group()[0], city)
        ##HONG KONG
        elif ctry=="HK":
            city = re.sub(r"\b[,]?(HK|HONG KONG)\b", "", city)
        ##HUNGARY
        ##IRELAND
        elif ctry=="EI":
            city = re.sub(r"\bCOUNTY\b", "", city)
        ##ITALY
        ##JAPAN
        elif ctry=="JP":
            #KEEPS THESE, PSEUDO STEP IT!
##            city = re.sub(r"[-]?(KEN|SHI|SI|CITY|GUN|KU|MACHI|CHO|MURA)", "", city)
            city = re.sub(r"ALL( OR)?|[-]?PREF(ECTURE)?[.]?", "", city)
            prov = ["AICHI", "AKITA", "AOMORI", "CHIBA", "EHIME", "FUKUI", "FUKUOKA", "FUKUSHIMA", "GIFU", "GUMMA", "HIROSHIMA", "HOKKAIDO", "HYOGO", "IBARAKI", "ISHIKAWA", "IWATE", "KAGAWA", "KAGOSHIMA", "KANAGAWA", "KOCHI", "KUMAMOTO", "KYOTO", "MIE", "MIYAGI", "MIYAZAKI", "NAGANO", "NAGASAKI", "NARA", "NIIGATA", "OITA", "OKAYAMA", "OKINAWA", "OSAKA", "SAGA", "SAITAMA", "SHIGA", "SHIMANE", "SHIZUOKA", "TOCHIGI", "TOKUSHIMA", "TOKYO", "TOTTORI", "TOYAMA", "WAKAYAMA", "YAMAGATA", "YAMAGUCHI", "YAMAMASHI", "RUMOI", "SHIRIBESHI", "SORACHI", "SOYA", "TOKACHI", "TSUHIMA"]
        ##KOREA
        elif ctry=="KR":
            city = re.sub(r"[-]?(MEGA)?CITY", "", city)
            city = re.sub(r"(BUK|BOOK|BUKE|BOK)", "BUG", city)
            city = re.sub(r"[-,]? *?(DO[ME]?|DU|DI)\b", "DO", city)
            city = re.sub(r"[-,]?NAM[- ]?[GD]O", "NAMDO", city)
            city = re.sub(r"[-,]?BUG[- ]?[GD]O", "BUGDO", city)
            #NONE OF THESE CITIES ARE FOUND!
            #CHUNGCHEONG IS A VEGATATION TYPE?  OINK?
            city = re.sub(r"[-]?CH(OO|U|E|EO|UR)(NG|N|G)[- ]?CH(OO|U|E|EO|UR)(NG|N|G)", "CHUNGCHEONG", city)
            city = re.sub(r"[-]?[HKG]AN+GWE?ON", "KANGWEON", city)
            city = re.sub(r"\b[HKG]AN+G", "KANG", city) #KANG
            #KYONG
            city = re.sub(r"\b[HKG]{1,2}[YAEOU]{1,3}N{0,2}[AEOU]?[GDK]", "KYONG", city) 
            city = re.sub(r"\b[GK]Y[EOU]{1,2}NG?[SE][AU]NG{0,2}\b", "GYEONGSANG", city)
            city = re.sub(r"\bCHEJU\b", "JEJU", city)
            city = re.sub(r"(CH|J)[EOU]{1,3}N?[LR]+A", "CHEOLLA", city)        
            prov = ["SEOUL", "CHUNGCHEONGBUGDO", "CHUNGCHEONGNAMDO", "KANGWONDO", ["KYONG[- ]?[GDK]?[IOE]{0,2}DO", "KYONGGIDO"], "GYEONGSANGBUGDO", "GYEONGSANGNAMDO", "JEJU", "JEOLLABUGDO", "JEOLLANAMDO"]
        ##LUXEMBURG
        elif ctry=="LU":
            city = re.sub(r"[/]| ?AN DER ?", ",", city)
        ##NETHERLANDS
        elif ctry=="NL":
            prov = [["AM(ER)?STERDA[MN]{0,2}", "AMSTERDAM"]]
        ##NORWAY
        ##RUSSIA
        elif ctry=="SU":
            ctry = "RU"
        elif ctry=="RU":
            if re.search(r"\bKIEV\b", city)!=None:
                ctry = "UA"
            elif re.search(r"\bRIGA\b", city)!=None:
                ctry = "LV"
            elif re.search(r"\bMINSK\b", city)!=None:
                ctry = "BY"
            elif re.search(r"\bTALLIN\b", city)!=None:
                ctry = "EE"
            elif re.search(r"\b(VILLNJUS|VILNIUS)\b", city)!=None:
                ctry = "LT"
            prov = ["MOSCOW", ["MOSKOVSK[A-Z]*?|MOSKOVSKA[VJY]A", "MOSKOVSKAVA"], ["LENINGRAD", "SAINT PETERSBURG"]]
        ##SWEDEN
        ##SWITZERLAND
        ##TAIWAN
        elif ctry=="TW":
            city = re.sub(r"\b[,]?(TW|TAIWAN)\b", "", city)
            city = re.sub(r"\b(AREA|INDUSTRIAL|TOWN|CITY|COUNTY|DIST(RICT)?|COUNTRY)\b", "", city)
            city = re.sub(r"\b(SHIEN)\b", "HSIEN", city)
            prov = [["T[EA]I[- ]?PEI", "TAIPEI"], ["T[EA]I[- ]?CHUNG", "TAICHUNG"], ["T[EA]I[- ]?NAN", "TAINAN"], ["[GK]AO[- ]?HSI[AEU]?NG", "KAOHSIUNG"], ["T[EA][IO]?[- ]?YUANG?", "TAOYUAN"], ["CHIA[- ]?[YI]{1,2}", "CHIAYI"], ["HSIN[- ]?CHU", "HSINCHU"], ["[CZ]HANG[- ]?H[UW]A", "CHANGHUA"], ["NAN[- ]?TOU?", "NANTOU"], ["PING[- ]?T[UO]NG", "PINGTUNG"], ["MIA[UO][- ]?LIH?", "MIAOLI"], ["YUN[- ]?LIN", "YUNLIN"], ["HUA[- ]?LIEN", "HUALIEN"], ["PENG[- ]?H[UY]", "PENGHU"], ["ILA[MN]", "ILAM"]]
        ##UK
        elif ctry=="GB":
            city = re.sub(r"COUNTY( OF)|CO?", "", city)
            city = re.sub(r"SHIR{1,}E", "SHIRE", city)
            prov = ["BEDFORDSHIRE", "BERKSHIRE", ["BUCK?INGHAMSHIRE", "BUCKINGHAMSHIRE"], "CAMBRIDGESHIRE", "CHESHIRE", "CORNWALL", "CUMBRIA", "DERBYSHIRE", "DEVON", "DORSET", "DURHAM", ["E(AST)? SUSSEX", "EAST SUSSEX"], "ESSEX", ["GLOU[A-Z]{0,5}ESTERSHIRE", "GLOUCESTERSHIRE"], "HAMPSHIRE", "HEREFORDSHIRE", "HERTFORDSHIRE", ["ISLE( OF)? WIGHT", "ISLE OF WIGHT"], "KENT", "LANCASHIRE", ["LEI[A-Z]{0,5}ESTERSHIRE", "LEICESTERSHIRE"], "LINCOLNSHIRE", "NORFOLK", "NORTHAMPTONSHIRE", "NORTHUMBERLAND", ["N(ORTH)? YORKSHIRE", "NORTH YORKSHIRE"], "NOTTINGHAMSHIRE", "OXFORDSHIRE", "OXON", "SHROPSHIRE", "SOMERSET", ["N(ORTH)? SOMERSET", "NORTH SOMERSET"], "STAFFORDSHIRE", "SUFFOLK", "SURREY", "WARWICKSHIRE", ["W(EST)? SUSSEX", "WEST SUSSEX"], "WILTSHIRE", ["WOR[A-Z]{0,5}ESTERSHIRE", "WORCESTERSHIRE"], "BARNSLEY", ["BLACKBURN W(ITH )?DARWEN", "BLACKBURN WITH DARWEN"], "BLACKPOOL", "BOLTON", "BOURNEMOUTH", "BRACKNELL FOREST", "BRIGHTON AND HOVE", "BURY", "CALDERDALE", "DARLINGTON", "DONCASTER", "DUDLEY", "GATESHEAD", "HALTON", "HARTLEPOOL", "KIRKLEES", "KNOWSLEY", "LUTON", "MEDWAY", "MIDDLESBROUGH", "MILTON", "KEYNES", ["N(ORTH)? TYNESIDE", "NORTH TYNESIDE"], "OLDHAM", "POOLE", "READING", "REDCAR AND CLEVELAND", "ROCHDALE", "ROTHERHAM", "SANDWELL", "SEFTON", "SLOUGH", "SOLIHULL", ["SOUTHEND[- ]ON[- ]SEA", "SOUTHEND ON SEA"], ["S(OUTH)? TYNESIDE", "SOUTH TYNESIDE"], ["S(AIN)?T HELENS", "ST HELENS"], "STOCKPORT", ["STOCKTON[- ]ON[- ]TEES", "STOCKTON ON TEES"], "SWINDON", "TAMESIDE", "THURROCK", "TORBAY", "TRAFFORD", "WALSALL", "WARRINGTON", "WIGAN", "WIRRAL", "WOLVERHAMPTON", ["BARKING (AND )?DAGENHAM", "BARKING AND DAGENHAM"], "BARNET", "BEXLEY", "BRENT", "BROMLEY", "CAMDEN", "CROYDON", "EALING", "ENFIELD", "GREENWICH", "HACKNEY", ["HAMMERSMITH (AND )?FULHAM", "HAMMERSMITH AND FULHAM"], "HARINGEY", "HARROW", "HAVERING", "HILLINGDON", "HOUNSLOW", "ISLINGTON", "LAMBETH", "LEWISHAM", "MERTON", "NEWHAM", "REDBRIDGE", "RICHMOND UPON THAMES", "SOUTHWARK", "SUTTON", "TOWER HAMLETS", "WALTHAM FOREST", "WANDSWORTH", "BIRMINGHAM", "BRADFORD", "COVENTRY", "LEEDS", "LIVERPOOL", "MANCHESTER", "NEWCASTLE UPON TYNE", "SALFORD", "SHEFFIELD", "SUNDERLAND", "WAKEFIELD", "WESTMINSTER", "BRISTOL", "DERBY", "KINGSTON UPON HULL", "LEICESTER", "LONDON", "NOTTINGHAM", "PETERBOROUGH", "PLYMOUTH", "PORTSMOUTH", "SOUTHAMPTON", ["STOKE[- ]ON[- ]TRENT", "STOKE ON TRENT"], "YORK", "ANTRIM", "COUNTY ANTRIM", "COUNTY ARMAGH", "COUNTY DOWN", "COUNTY FERMANAGH", "COUNTY LONDONDERRY", "COUNTY TYRONE", "ABERDEEN CITY", "ABERDEENSHIRE", "ANGUS", ["ARGYLL (AND )?BUTE", "ARGYLL AND BUTE"], "SCOTTISH BORDERS", "CLACKMANNANSHIRE", ["DUMFRIES (AND )?GALLOWAY", "DUMFRIES AND GALLOWAY"], "DUNDEE CITY", ["E(AST)? AYRSHIRE", "EAST AYRSHIRE"], ["E(AST)? DUNBARTONSHIRE" "EAST DUNBARTONSHIRE"], ["E(AST)? LOTHIAN", "EAST LOTHIAN"], ["E(AST)? RENFREWSHIRE", "EAST RENFREWSHIRE"], "EDINBURGH", "FALKIRK", "FIFE", "GLASGOW", "HIGHLAND", ["ARGYLL (AND )?BUTE" "ARGYLL AND BUTE"], "EDINBURGH", "GLASGOW", "INVERCLYDE", "MIDLOTHIAN", "MORAY", "NORTH AYRSHIRE", ["N(ORTH)? LANARKSHIRE" "NORTH LANARKSHIRE"], "ORKNEY ISLANDS", ["PERTH (AND )?KINROSS", "PERTH AND KINROSS"], "RENFREWSHIRE", "SHETLAND ISLANDS", ["S(OUTH)? AYRSHIRE", "SOUTH AYRSHIRE"], ["S(OUTH)? LANARKSHIRE", "SOUTH LANARKSHIRE"], "STIRLING", ["W(EST)? DUNBARTONSHIRE", "WEST DUNBARTONSHIRE"], "EILEAN SIAR", "WESTERN ISLES", ["W(EST)? LOTHIAN", "WEST LOTHIAN"], "ISLE OF ANGLESEY", "CEREDIGION", "CARMARTHENSHIRE", "DENBIGHSHIRE", "FLINTSHIRE", "MONMOUTHSHIRE", "PEMBROKESHIRE", "POWYS", "VALE OF GLAMORGAN", "ENGLAND", "SCOTLAND", "WALES", "NORTHERN IRELAND", "REPUBLIC OF IRELAND", "ISLE OF MAN", "CHANNEL ISLANDS"]
        ##USA
        elif ctry=="US":
            city = re.sub(r"^ST\b", "SAINT", city)
            city = re.sub(r"\bCOLO\b", "COLORADO", city)
            city = re.sub(r"\bSPGS\b", "SPRINGS", city)
            city = re.sub(r"\bJCTN?\b", "JUNCTION", city)
            city = re.sub(r"\bMTN\b", "MOUNTAIN", city)
            city = re.sub(r"\bMT\b", "MOUNT", city)
            city = re.sub(r"\bHG?TS\b", "HEIGHTS", city)
            city = re.sub(r"\bCOUNTY\b", "", city)
            city = re.sub(r"\bGDNS\b", "GARDENS", city)
            city = re.sub(r"\bSLC\b", "SALT LAKE CITY", city)
            if city=="LA":
                city = "LOS ANGELES"
            city = re.sub(r"\bLV\b", "LAS VEGAS", city)
            city = re.sub(r"\bSF\b", "SAN FRANCISCO", city)
            city = re.sub(r"\bTOWNSEND\b", "", city)
            city = re.sub(r"\bTOWNS(H|HI|HIP)?\b", "", city)
            city = re.sub(r"\bFT\b", "FORT", city)
            city = re.sub(r"\bNO?\b", "NORTH", city)
            city = re.sub(r"\bE\b", "EAST", city)
            city = re.sub(r"\bSO?\b", "SOUTH", city)
            city = re.sub(r"\bW\b", "WEST", city)
            city = re.sub(r"\bU\b", "UPPER", city)
            city = re.sub(r"\bIS\b", "ISLAND", city)
            city = re.sub(r"\bPK\b", "PARK", city)
            city = re.sub(r"\bOKLA\b", "OKLAHOMA", city)
            city = re.sub(r"\bPO BOX\b", "", city)

        for x in prov:
            if type(x)==types.ListType:
                if re.search(r"([, ]|\b)(%s)([, ]|\b)" % x[0], city)!=None:
                    city = re.sub(r"([, ]|\b)(%s)([, ]|\b)" % x[0], ",%s," % x[1], city)
                    break
            elif type(x)==types.StringType:
                if city.find(x)>=0:
                    city = re.sub(r"([, ]|\b)(%s)([, ]|\b)" % x, ",%s," % x, city)
                    break

        if len(city)>0:
            if city[0]==",":
                city = city[1:]
            if city[-1]==",":
                city = city[:-1]
            
        #remove dash, excess spaces with single space
        city = re.sub(r"([-]|  +)", " ", city)
        return city.strip()
