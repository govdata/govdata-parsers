import os
import urllib
import re
import cPickle as pickle

import numpy as np
import tabular as tb
import pymongo as pm

from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup, NavigableString
from mechanize import Browser
from starflow.utils import activate, MakeDir, Contents, listdir, IsDir, uniqify, PathExists,RecursiveFileList, ListUnion, MakeDirs,delete,strongcopy

import govdata.core
import utils.htools as htools

from utils.basic import wget


#=-=-=-=-=-=-=-=-=-Utilities
def SafeContents(x):
    return ' '.join(Contents(x).strip().split())
    
def filldown(x):
    y = np.array([xx.strip() for xx in x])
    nz = np.append((y != '').nonzero()[0],[len(y)])
    return y[nz[:-1]].repeat(nz[1:] - nz[:-1])

    
def gethierarchy(x,f,postprocessor = None):
    hl = np.array([f(xx) for xx in x])
    # normalize 
    ind = np.concatenate([(hl == min(hl)).nonzero()[0], np.array([len(hl)])])
    if ind[0] != 0:
        ind = np.concatenate([np.array([0]), ind])  
    hl2 = []
    for i in range(len(ind)-1):
        hls = hl[ind[i]:ind[i+1]].copy()
        hls.sort()
        hls = tb.utils.uniqify(hls)
        D = dict(zip(hls, range(len(hls))))
        hl2 += [D[h] for h in hl[ind[i]:ind[i+1]]]

    hl = np.array(hl2)
    m = max(hl)
    cols = []
    for v in range(m+1):
        vxo = hl < v
        vx = hl == v
        if vx.any():
            nzv = np.append(vx.nonzero()[0],[len(x)])
            col = np.append(['']*nzv[0],x[nzv[:-1]].repeat(nzv[1:] - nzv[:-1]))
            col[vxo] = ''
            cols.append(col)
        else:
            cols.append(np.array(['']*len(x)))
    
    if postprocessor:
        for i in range(len(cols)):
            cols[i] = np.array([postprocessor(y) for y in cols[i]])
            
    return [cols,hl]
 
	
    

def getzip(url,zippath,dirpath=None):
    assert zippath.endswith('.zip')
    if dirpath == None:
        dirpath = zippath[:-4]
    print url, zippath      
    os.system('wget ' + url + ' -O ' + zippath)
    os.system('unzip -d ' + dirpath + ' ' + zippath)
    
    
def WgetMultiple(link,fname,opstring='',  maxtries=5):
    for i in range(maxtries):
        wget(link, fname, opstring)
        F = open(fname,'r').read().strip()
        if not (F.lower().startswith('<!doctype') or F == '' or 'servlet error' in F.lower()):
            return
        else:
            print 'download of ' + link + ' failed'
    print 'download of ' + link + ' failed after ' + str(maxtries) + ' attempts'
    return
    
def hr(x):
    return  len(x) - len(x.lstrip(' '))
    
def hr2(x):
    return  len(x.split('\xc2\xa0')) - 1
    
    
def hr3(x):
    return  len(x) - len(x.lstrip('\t'))

    
def GetFootnotes(line, FootnoteSplitter='/'):
    newline = ' '*(len(line)-len(line.lstrip())) + ' '.join([' '.join(x.split()[:-1]) for x in line.split(FootnoteSplitter)[:-1]])
    footnotes = ', '.join([x.split()[-1] for x in line.split(FootnoteSplitter)[:-1]])
    return (newline, footnotes)

def GetFootnotes2(line, FootnoteSplitter='\\'):
    newline = ' '.join(line.split(FootnoteSplitter)[:-1])
    footnotes = ', '.join([x.split()[0] for x in line.split(FootnoteSplitter)[1:]])
    return (newline, footnotes)

def GetFootnotesLazy(line, FootnoteSplitter='\\'):
    newline = line.split(FootnoteSplitter)[0]
    footnotes = line.split(FootnoteSplitter)[1]
    return (newline, footnotes)
    
def CleanLinesForMetadata(x):
    x = [line.strip('"').strip() for line in x]
    line = x[0]
    while line == '':
        x = x[1:]
        line = x[0]
    line = x[-1]
    while line == '':
        x = x[:-1]
        line = x[-1]
    return x
        
    
def ExpandString(S):
    ind2 = [i for i in range(1,len(S)-1) if S[i].lower() != S[i] and S[i+1].lower() == S[i+1]] + [len(S)]
    ind1 = [0] + ind2[:-1]
    return ' '.join([S[i:j] for (i,j) in zip(ind1,ind2)])
    
    
def nea_dateparse(x):
    mmap = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    d = x.split('-')
    if len(d) == 1:
        return d[0] + 'X' + 'XX' 
    elif set(d[1].lower()) <= set(['i','v']):
        D = {'i':'1','ii':'2','iii':'3','iv':'4'}
        return d[0] + D[d[1].lower()] + 'XX'
    elif d[1].lower() in mmap:
        ind = str(mmap.index(d[1].lower())+1)
        ind = ind if len(ind) == 2 else '0' + ind
        return d[0] + 'X' + ind
        
        
def NEA_Parser(page, headerlines=None, FootnoteSplitter = '/', FootnotesFunction = GetFootnotes, CategoryColumn=None,FormulaColumn=None):
    
    [Y, header, footer, keywords]  = BEA_Parser(page, headerlines=headerlines, FootnoteSplitter = FootnoteSplitter, FootnotesFunction = FootnotesFunction, CategoryColumn=CategoryColumn, NEA=True, FormulaColumn=FormulaColumn)
    levelnames = [n for n in Y.dtype.names if n.startswith('Level_')]
    displaylevels = np.array([np.where(Y[n] != '', i,0) for (i,n) in enumerate(levelnames)]).T.max(axis=1)
    Y = Y.addcols(displaylevels,names=['DisplayLevel'])     
    
    labels = [x.strip() for x in Y['Category']]
    for k in range(0,Y['DisplayLevel'].max()):
        badcounts = [i for i in range(len(labels)) if labels.count(labels[i]) > 1]
        for i in badcounts:
            if Y['DisplayLevel'][i] - k >= 1:
                labels[i] = (labels[i] + ' - ' + Y['Level_' + str(Y['DisplayLevel'][i] - k)][i]).strip()
    Y = Y.addcols(labels,names=['Label'])
    
    Y.metadata = {'labelcollist':['Label']}
                    
    return [Y, header, footer, keywords]                
    
                                    
def BEA_Parser(page, headerlines=None, FootnoteSplitter = '/', FootnotesFunction = GetFootnotes, CategoryColumn=None, NEA=False, FormulaColumn=None):

    if NEA:
        G = [line.strip() for line in open(page, 'rU').read().strip('\n').split('\n')]  
        # get header
        keepon = 1
        i = 0
        header = []
        while keepon:
            line = G[i]
            if not line.startswith('Line'):
                header += [line]
                i = i + 1
            else:
                keepon = 0              
        [F, meta] = tb.io.loadSVrecs(page, headerlines=i+1,delimiter = ',')

    else:
        [F, meta] = tb.io.loadSVrecs(page, headerlines=headerlines)
        header = None

    names = [n.strip() for n in meta['names']]

    if not NEA:
        names = names[:len(F[0])]
        F = [f[:len(names)] for f in F]

    if names[1] == '':
        names[1] = 'Category'
    
    # get footer
    keepon = 1
    i = len(F)-1
    footer = [] 
    while keepon:
        rec = F[i]
        if len(rec) != len(names):
            footer = [','.join(rec)] + footer
            i = i - 1
        else:
            keepon = 0

    F = F[:i+1]
    F = [line + ['']*(len(names)-len(line)) for line in F ]
    

    if CategoryColumn:
        i = [i for i in range(len(names)) if names[i].strip() == CategoryColumn][0]
        CatCol = np.array([row[i] for row in F])
        
    F = [tuple([col.strip() for col in row]) for row in F]

    ind = [i for i in range(len(names)) if names[i][:4].isdigit()][0]
    X = tb.tabarray(records=F, names=names, coloring={'Info': names[:ind], 'Data': names[ind:]})    
    
    FootnoteColumns = []
    FootnoteNames = []
    for cname in X.coloring['Info']:            
        L = [len(c.split(FootnoteSplitter)) > 1 for c in X[cname]]
        if any(L):
            #Column_Footnote = [FootnotesFunction(X[cname][i]) if L[i] else (col[i], '') for i in range(len(X))]
            Column_Footnote = [FootnotesFunction(c) if l else (c, '') for (c, l) in zip(X[cname], L)]
            X[cname] = [c for (c,n) in Column_Footnote]
            FootnoteColumns += [[n for (c,n) in Column_Footnote]]
            FootnoteNames += [cname + ' Footnotes']
            if cname == CategoryColumn:
                Column_Footnote = np.array([FootnotesFunction(c) if l else (c, '') for (c, l) in zip(CatCol, L)])
                CatCol = np.array([c for (c,n) in Column_Footnote])
                
    if FootnoteColumns:
        Footnotes = tb.tabarray(columns = FootnoteColumns, names = FootnoteNames, coloring = {'Footnotes': FootnoteNames})
    else:
        Footnotes = None
        
    if FormulaColumn:
        L = [len(c.split('(')) > 1 and any([x.isdigit() and x<1000 for x in c.split('(')[1]]) for c in X[FormulaColumn]]
        if any(L):
            Formula = [X[FormulaColumn][i].split('(')[1].split(')')[0] if L[i] else '' for i in range(len(X))]
            X[FormulaColumn] = [X[FormulaColumn][i].split('(')[0].rstrip() if L[i] else X[FormulaColumn][i] for i in range(len(X))]
            X = X['Info'].colstack(tb.tabarray(columns = [Formula], names = ['Formula'])).colstack(X['Data'])
            X.coloring['Info'] += ['Formula']

    if CategoryColumn:
        [cols, hl] = gethierarchy(CatCol, hr, postprocessor = lambda x : x.strip())      
        columns = [c for c in cols if not (c == '').all()]
        if len(columns) > 1:
            categorynames = ['Level_' + str(i) for i in range(1, len(columns)+1)]       
            Categories = tb.tabarray(columns = columns, names = categorynames, coloring = {'Categories': categorynames})
        else:
            Categories = None
    else:
        Categories = None
    
    Y = X['Info']
    if Footnotes != None:
        Y = Y.colstack(Footnotes)
    if Categories != None:
        Y = Y.colstack(Categories)      
    if NEA:
        X.replace('---','')
        Y = Y.colstack(tb.tabarray(columns=[tb.utils.DEFAULT_TYPEINFERER(X[c]) for c in X.coloring['Data']], names=X.coloring['Data'], coloring={'Data': X.coloring['Data']}))
    else:
        Y = Y.colstack(X['Data'])
    
    if CategoryColumn:
        keywords = [y for y in tb.utils.uniqify([x.replace(',', '').replace('.', '').replace(',', '') for x in X[CategoryColumn]]) if y]
    else:
        keywords = []

    if footer:
        footer = CleanLinesForMetadata(footer)
    if header:
        header = CleanLinesForMetadata(header)
        
    return [Y, header, footer, keywords]    

        
def NEA_preparser2(inpath,filepath,metadatapath,L = None):

    MakeDir(filepath)

    if L == None:
        L = [inpath + x for x in listdir(inpath) if x.endswith('.tsv')]
    T = [x.split('/')[-1].split('_')[0].strip('.') for x in L]
    R = tb.tabarray(columns=[L,T],names = ['Path','Table']).aggregate(On=['Table'],AggFunc=lambda x : '|'.join(x))
    
    ColGroups = {}
    Metadict = {}
    for (j,r) in enumerate(R):
        ps = r['Path'].split('|')
        t = r['Table']
        print t
        assert t != ''
        X = [tb.tabarray(SVfile = p) for p in ps]
        X1 = [x[x['Line'] != ''].deletecols(['Category','Label','DisplayLevel']) for x in X]
        for i in range(len(X)):
            X1[i].metadata = X[i].metadata
            X1[i].coloring['Topics'] = X1[i].coloring.pop('Categories')
            X1[i].coloring['timeColNames'] = X1[i].coloring['Data']
            X1[i].coloring.pop('Data')
            for k in range(len(X1[i].coloring['timeColNames'])):
                name = X1[i].coloring['timeColNames'][k]
                X1[i].renamecol(name,nea_dateparse(name))
                
            
        if len(X1) > 1:     
            keycols = [x for x in X1[0].dtype.names if x not in X1[0].coloring['timeColNames']]
            Z = tb.tab_join(X1,keycols=keycols)
        else:
            Z = X1[0]
            
        topics = sorted(uniqify(Z.coloring['Topics']))
        Z.coloring['Topics'] = topics
        for topic in topics:
            Z.renamecol(topic,'Topic ' + topic)
            
        K = ['Category','Section','units','notes','downloadedOn','lastRevised','Table','footer','description']
        Z.metadata = {}
        for k in K:
            h = [x.metadata[k] for x in X if k in x.metadata.keys()]
            if h:
                if isinstance(h[0],str):
                    Z.metadata[k] = ', '.join(uniqify(h))
                elif isinstance(h[0],list) or isinstance(h[0],tuple):
                    Z.metadata[k] = uniqify(ListUnion(h))
                else:
                    print 'metadata type for key', k , 'in table', t, 'not recognized.'
                    
        Section = Z.metadata.pop('Section')

        Table = Z.metadata.pop('Table').strip().split('.')[-1].strip()
        
        for k in Z.coloring.keys():
            if k in ColGroups.keys():
                ColGroups[k] = uniqify(ColGroups[k] + Z.coloring[k])
            else:
                ColGroups[k] = Z.coloring[k]
        
        Metadict[t] = Z.metadata
        Metadict[t]['title'] = Table

        Z = Z.addcols([[t]*len(Z),[Table]*len(Z),[Section]*len(Z),[t]*len(Z)],names=['TableNo','Table','Section','subcollections'])
        Z.saveSV(filepath + str(j) + '.tsv',metadata=['dialect','formats','names'])
    
    AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
    AllMeta = {}
    for k in AllKeys:
        if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
            AllMeta[k] = Metadict[Metadict.keys()[0]][k]
            for l in Metadict.keys():
                Metadict[l].pop(k)
    
    Category = AllMeta.pop('Category')

    AllMeta['topicHierarchy'] = ('agency','subagency','program','dataset','Section','Table')
    AllMeta['uniqueIndexes'] = ['TableNo','Line']
    ColGroups['Topics'].sort()
    ColGroups['labelColumns'] =  ['Table','Topics']
    AllMeta['columnGroups'] = ColGroups
    AllMeta['description'] = 'National Income and Product Accounts (NIPA) data from the <a href="http://www.bea.gov/national/nipaweb/SelectTable.asp?Selected=N">All NIPA Tables</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the NIPAs, see: <a href="http://www.bea.gov/scb/pdf/misc/nipaguid.pdf">A Guide to the National Income and Product Accounts of the United States (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/2009/11%20November/1109_nipa_method.pdf">Updated Summary of NIPA Methodologies (PDF)</a>, and <a href="http://www.bea.gov/scb/pdf/2003/08August/0803NIPA-Preview.pdf#page=9">Guide to the Numbering of the NIPA Tables (PDF)</a>.'
    AllMeta['dateFormat'] = 'YYYYqmm'
    AllMeta['keywords'] = ['NIPA','GDP','income']
    AllMeta['sliceCols'] = [['Section','Table','Topic Level_0','Topic Level_1','Topic Level_2'] + [tuple(ColGroups['Topics'][:i]) for i in range(4,len(ColGroups['Topics']) + 1)]]
    AllMeta['phraseCols'] = ['Section','Table','Topic','Line','TableNo']

    
    Subcollections = Metadict
    Subcollections[''] = AllMeta
        
    F = open(metadatapath,'w')
    pickle.dump(Subcollections,F)
    F.close()


#=-=-=-=-=-=-=-=-=-=-=-=-=-=FAT
FAT_NAME = 'BEA_FAT'
@activate(lambda x : 'http://www.bea.gov/national/FA2004/SelectTable.asp',lambda x : x[0])
def FAT_downloader(maindir):

    MakeDirs(maindir)
   
    get_FAT_manifest(maindir)
    
    connection = pm.Connection()
    
    incremental = FAT_NAME in connection['govdata'].collection_names()
    
    MakeDir(maindir + 'raw/')
    
    URLBase = 'http://www.bea.gov/national/FA2004/csv/NIPATable.csv?'
    
   
    X = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in X:
        
        NC = x['NumCode']
        Freq = x['Freq']
        if incremental:
            Vars = ['TableName','FirstYear','LastYear','Freq']
            FY = x['FirstYear'] 
            LY = x['LastYear']
            url = URLBase + '&'.join([v + '=' + str(val) for (v,val) in zip(Vars,[NC,FY,LY,Freq])])
        else:
            Vars = ['TableName','AllYearChk','FirstYear','LastYear','Freq']
            FY = 1800
            LY = 2200
            url = URLBase + '&'.join([v + '=' + str(val) for (v,val) in zip(Vars,[NC,'YES',FY,LY,Freq])])
     
        
        topath = maindir + 'raw/' + x['Section'] + '_' + x['Table']  + '.csv'
        
        WgetMultiple(url,topath)
        
                
        
def get_FAT_manifest(download_dir,depends_on = 'http://www.bea.gov/national/FA2004/SelectTable.asp'):
    
    wget(depends_on,download_dir + 'manifest.html')
    nc = re.compile('SelectedTable=[\d]+')
    fy = re.compile('FirstYear=[\d]+')
    ly = re.compile('LastYear=[\d]+')
    fr = re.compile('Freq=[a-zA-Z]+')
    
    L = lambda reg , x : int(reg.search(str(dict(x.findAll('a')[0].attrs)['href'])).group().split('=')[-1])
    L2 = lambda reg , x : reg.search(str(dict(x.findAll('a')[0].attrs)['href'])).group().split('=')[-1]
    
    path = download_dir + 'manifest.html'
    Soup = BeautifulSoup(open(path),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
    c1 = lambda x : x.name == 'a' and 'name' in dict(x.attrs).keys() and dict(x.attrs)['name'].startswith('S')
    c2 = lambda x : x.name == 'tr' and 'class' in dict(x.attrs).keys() and dict(x.attrs)['class'] == 'TR' and x.findAll('a') and 'href' in dict(x.findAll('a')[0].attrs).keys() and  dict(x.findAll('a')[0].attrs)['href'].startswith('Table')
    
    p1 = lambda x : Contents(x).strip().strip('\xc2\xa0').strip()
    p2 = lambda x : (p1(x),'http://www.bea.gov/national/FA2004/' + str(dict(x.findAll('a')[0].attrs)['href']),L(nc,x),L(fy,x),L(ly,x),L2(fr,x))
    
    X = htools.MakeTable(Soup,[c1,c2],[p1,p2],['Section',['Table','URL','NumCode','FirstYear','LastYear','Freq']])
    secnums = [x['Section'].split(' ')[1].strip() for x in X]
    secnames = [x['Section'].split('-')[1].strip() for x in X]
    tablenums = [x['Table'].split(' ')[1].split('.')[-2].strip() for x in X]
    tablenames = [' '.join(x['Table'].split(' ')[2:]).strip() for x in X]
    X = X.addcols([secnums,secnames,tablenums,tablenames],names=['Section','SectionName','Table','TableName'])
    X.saveSV(download_dir + 'manifest.tsv',metadata=True)

@activate(lambda x : (x[0] + 'raw/',x[0] + 'manifest.tsv'), lambda x : x[0] + 'preparsed/')
def FAT_preparser1(maindir):    

    targetdir = maindir + 'preparsed/'
    sourcedir = maindir + 'raw/'
    
    MakeDir(targetdir) 
    M = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in M:
        f = sourcedir + x['Section'].strip('.') + '_' + x['Table'] + '.csv'
        print f 
        savepath = targetdir + x['Section'].strip('.') + '_' + x['Table'] + '.tsv'
    
        [X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category',FormulaColumn='Category')        
        
        metadata = {}
        metadata['Header'] = '\n'.join(header)
        (title, units, bea) = header
        metadata['title'] = title
        metadata['description'] = 'Fixed Asset "' + title + '" from the <a href="http://www.bea.gov/national/FA2004/SelectTable.asp">Standard Fixed Asset Tables</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the Fixed Asset Tables, see: <a href="http://www.bea.gov/national/pdf/Fixed_Assets_1925_97.pdf"> Methodology, Fixed Assets and Consumer Durable Goods in the United States, 1925-97 | September 2003 (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/national/niparel/1997/0797fr.pdf">The Measurement of Depreciation in the NIPA\'s | SCB, July 1997 (PDF) </a>, and <a href="http://www.bea.gov/national/FA2004/Tablecandtext.pdf">BEA Rates of Depreciation, Service Lives, Declining-Balance Rates, and Hulten-Wykoff categories | February 2008  (PDF)</a>.'
        metadata['Agency'] = 'DOC'
        metadata['Subagency'] = 'BEA'
        metadata['Type'] = 'National'
        metadata['Category'] = 'Fixed Asset Tables'
        metadata['Table'] = ' '.join(title.split()[1:])
        metadata['SectionNo'] = x['Section']
        metadata['Section'] = x['SectionName']
        metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Section', 'Table'])
        metadata['units'] = units.strip('[]')
        if footer:
            metadata['footer'] = '\n'.join(footer)

        metadata['keywords'] = 'National Economic Accounts,' + ','.join(keywords)       
            
        X.metadata.update(metadata)
        X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
        
        
@activate(lambda x: (x[0] + 'preparsed/',x[0]+'manifest.tsv'),lambda x : (x[0] + '__PARSE__/',x[0] + '__metadata.pickle'))
def FAT_preparser2(maindir): 
    sourcedir = maindir + 'preparsed/'
    filedir = maindir + '__PARSE__/'
    metadatapath = maindir + '__metadata.pickle'

    MakeDir(filedir)
   
    GoodKeys = ['Category', 'Section', 'units', 'Table', 'footer','description']
    
    Metadict = {}
    ColGroups = {}
    
    M = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for (i,x) in enumerate(M):
        l = sourcedir + x['Section'].strip('.') + '_' + x['Table'] + '.tsv'
        print l
        t = x['Section'] + '.' + x['Table']
        print t
        X = tb.tabarray(SVfile = l)
  
        topics = sorted(uniqify(X.coloring.pop('Categories')))
        X.coloring['Topics'] = topics
        for topic in topics:
            X.renamecol(topic,'Topic ' + topic)
               
        X1 = X[X['Line'] != ''].deletecols(['Category','Label','DisplayLevel'])
        X1.metadata = X.metadata
        X = X1
                 
        X.coloring['timeColNames'] = X.coloring.pop('Data')
        for j in range(len(X.coloring['timeColNames'])):
            name = X.coloring['timeColNames'][j]
            X.renamecol(name,nea_dateparse(name))
            
        SectionNo = X.metadata.pop('SectionNo').split('-')[-1].strip()
        Section = X.metadata.pop('Section')
        Table = X.metadata.pop('Table')
        
        for k in X.coloring.keys():
            if k in ColGroups.keys():
                ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
            else:
                ColGroups[k] = uniqify(X.coloring[k])
        
        Metadict[t] = dict([(k,X.metadata[k]) for k in GoodKeys if k in X.metadata.keys()])
        Metadict[t]['title'] = Table
        
        X = X.addcols([[t]*len(X),[Table]*len(X),[SectionNo]*len(X),[Section]*len(X),[t]*len(X)],names=['TableNo','Table','SectionNo','Section','subcollections'])
        X.saveSV(filedir + str(i) + '.tsv',metadata=['dialect','names','formats'])
    
    AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
    AllMeta = {}
    for k in AllKeys:
        if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
            AllMeta[k] = Metadict[Metadict.keys()[0]][k]
            for l in Metadict.keys():
                Metadict[l].pop(k)
                
    Category = AllMeta['Category']
    AllMeta.pop('Category')
    AllMeta['topicHierarchy'] =  ('agency','subagency','program','dataset','Section','Table')
    AllMeta['uniqueIndexes'] = ['TableNo','Line']
    ColGroups['Topics'].sort()
    ColGroups['labelColumns'] = ['Table','Topics']
    AllMeta['description'] = 'The <a href="http://www.bea.gov/national/FA2004/SelectTable.asp">Standard Fixed Asset Tables</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the Fixed Asset Tables, see: <a href="http://www.bea.gov/national/pdf/Fixed_Assets_1925_97.pdf"> Methodology, Fixed Assets and Consumer Durable Goods in the United States, 1925-97 | September 2003 (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/national/niparel/1997/0797fr.pdf">The Measurement of Depreciation in the NIPA\'s | SCB, July 1997 (PDF) </a>, and <a href="http://www.bea.gov/national/FA2004/Tablecandtext.pdf">BEA Rates of Depreciation, Service Lives, Declining-Balance Rates, and Hulten-Wykoff categories | February 2008  (PDF)</a>.'
    AllMeta['columnGroups'] = ColGroups
    AllMeta['dateFormat'] = 'YYYYqmm' 
    AllMeta['sliceCols'] = [['Section','Table'] + [tuple(ColGroups['Topics'][:i]) for i in range(1,len(ColGroups['Topics']) +1)]]
    AllMeta['keywords'] = ['fixed assets','equipment','software','structures' ,'durable goods']

    Subcollections = Metadict
    Subcollections[''] = AllMeta

    F = open(metadatapath ,'w')
    pickle.dump(Subcollections,F)
    F.close()
        

def fat_trigger():
    connection = pm.Connection()
    
    incremental = FAT_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        
        
FAT_PARSER = govdata.core.GovParser(FAT_NAME,
                                    govdata.core.CsvParser,
                                    downloader = [(FAT_downloader,'raw'),
                                                  (FAT_preparser1,'preparse1'),
                                                  (FAT_preparser2,'preparse2')],
                                    trigger = fat_trigger,
                                    incremental=True)

            
#=-=-=-=-=-=-=-=-=-=-=-=-=-=NIPA
NIPA_NAME = 'BEA_NIPA'
@activate(lambda x : 'http://www.bea.gov/national/nipaweb/csv/NIPATable.csv',lambda x : x[0])
def NIPA_downloader(maindir):

    MakeDirs(maindir)
   
    get_manifest(maindir)
    
    connection = pm.Connection()
    
    incremental = NIPA_NAME in connection['govdata'].collection_names()
    
    MakeDir(maindir + 'raw/')
    
    URLBase = 'http://www.bea.gov/national/nipaweb/csv/NIPATable.csv?'
    
    Vars = ['TableName','FirstYear','LastYear','Freq']
    X = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in X:
        
        NC = x['NumCode']
        Freq = x['Freq']
        if incremental:
            FY = x['FirstYear'] 
            LY = x['LastYear']
        else:
            FY = 1800
            LY = 2200
            ystr = ''
        
        url = URLBase + '&'.join([v + '=' + str(val) for (v,val) in zip(Vars,[NC,FY,LY,Freq])])
        
        topath = maindir + 'raw/' + x['Number'].strip('.') + '_' + Freq + '.csv'
        
        WgetMultiple(url,topath)

    

@activate(lambda x : (x[0] + 'raw/',x[0] + 'manifest.tsv'), lambda x : x[0] + 'preparsed/')
def NIPA_preparser1(maindir):
    targetdir = maindir + 'preparsed/'
    sourcedir = maindir + 'raw/'
    
    MakeDir(targetdir) 
    M = tb.tabarray(SVfile = maindir + 'manifest.tsv')
    for x in M:
        f = sourcedir + x['Number'].strip('.') + '_' + x['Freq'] + '.csv'
        print f 
        savepath = targetdir + x['Number'].strip('.') + '_' + x['Freq'] + '.tsv'
    
        [X, header, footer, keywords] = NEA_Parser(f, FootnoteSplitter='\\', FootnotesFunction=GetFootnotesLazy, CategoryColumn='Category',FormulaColumn='Category')        

        metadata = {}
        metadata['Header'] = '\n'.join(header)
        [title, units] = header[:2]
        notes = '\n'.join(header[2:-2])
        [owner, info] = header[-2:]
        metadata['title'] = title
        metadata['description'] = 'National Income and Product Accounts (NIPA) "' + title + '" from the <a href="http://www.bea.gov/national/nipaweb/SelectTable.asp?Selected=N">All NIPA Tables</a> data set under the <a href="http://www.bea.gov/national/index.htm">National Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For additional information on the NIPAs, see: <a href="http://www.bea.gov/scb/pdf/misc/nipaguid.pdf">A Guide to the National Income and Product Accounts of the United States (PDF)</a>, <a href="http://www.bea.gov/scb/pdf/2009/11%20November/1109_nipa_method.pdf">Updated Summary of NIPA Methodologies (PDF)</a>, and <a href="http://www.bea.gov/scb/pdf/2003/08August/0803NIPA-Preview.pdf#page=9">Guide to the Numbering of the NIPA Tables (PDF)</a>.'
        metadata['Agency'] = 'DOC'
        metadata['Subagency'] = 'BEA'
        metadata['Type'] = 'National'
        metadata['Category'] = 'NIPA Tables'
        metadata['Section'] = x['Section']
        metadata['Table'] = ' '.join(title.split()[1:])
        metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Section', 'Table'])
        metadata['units'] = units.strip('[]')
        metadata['notes'] = notes
        metadata['Owner'] = owner
        metadata['downloadedOn'] = info.split('Last')[0]
        metadata['lastRevised'] = info.split('Revised')[1].strip()      

        if footer:
            metadata['Footer'] = '\n'.join(footer)
    
        table = title.split()[1].strip('.')

        metadata['keywords'] = ['National Economic Accounts'] + keywords
            
        X.metadata.update(metadata)
        X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
    
    
@activate(lambda x: x[0] + 'preparsed/',lambda x : (x[0] + '__PARSE__/',x[0] + '__metadata.pickle'))
def NIPA_preparser2(maindir): 
    inpath = maindir + 'preparsed/'
    filedir = maindir + '__PARSE__/'
    metapath = maindir + '__metadata.pickle'
    NEA_preparser2(inpath,filedir,metapath)
    
@activate(lambda x : 'http://www.bea.gov/national/nipaweb/Index.asp',lambda x : (x[0] + 'additional_info.html',x[0] + 'additional_info.csv',x[0] + '__FILES__/'))   
def get_additional_info(download_dir):
    wget('http://www.bea.gov/national/nipaweb/Index.asp',download_dir + 'additional_info.html')
    page = download_dir + 'additional_info.html'
    Soup = BeautifulSoup(open(page,'r'),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
    PA = [(SafeContents(p), str(dict(p.findAll('a')[0].attrs)['href'])) for p in Soup.findAll('blockquote')[0].findAll('p')]
    Recs = [(p, 'http://www.bea.gov' + a) for (p,a) in PA]
    X = tb.tabarray(records = Recs, names = ['Name', 'URL'])
    X.saveSV(download_dir + 'additional_info.csv',metadata=True)
    MakeDir(download_dir + '__FILES__')
    for x in X:
        name = download_dir + '__FILES__/' + x['Name'].replace(' ','_')
        wget(x['URL'],name)
    

def get_manifest(download_dir,depends_on = 'http://www.bea.gov/national/nipaweb/SelectTable.asp?'):
    
    wget(depends_on,download_dir + 'manifest.html')
    nc = re.compile('SelectedTable=[\d]+')
    fy = re.compile('FirstYear=[\d]+')
    ly = re.compile('LastYear=[\d]+')
    page = download_dir + 'manifest.html'
    S = [s.strip() for s in open(page, 'r').read().split('Section')[1:]]
    Section_SplitSoup = [(s.split('-')[1].split('<')[0].strip(), BeautifulSoup(s,convertEntities=BeautifulStoneSoup.HTML_ENTITIES)) for s in S]
    D = {'(A)': 'Year', '(Q)': 'Qtr', '(M)': 'Month'}
    Recs = []
    for (Section, Soup) in Section_SplitSoup:
        alist = [tr.findAll('a')[0] for tr in Soup.findAll('tr') if tr.findAll('a')]
        Table_URL = [(SafeContents(a), 'http://www.bea.gov/national/nipaweb/' + str(dict(a.attrs)['href'])) for a in alist if 'href' in dict(a.attrs).keys()]
        Number_Name_URL = [(t.split()[1], ' '.join(t.split()[2:]), url) for (t, url) in Table_URL]
        XYZ = [tuple(n[0].strip('.').split('.')) if len(n[0].strip('.').split('.'))==3 else tuple(n[0].strip('.').split('.')) + ('',) for n in Number_Name_URL]
        for i in range(len(XYZ)):
            (Section_Number, Subsection_Number, Table_Number) = XYZ[i]
            (Number, Name, u) = Number_Name_URL[i]
            Dlist = [d for d in D.keys() if d in Name]
            for d in Dlist:
                Freq = D[d]
                URL = u.split('Freq=')[0] + 'Freq=' + Freq + '&' + '&'.join(u.split('Freq=')[1].split('&')[1:])
                NumCode = int(nc.search(URL).group().split('=')[-1])
                FirstYear = int(fy.search(URL).group().split('=')[-1])
                LastYear = int(ly.search(URL).group().split('=')[-1])
                Recs += [(Section, Section_Number, Subsection_Number, Table_Number, Number, Freq, Name, URL,NumCode,FirstYear,LastYear)]
        M =  tb.tabarray(records = Recs, names = ['Section', 'Section_Number', 'Subsection_Number', 'Table_Number', 'Number', 'Freq', 'Name', 'URL','NumCode','FirstYear','LastYear'])
        
        M.saveSV(download_dir + 'manifest.tsv',metadata=True)       
        
def trigger():
    connection = pm.Connection()
    
    incremental = NIPA_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        

NIPA_PARSER = govdata.core.GovParser(NIPA_NAME,govdata.core.CsvParser,downloader = [(NIPA_downloader,'raw'),(NIPA_preparser1,'preparse1'),(NIPA_preparser2,'preparse2'),(get_additional_info,'additional_info')],trigger = trigger,incremental=True)
        
            
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

@activate(lambda x : (x[0] + 'State_Preparsed/',x[0] + 'State_Manifest_1.tsv',x[0] + 'Metro_Preparsed.tsv'),lambda x : (x[0] + '__PARSE__/',x[0] + '__metadata.pickle'))
def RegionalGDP_Preparse2(maindir):

    inpath = maindir + 'State_Preparsed/'
    outpath = maindir + '__PARSE__/'
    MakeDir(outpath)
    
    
    R = tb.tabarray(SVfile = maindir + 'State_Manifest_1.tsv')[['Region','IC','File']].aggregate(On=['Region','IC'],AggFunc = lambda x : '|'.join(x))
    
    GoodKeys = ['Category', 'description','footer', 'LastRevised']  

    Metadict = {}
    LenR = len(R)
    ColGroups = {}
    for (i,r) in enumerate(R):
        state = r['Region']
        indclass = r['IC']
        ps = r['File'].split('|')
        print state,indclass
        
        X = [tb.tabarray(SVfile = inpath + p[:-4] + '.tsv') for p in ps]
        for (j,x) in enumerate(X):
            x1 = x.deletecols(['Component'])
            x1.renamecol('Component Code','ComponentCode')
            x1.renamecol('Industry Code','IndustryCode')
            x1.renamecol('ParsedComponent','Component')
            x1.metadata = x.metadata
            x1.metadata['description'] = '.'.join(x1.metadata['description'].split('.')[1:]).strip()
            X[j] = x1
    
        if len(X) > 1:
            Z = tb.tab_join(X)
        else:
            Z = X[0]
           
        inds = sorted(uniqify(Z.coloring.pop('Categories')))
        Z.coloring['IndustryHierarchy'] = inds
        for ind in inds:
            Z.renamecol(ind,'Industry ' + ind)        
        
        Z.renamecol('State','Location')
        Z = Z.addcols(['{"s":' + repr(z['Location']) + ',"f":{"s":' + repr(z['FIPS']) + '}}' for z in Z],names = ['Location'])
        Z = Z.deletecols(['FIPS'])
        
        Z.coloring['timeColNames'] = Z.coloring['Data']
        Z.coloring.pop('Data')
        for j in range(len(Z.coloring['timeColNames'])):
            name = Z.coloring['timeColNames'][j]
            Z.renamecol(name,nea_dateparse(name))
            
        Z.metadata = {}
        for k in GoodKeys:
            h = [x.metadata[k] for x in X if k in x.metadata.keys()]
            if h:
                if isinstance(h[0],str):
                    Z.metadata[k] = ' '.join(uniqify(h))
                elif isinstance(h[0],list) or isinstance(h[0],tuple):
                    Z.metadata[k] = ' '.join(uniqify(ListUnion(h)))
                else:
                    print 'metadata type for key', k , 'in table', t, 'not recognized.'

        Z.coloring['labelColumns'] =  ['Location','Industry','Component']       
        for k in Z.coloring.keys():
            if k in ColGroups.keys():
                ColGroups[k] = uniqify(ColGroups[k] + Z.coloring[k])
            else:
                ColGroups[k] = Z.coloring[k]        
        
        Metadict[state] = Z.metadata
        
        Z = Z.addcols([len(Z)*[indclass], len(Z)*['S']],names=['IndClass','subcollections'])
        Z.saveSV(outpath + str(i) + '.tsv',metadata=['dialect','names','formats'])
    
    AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
    AllMeta = {}
    for k in AllKeys:
        if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
            AllMeta[k] = Metadict[Metadict.keys()[0]][k]

    Subcollections = {'S':AllMeta}
    Subcollections['S']['Title'] = 'GDP by State'
    
    del(Z)
        
    L = ['Metro_Preparsed.tsv']

    Metadict = {}
    for (i,l) in enumerate(L):
        print l
        X = tb.tabarray(SVfile = maindir + l)
        X.renamecol('industry_id','IndustryCode')
        X.renamecol('component_id','ComponentCode')
        X.renamecol('area_name','Metropolitan Area')
        X.renamecol('ParsedComponent','Component')
        X.renamecol('industry_name','Industry')

        if 'Categories' in X.coloring.keys(): 
            inds = sorted(uniqify(X.coloring.pop('Categories')))
            X.coloring['IndustryHierarchy'] = inds
            for ind in inds:
                X.renamecol(ind,'Industry ' + ind)       
            
        X1 = X.deletecols('component_name')
        X1 = X1.addcols(['{"m":' + repr(x['Metropolitan Area']) + ',"f":{"m":' + repr(x['FIPS']) + '}}' for x in X],names=['Location'])
        X1 = X1.deletecols(['FIPS','Metropolitan Area'])      
        X1.metadata = X.metadata
        X = X1  
        
        X.metadata['description'] = '--'.join(X.metadata['description'].split('--')[2:]).strip()

        for k in X.metadata.keys():
            if k not in GoodKeys: 
                X.metadata.pop(k)
       
        X.coloring['timeColNames'] = X.coloring['Data']
        X.coloring.pop('Data')
        for j in range(len(X.coloring['timeColNames'])):
            name = X.coloring['timeColNames'][j]
            X.renamecol(name,nea_dateparse(name))

        X.coloring['labelColumns'] = ['Location','Industry','Component']    
        for k in X.coloring.keys():
            if k in ColGroups.keys():
                ColGroups[k] = uniqify(ColGroups[k] + X.coloring[k])
            else:
                ColGroups[k] = X.coloring[k]                
        
        Metadict[l] = X.metadata
        X = X.addcols([['NAICS']*len(X),['M']*len(X)],names=['IndClass','subcollections'])
        X.saveSV(outpath + str(i+LenR) + '.tsv',metadata=['dialect','names','formats'])

    AllKeys = uniqify(ListUnion([k.keys() for k in Metadict.values()]))
    AllMeta = {}
    for k in AllKeys:
        if all([k in Metadict[l].keys() for l in Metadict.keys()]) and len(uniqify([Metadict[l][k] for l in Metadict.keys()])) == 1:
            AllMeta[k] = Metadict[Metadict.keys()[0]][k]
        
    Subcollections['M'] = AllMeta
    Subcollections['M']['Title'] = 'GDP by Metropolitan Area'
    
    ColGroups['IndustryHierarchy'].sort()
    IH = [tuple(ColGroups['IndustryHierarchy'][:i]) for i in range(1,len(ColGroups['IndustryHierarchy']) + 1)]

    AllMeta = {}
    AllMeta['topicHierarchy']  = ('agency','subagency','program','dataset','Category')
    AllMeta['uniqueIndexes'] = ['Location','IndustryCode','ComponentCode','IndClass']
    ColGroups['spaceColumns'] = ['Location']
    AllMeta['columnGroups'] = ColGroups
    AllMeta['dateFormat'] = 'YYYYqmm' 
    AllMeta['sliceCols'] = [['Location'] + IH ,['Location','Component'],['Component'] + IH]
    AllMeta['phraseCols'] = ['Component', 'IndClass','IndustryHiearchy','Units']    

    Subcollections[''] = AllMeta
        
    F = open(maindir+'__metadata.pickle','w')
    pickle.dump(Subcollections,F)
    F.close()   


@activate(lambda x : x[0] + 'Metro_Raw/allgmp.csv',lambda x : x[0] + 'Metro_Preparsed.tsv')
def Metro_PreParse1(maindir):
    f = maindir + 'Metro_Raw/allgmp.csv'
    savepath = maindir + 'Metro_Preparsed.tsv'
    [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, CategoryColumn='industry_name')

    X = X[(X['component_name'] != '') & (X['component_name'] != 'component_name')]
    p = re.compile('\(.*\)')
    ParsedComp = [p.sub('',x).strip() for x in X['component_name']]
    Units = [x[p.search(x).start()+1:p.search(x).end()-1] for x in X['component_name']]
    X = X.addcols([ParsedComp,Units],names=['ParsedComponent','Units'])


    metadata = {}
    metadata['keywords'] = ['Regional Economic Accounts']
    metadata['title'] = 'GDP by Metropolitan Area'
    metadata['description'] = 'Gross domestic product (GDP) for individual metropolitan statistical areas -- from the <a href="http://www.bea.gov/regional/gdpmetro/">GDP by Metropolitan Areas</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  Note that NAICS industry detail is based on the 1997 NAICS.  For more information on Metropolitan Statistical Areas, see the BEA website on <a href="http://www.bea.gov/regional/docs/msalist.cfm?mlist=45">Statistical Areas</a>.  Component units are as follows:  GDP by Metropolitan Area (millions of current dollars), Quantity Indexes for Real GDP by Metropolitan Area (2001=100.000), Real GDP by Metropolitan Area (millions of chained 2001 dollars), Per capita real GDP by Metropolitan Area (chained 2001 dollars).'
    metadata['Agency'] = 'DOC'
    metadata['Subagency'] = 'BEA'
    metadata['Type'] = 'Regional'
    metadata['Category'] = 'GDP by Metropolitan Area'
    metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'Region'])
    if footer:
        footer = [ff for ff in footer if ff.strip('\x1a')]
        metadata['footer'] = '\n'.join(footer)
        metadata['Notes'] = '\n'.join([x.split('Note:')[1].strip() for x in footer[:-1]])
        metadata['Source'] = footer[-1].split('Source:')[1].strip()
        metadata['LastRevised'] = metadata['Source'].split('--')[-1].strip()
    
    X.metadata = metadata
    X.metadata['unitcol'] = ['Units']
    X.metadata['labelcollist'] = ['industry_name','ParsedComponent']
    X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')
        

@activate(lambda x : ( x[0] + 'State_Manifest_1.tsv', x[0] + 'State_Raw/'),lambda x : x[0] + 'State_Preparsed/')
def State_PreParse1(maindir):
    target = maindir + 'State_Preparsed/'
    sourcedir = maindir + 'State_Raw/'
    manifest = maindir + 'State_Manifest_1.tsv'
    
    MakeDir(target)
    M = tb.tabarray(SVfile=manifest)
    for mm in M:
        FIPS,Region,IC,file = mm['FIPS'],mm['Region'],mm['IC'],mm['File']

        f = sourcedir + file
        savepath = target + file[:-4] + '.tsv'
        [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='\\', FootnotesFunction=GetFootnotes2, CategoryColumn='Industry')
    
        p = re.compile('\(.*\)')
        ParsedComp = [p.sub('',x).strip() for x in X['Component']]
        Units = [x[p.search(x).start()+1:p.search(x).end()-1] for x in X['Component']]
        X = X.addcols([ParsedComp,Units],names=['ParsedComponent','Units'])
            
        metadata = {}

        Years = [int(y[:4]) for y in X.coloring['Data'] if y[:4].isdigit()]
        
        metadata['keywords'] = ['Regional Economic Accounts']
        metadata['description'] = 'Gross domestic product (GDP) for a single state or larger region, ' + Region + ' using the ' + IC + ' industry classification.  This data comes from the <a href="http://www.bea.gov/regional/gsp/">GDP by State</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  Component units are as follows:  Gross Domestic Product by State (millions of current dollars), Compensation of Employees (millions of current dollars), Taxes on Production and Imports less Subsidies (millions of current dollars), Gross Operating Surplus (millions of current dollars), Real GDP by state (millions of chained 2000 dollars), Quantity Indexes for Real GDP by State (2000=100.000), Subsidies (millions of current dollars), Taxes on Production and Imports (millions of current dollars), Per capita real GDP by state (chained 2000 dollars).'
        metadata['Agency'] = 'DOC'
        metadata['Subagency'] = 'BEA'
        metadata['Type'] = 'Regional'
        metadata['Category'] = 'GDP by State'
        metadata['IndustryClassification'] = IC
        metadata['Region'] = Region
        metadata['FIPS'] = X['FIPS'][0]
        metadata['Categories'] = ', '.join(['Agency', 'Subagency', 'Type', 'Category', 'IndustryClassification', 'Region', 'TimePeriod'])
        if footer:
            footer = CleanLinesForMetadata(footer)
            metadata['footer'] = '\n'.join(footer)
            [source] = footer
            metadata['Source'] = source.split('Source:')[1].strip()
        
        X.metadata = metadata
        X.metadata['unitcol'] = ['Units']
        X.metadata['labelcollist'] = ['Industry','ParsedComponent']
        X.saveSV(savepath, metadata=True, comments='#', delimiter='\t')


def GetStateManifest(maindir):
    wget('http://www.bea.gov/regional/gsp/',maindir + 'State_Index.html')
    page = maindir + 'State_Index.html'
    Soup = BeautifulSoup(open(page),convertEntities=BeautifulStoneSoup.HTML_ENTITIES)
    O = Soup.findAll('select',{'name':'selFips'})[0].findAll('option')
    L = [(str(dict(o.attrs)['value']),Contents(o).strip()) for o in O]
    tb.tabarray(records = L,names = ['FIPS','Region']).saveSV(maindir + 'State_Manifest.tsv',metadata=True)
    
    
@activate(lambda x : 'http://www.bea.gov/national/nipaweb/csv/NIPATable.csv',lambda x : x[0])
def RegionalGDP_initialize(maindir):
    MakeDirs(maindir)
    GetStateManifest(maindir)
    
@activate(lambda x : x[0] + 'State_Manifest.tsv',lambda x : (x[0] + 'State_Raw/',x[0] + 'State_Manifest_1.tsv'))
def DownloadStateFiles(maindir):
    connection = pm.Connection()
    incremental = REG_NAME in connection['govdata'].collection_names()
    
    X = tb.tabarray(SVfile = maindir + 'State_Manifest.tsv')
    rawdir = maindir + 'State_Raw/'
    MakeDir(rawdir)
    Recs = []
    for x in X:
        fips = x['FIPS']
        if fips != 'ALL':
            Region = x['Region']
            Recs.append((fips,Region,'NAICS','NAICS_' + fips+ '.csv'))

            if incremental:
                selYears = 'selYears=2008'   #this could be improved
            else:
                selYears = 'selYears=ALL'
            Postdata= 'series=NAICS&selTable=ALL&selFips=' + str(fips) + '&selLineCode=ALL&' + selYears + '&querybutton=Download+CSV'
            WgetMultiple('http://www.bea.gov/regional/gsp/action.cfm', rawdir + 'NAICS_' + fips + '.csv', opstring = '--post-data="' + Postdata + '"')
            
            if not incremental:
                Recs.append((fips,Region,'SIC','SIC_1_' + fips+ '.csv'))
                Recs.append((fips,Region,'SIC','SIC_2_' + fips+ '.csv'))            
                selYears = '&'.join(['selYears=' + str(y) for y in range(1963,1983)])
                Postdata= 'series=SIC&selTable=ALL&selFips=' + str(fips) + '&selLineCode=ALL&' + selYears + '&querybutton=Download+CSV'
                WgetMultiple('http://www.bea.gov/regional/gsp/action.cfm', rawdir + 'SIC_1_' + fips + '.csv', opstring = '--post-data="' + Postdata + '"')
                selYears = '&'.join(['selYears=' + str(y) for y in range(1983,1998)])
                Postdata= 'series=SIC&selTable=ALL&selFips=' + str(fips) + '&selLineCode=ALL&' + selYears + '&querybutton=Download+CSV'
                WgetMultiple('http://www.bea.gov/regional/gsp/action.cfm', rawdir + 'SIC_2_' + fips + '.csv', opstring = '--post-data="' + Postdata + '"')
        
    tb.tabarray(records = Recs,names = ['FIPS','Region','IC','File']).saveSV(maindir + 'State_Manifest_1.tsv',metadata=True)    

@activate(lambda x : x[0] + 'GDPMetro.zip',lambda x : x[0] + 'Metro_Raw/')
def DownloadMetroFiles(maindir):
    wget('http://www.bea.gov/regional/zip/GDPMetro.zip',maindir + 'GDPMetro.zip')
    os.system('unzip -d ' + maindir + 'Metro_Raw ' + maindir + 'GDPMetro.zip')

REG_NAME = 'BEA_RegionalGDP'

     
def reg_trigger():
    connection = pm.Connection()
    
    incremental = REG_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        
REG_PARSER = govdata.core.GovParser(REG_NAME,
                                        govdata.core.CsvParser,
                                        downloader = [(RegionalGDP_initialize,'initialize'),
                                                      (GetStateManifest,'state_manifest'),
                                                      (DownloadStateFiles,'get_state_files'),
                                                      (DownloadMetroFiles,'get_metro_files'),
                                                      (State_PreParse1,'state_preparse1'),
                                                      (Metro_PreParse1,'metro_preparse1'),
                                                      (RegionalGDP_Preparse2,'preparse2')],
                                        trigger = reg_trigger,
                                        incremental=True)



#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=


PI_NAME = 'BEA_PersonalIncome'

def PI_dateparse(x):
    d = x.split('.')
    if len(d) == 1:
        return d[0] + 'X' + 'XX' 
    else:
        return d[0] + str(int(d[1]))  + 'XX'

@activate(lambda x : 'http://www.bea.gov/regional/sqpi/action.cfm?zipfile=/regional/zip/sqpi.zip',lambda x : x[0] + 'sqpi_raw/')
def SQPI_downloader(maindir):

    MakeDirs(maindir + 'sqpi_raw/')
    wget('http://www.bea.gov/regional/sqpi/action.cfm?zipfile=/regional/zip/sqpi.zip',maindir + 'sqpi_raw/sqpi.zip')
    os.system('cd ' + maindir + 'sqpi_raw; unzip sqpi.zip; rm sqpi.zip')
            
    t = 'SQ1'
    selYears = range(1969,2020)
        
    #postdata = '--post-data="selLineCode=10&rformat=Download&selTable=' + t + '&selYears=' + ','.join(map(str,selYears)) + '"'
    #wget('http://www.bea.gov/regional/sqpi/drill.cfm',maindir + 'sqpi_raw/' + t + '.csv',opstring = postdata)
        
            
@activate(lambda x : 'http://www.bea.gov/regional/spi/action.cfm',lambda x : x[0] + 'sapi_raw/')
def SAPI_downloader(maindir):
    target = maindir + 'sapi_raw/'
    MakeDirs(target)
    src = 'http://www.bea.gov/regional/spi/action.cfm'
    for x in ['sa','sa_sum','sa_naics','sa_sic']:
        postdata = '--post-data="archive=' + x + '&DownloadZIP=Download+ZIP"'           
        wget(src,target + x + '.zip', opstring=postdata)
        os.system('unzip -d ' + target + x + ' ' + target + x + '.zip')

@activate(lambda x : 'http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line', lambda x : (x[0] + 'lapi_codes/',x[0] + 'lapi_codes.tsv'))
def get_line_codes(maindir):
    target = maindir + 'lapi_codes/'
    MakeDirs(target)
    wget('http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line',target + '/index.html')
    Soup = BeautifulSoup(open(target + 'index.html'))
    O = Soup.findAll('select',id='selTable')[0].findAll('option')
    O1 = [(str(dict(o.attrs)['value']),Contents(o).split(' - ')[0].strip(),Contents(o).split(' - ')[1].strip()) for o in O]
    g = 'http://www.bea.gov/regional/reis/default.cfm#step2'
    Recs = []
    for (op,n,m) in O1:
        s = '--post-data="singletable=' + op + '&nextarea=Next+%E2%86%92&section=next&selTable=Single+Line&areatype=ALLCOUNTY&catable_name=' + op + '"'
        wget(g,target + op + '.html',opstring=s)
        Soup = BeautifulSoup(open(target + op + '.html'))
        O = Soup.findAll('select',{'name':'selLineCode'})[0].findAll('option')
        Recs += [(op,m,str(dict(o.attrs)['value']),Contents(o).split(' - ')[1].strip()) for o in O]
    tb.tabarray(records = Recs,names = ['Table','TableDescr','Code','CodeDescr']).saveSV(maindir + 'lapi_codes.tsv',metadata=True)
    
@activate(lambda x : x[0] + 'lapi_codes.tsv',lambda x : x[0] + 'lapi_codes_processed.tsv')
def process_line_codes(maindir):
    inpath = maindir + 'lapi_codes.tsv'
    outpath = maindir + 'lapi_codes_processed.tsv'
    
    X = tb.tabarray(SVfile = inpath)
    Vals = []
    v = ()
    pi = 0
    for x in X:
        ci = x['CodeDescr'].count('&nbsp;') / 2
        t = x['CodeDescr'].split('&nbsp;')[-1]
        if ci <= pi:
            v = v[:ci] + (t,)
        elif ci > pi:
            v = v + (t,)
        Vals.append(v)
        pi = ci
   
    m = max(map(len,Vals))
    
    Vals = [v + (m - len(v))*('',) for v in Vals]
    NewX = tb.tabarray(records = Vals,names = ['Level_' + str(i) for i in range(m)])
    X = X.colstack(NewX)
    X.coloring['Hierarchy'] = list(NewX.dtype.names)
    X.saveSV(outpath,metadata = True)
    
@activate(lambda x : (x[0] + 'lapi_codes.tsv','http://www.bea.gov/regional/reis/drill.cfm'),lambda x : x[0] + 'lapi_raw/' + x[1] + '_' + x[2] + '/')
def LAPI_downloader(maindir,table,level):
    target = maindir + 'lapi_raw/' + table + '_' + level + '/'
    MakeDirs(target)
    X = tb.tabarray(SVfile = maindir + 'lapi_codes.tsv')
    X = X[X['Table'] == table]
    connection = pm.Connection()
    incremental = PI_NAME in connection['govdata'].collection_names()
    if incremental:
        selYears = range(2008,2012)
    else:
        selYears = range(1969,2012)
    for x in X['Code']:
        s = '--post-data="areatype=' + level + '&SelLineCode=' + x + '&rformat=Download&selTable=Single+Line&catable_name=' + table + '&' + '&'.join(map(lambda y : 'selYears='+str(y),selYears)) + '"'
        print 'Getting:', s
        wget('http://www.bea.gov/regional/reis/drill.cfm',target + x + '.csv',opstring = s)

@activate(lambda x : 'http://www.bea.gov/regional/docs/footnotes.cfm', lambda x : (x[0] + 'footnotes/',x[0] + 'footnotes.tsv'))
def get_footnotes(maindir):
    target = maindir + 'footnotes/'
    MakeDirs(target)
    index = target + 'index.html'
    wget('http://www.bea.gov/regional/docs/footnotes.cfm',index)
    Soup = BeautifulSoup(open(index))
    A = Soup.findAll(lambda x : x.name == 'a' and  'footnotes.cfm' in str(x))
    Tables = [Contents(a).strip() for a in A]
    Recs = []
    for t in Tables:
        wget('http://www.bea.gov/regional/docs/footnotes.cfm?tablename=' + t, target + t + '.html')
        Soup = BeautifulSoup(open(target + t + '.html'))
        caption = Contents(Soup.findAll('caption')[0]).split(' - ')[-1].strip()
        TR = Soup.findAll('caption')[0].findParent().findAll('tr')
        Recs += [(t,caption,Contents(tr.findAll('strong')[0]).replace('\t',' '), Contents(tr.findAll('td')[-1]).replace('\t',' ')) for tr in TR]
    tb.tabarray(records = Recs, names = ['Table','TableDescr','Number','Text']).saveSV(maindir + 'footnotes.tsv',metadata=True)

   
def pi_trigger():
    connection = pm.Connection()
    
    incremental = PI_NAME in connection['govdata'].collection_names()
    if incremental:
        return 'increment'
    else:
        return 'overall'
        
        
@activate(lambda x : (x[0] + 'sqpi_raw/',x[0] + 'footnotes.tsv'),lambda x : x[0] + '__PARSE__/sqpi/')
def SQPI_preparse(maindir):
    sourcedir = maindir + 'sqpi_raw/'
    target =  maindir + '__PARSE__/sqpi/'
    MakeDirs(target)
    
    Y = tb.tabarray(SVfile = maindir + 'footnotes.tsv')
    filelist = [sourcedir + x for x in  listdir(sourcedir) if x.endswith('.csv')]
    
    for (i,f) in enumerate(filelist):
        print f

        temppath = target + f.split('/')[-1]
        strongcopy(f,temppath)
        F = open(temppath,'rU').read().strip().split('\n')
        sline = [j for j in range(len(F)) if F[j].startswith('"Source:')][0]
        nline = [j for j in range(len(F)) if F[j].startswith('"State FIPS')][0]
        s = F[nline] + '\n' + '\n'.join([F[j] for j in range(len(F)) if j not in [sline, nline]]) + '\n' + F[sline]
        F = open(temppath,'w')
        F.write(s)
        F.close()

        [X, header, footer, keywords] = BEA_Parser(temppath, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn='Line Title')
        delete(temppath)
        
        table = X['Table'][0]
        X = X.deletecols(['First Year']).addcols(len(X)*[table + ',SQ'],names=['subcollections'])
        X = X.addcols(['{"s":' + repr(x) + ',"f":{"s":' + repr(f) + '}}' for (f,x) in X[['State FIPS','State Name']]],names = ['Location'])
        X = X.deletecols(['State FIPS','State Name'])    

        subj = sorted(uniqify(X.coloring.pop('Categories')))
        X.coloring['SubjectHierarchy'] = subj
        for s in subj:
            X.renamecol(s,'Subject ' + s)
        
        X.renamecol('Line Code','LineCode')
        X.renamecol('Line Title','Line')
        X.renamecol('Line Title Footnotes', 'Line Footnotes')

        TimeColNames = X.coloring.get('Data')       
        for n in TimeColNames:
            X.renamecol(n,PI_dateparse(n))    
        X.coloring['TimeColNames'] = X.coloring.pop('Data')

        X.saveSV(target + str(i) + '.tsv',metadata=['dialect','names','formats','coloring'])
        

                
@activate(lambda x : x[0] + 'sapi_raw/',lambda x : x[0] + '__PARSE__/sapi/')
def SAPI_preparse(maindir):
    sourcedir = maindir + 'sapi_raw/'
    target =  maindir + '__PARSE__/sapi/'
    MakeDirs(target)
 
   
    M = [x for x in RecursiveFileList(sourcedir) if x.endswith('.csv')]

    for (i,f) in enumerate(M):
        print 'Processing', i, f
        
        temppath = target + f.split('/')[-1]
        strongcopy(f,temppath)
        F = open(temppath,'rU').read().strip().split('\n')
        sline = [j for j in range(len(F)) if F[j].startswith('"Source:')][0]
        nline = [j for j in range(len(F)) if F[j].startswith('"State FIPS')][0]
        s = F[nline] + '\n' + '\n'.join([F[j] for j in range(len(F)) if j not in [sline, nline]]) + '\n' + F[sline]
        F = open(temppath,'w')
        F.write(s)
        F.close()

        [X, header, footer, keywords] = BEA_Parser(temppath, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn='Line Title')
        delete(temppath)
        
        
        for name in X['Data'].dtype.names:
            if name.endswith('p'):
                newname = name.strip('p')
                X.renamecol(name,newname)
              
        table = X['Table'][0] 
        
        Summary = f.split('/')[-2] == 'sa_sum'
        if Summary:             
            unitsdict = {'personal income': 'Thousands of dollars', 'population': 'Number of persons', 'per capita personal income': 'Dollars'}
            units = [unitsdict[i.lower().replace('disposable ', '').strip()] for i in X['Line Title']]          
            summarycol = ['Aggregate']*len(X)
            X = X[['Info','Data','Footnotes']].addcols([units,summarycol],names=['Units','Subject Level_1'])
     
        X = X.deletecols(['First Year']).addcols(len(X)*[table + ',SA' + (',SA_S' if Summary else '')],names=['subcollections'])
        X = X.addcols(['{"s":' + repr(sname) + ',"f":{"s":' + repr(fips) + '}}' for (fips,sname) in X[['State FIPS','State Name']]],names = ['Location'])
        X = X.deletecols(['State FIPS','State Name'])   
        
        X.coloring['SubjectHierarchy'] = ['Subject Level_1']
                
        X.renamecol('Line Code','LineCode')
        X.renamecol('Line Title','Line')
        X.renamecol('Line Title Footnotes', 'Line Footnotes')
        
        TimeColNames = X.coloring.get('Data')
        for n in TimeColNames:
            X.renamecol(n,PI_dateparse(n))
        X.coloring['TimeColNames'] = X.coloring.pop('Data')
        
        X.saveSV(target + str(i) + '.tsv',metadata=['dialect','names','formats','coloring'])
            
    
def loc_processor(f,x,level):   
    if level == 'ALLCOUNTY':
        return '{"c":' + repr(','.join(x.split(',')[:-1]).strip()) + ',"S":' + repr( x.split(',')[-1].strip()) +',"f":{"c":' + repr(f[2:]) + ',"s":' + repr(f[:2]) + '}}' 
    elif level ==  'STATE':
        return '{"s":' + REPR(x) + ',"f":{"s":' + repr(f[:2]) + '}}' 
    elif level == 'METRO':
        return '{"m":' + REPR(x) + ',"f":{"m":' + repr(f) + '}}' 
    elif level == 'CSA':
        return '{"b":' + REPR(x) + ',"f":{"b":' + repr(f[2:]) + '}}' 
    elif level == 'MDIV':
        return '{"B":' + REPR(x) + ',"f":{"B":' + repr(f) + '}}' 
    elif level == 'ECON':
        return '{"X":' + REPR(x) + ',"f":{"X":' + repr(f) + '}}' 
        
def REPR(x):
    try:
        x.decode('utf-8')
    except UnicodeDecodeError:
        x = x.decode('latin-1').encode('utf-8')
        
    return repr(x)

DRF =  re.compile('\d{4}-\d{4}')
DRFS =  re.compile('^\d{4}-\d{4}$')
def daterangecorrect(x):
    if DRF.search(x):
        if DRFS.search(x):
            return x.split('-')[-1]
    else:
        return x
        
@activate(lambda x : (x[0] + 'lapi_raw/' + x[1] + '_' + x[2] + '/',x[0] + 'lapi_codes_processed.tsv'),lambda x : x[0] + '__PARSE__/lapi/'+x[1] + '_' + x[2] + '/')
def LAPI_preparse(maindir,table,level):
    sourcedir = maindir + 'lapi_raw/' + table + '_' + level + '/'
    target =  maindir + '__PARSE__/lapi/' + table + '_' + level + '/'
    MakeDirs(target)
    
    Codes = tb.tabarray(SVfile = maindir + 'lapi_codes_processed.tsv')
    Codes = Codes[Codes['Table'] == table]
    hr = Codes['Hierarchy'].dtype.names
    subjnames = ['Subject Level_' + str(int(n.split('_')[-1]) + 1) for n in hr]
    
    for (i,c) in enumerate(Codes):
        linecode,line = c['Code'],c['CodeDescr']
        
        f = sourcedir + linecode + '.csv'
        [X, header, footer, keywords] = BEA_Parser(f, headerlines=1, FootnoteSplitter='/', FootnotesFunction=GetFootnotes, CategoryColumn=None)
        NewData = [(name,daterangecorrect(name)) for name in X['Data'].dtype.names]
        for (name,newname) in NewData:
            if name != newname:
                if newname:
                    X.renamecol(name,newname)
                else:
                    X.coloring['Data'].remove(name)
        
        title = X.dtype.names[0]
        if len(title.split('/')) > 1:
            LineFootnote = title.split('/')[0].split()[-1]
            title = ' '.join(title.split('/')[0].split()[:-1]).strip()
        else:
            LineFootnote = None
        X.renamecol(X.dtype.names[0], 'LineCode')
        
        id = table + '_' + linecode
      
        subjdata = [list(x) for x in zip(*[tuple(Codes['Hierarchy'][i])]*len(X))]

        X = X.addcols([[table]*len(X),[line]*len(X),[table + ',LA,' + id]*len(X)] + subjdata ,names=['Table','Line','subcollections'] + subjnames) 
        X = X.addcols([loc_processor(fips,aname,level) for (fips,aname) in X[['FIPS','AreaName']]],names = ['Location'])
        X = X.deletecols(['FIPS','AreaName'])

        TimeColNames = X.coloring['Data']
        for n in TimeColNames:
            X.renamecol(n,PI_dateparse(n))    
        X.coloring['timeColNames'] = X.coloring.pop('Data')
        X.coloring['SubjectHierarchy'] = subjnames
        
        X.metadata = {'LineFootnote':LineFootnote,'Table':table,'Line':line,'LineCode':linecode}

        X.saveSV(target + str(i) + '.tsv',metadata=True)
        

@activate(lambda x : x[0] + 'footnotes.tsv',lambda x : x[0] + '__metadata.pickle')
def PI_metadata(maindir):
    Y = tb.tabarray(SVfile = maindir + 'footnotes.tsv')
   
    Metadata = {}
    
    TFN = Y.aggregate(On=['Table'],AggList=[('TableDescr',lambda x : x[0]),('Footnote',lambda x : '\n'.join([w +': ' + z for (w,z) in zip(x['Number'],x['Text'])]),['Number','Text'])],KeepOthers=False)
    for x in TFN:
        Metadata[x['Table']] = {'Title':x['TableDescr'], 'Footnotes': x['Footnote']}
    
    Metadata['SQ'] = {'Title':'State Quarertly Persona Income','Description':'<a href="http://www.bea.gov/regional/sqpi/">State Quarterly Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.', 'Units':'Millions of dollars, seasonally adjusted at annual rates'}
    
    Metadata['SA_S'] = {'Title': 'State Annual Summary'}
    Metadata['SA'] = {'Title': 'State Annual Personal Income', 'Description' :  '<a href="http://www.bea.gov/regional/spi/">State Annual Personal Income</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  U.S. DEPARTMENT OF COMMERCE--ECONOMICS AND STATISTICS ADMINISTRATION BUREAU OF ECONOMIC ANALYSIS--REGIONAL ECONOMIC INFORMATION SYSTEM STATE ANNUAL TABLES 1969 - 2008 for the states and regions of the nation September 2009 These files are provided by the Regional Economic Measurement Division of the Bureau of Economic Analysis. They contain tables of annual estimates (see below) for 1969-2008 for all States, regions, and the nation. State personal income estimates, released September 18, 2009, have been revised for 1969-2008 to reflect the results of the comprehensive revision to the national income and product accounts released in July 2009 and to incorporate newly available state-level source data. For the year 2001 in the tables SA05, SA06, SA07, SA25, and SA27, the industry detail is available by division-level SIC only. Tables based upon the North American Industry Classification System (NAICS) are available for 2001-07. Newly available earnings by NAICS industry back to 1990 were released on September 26, 2006.   For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.   Historical estimates 1929-68 will be updated in the next several months. TABLES The estimates are organized by table. The name of the downloaded file indicates the table. For example, any filename beginning with "SA05" contains information from the SA05 table. With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars. SA04 - State income and employment summary (1969-2008) SA05 - Personal income by major source and earnings by industry (1969-2001, 1990-2008) SA06 - Compensation of employees by industry (1998-2001, 2001-08) SA07 - Wage and salary disbursements by industry (1969-01, 2001-08) SA25 - Total full-time and part-time employment by industry (1969-2001, 2001-08) SA27 - Full-time and part-time wage and salary employment by industry (1969-2001, 2001-08) SA30 - State economic profile (1969-08) SA35 - Personal current transfer receipts (1969-08) SA40 - State property income (1969-2008) SA45 - Farm income and expenses (1969-2008) SA50 - Personal current taxes (this table includes the disposable personal income estimate) (1969-08) DATA (*.CSV) FILES The files containing the estimates (data files) are in comma-separated-value text format with textual information enclosed in quotes.  (L) Less than $50,000 or less than 10 jobs, as appropriate, but the estimates for this item are included in the total. (T) SA05N=Less than 10 million dollars, but the estimates for this item are included in the total. SA25N=Estimate for employment suppressed to cover corresponding estimate for earnings. Estimates for this item are included in the total. (N) Data not available for this year. If you have any problems or comments on the use of these data files call or write: Regional Economic Information System Bureau of Economic Analysis (BE-55) U.S. Department of Commerce Washington, D.C. 20230 Phone (202) 606-5360 FAX (202) 606-5322 E-Mail: reis@bea.gov'}
    
    Metadata['LA'] = {'Title':'Local Area Personal Income', 'Description' : 'Local Area Personal Income data for all US counties, from the <a href="http://www.bea.gov/regional/reis/default.cfm?selTable=Single%20Line">Local Area Personal Income "Single Line of data for all counties"</a> data set under the <a href="http://www.bea.gov/regional/index.htm">Regional Economic Accounts</a> section of the <a href="http://www.bea.gov/">Bureau of Economic Accounts (BEA)</a> website.  For more information on the industry classifications, see the BEA web pages on  <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=NAICS">NAICS (1997-2008)</a> and <a href="http://www.bea.gov/regional/definitions/nextpage.cfm?key=SIC">SIC (1963-1997)</a>.  With the exception of per capita estimates, all dollar estimates are in thousands of dollars. All employment estimates are number of jobs. All dollar estimates are in current dollars.'}

    AllMeta = {} 
    AllMeta['topicHierarchy']  = ('Agency','Subagency','Dataset','Category','Subcategory','SubjectHierarchy')
    AllMeta['uniqueIndexes'] = ['Location','Table','LineCode']
    AllMeta['columnGroups'] = {'SpaceColumns' : ['Location'],'SubjectHierarchy':['Subject Level_' + str(i) for i in range(1,8)]}
    AllMeta['dateFormat'] = 'YYYYqmm'
    
    SH  = AllMeta['columnGroups']['SubjectHierarchy']
    SH.sort()
    SHL = [tuple(SH[:i]) for i in range(1,len(SH) + 1)] 
    
    AllMeta['sliceCols'] = [['Location','Table'] + SHL]
    AllMeta['phraseCols'] = ['Table','SubjectHierarchy','Line','LineCode']  
    Metadata[''] = AllMeta
    
    F = open(maindir + '__metadata.pickle','w')
    pickle.dump(Metadata,F)
    F.close()
    

class pi_parser(govdata.core.CsvParser):

    def refresh(self,file):
        print 'refreshing: ', file
        self.Data = tb.tabarray(SVfile = file,verbosity = 0)
        self.IND = 0
       
        for k in ['TimeColNames']:
            self.metadata['']['ColumnGroups'][k] = uniqify(self.metadata['']['ColumnGroups'].get(k,[]) + self.Data.coloring.get(k,[]))
        
        if 'LineFootnote' in self.Data.metadata.keys():
            id = self.Data.metadata['Table'] + '_' + self.Data.metadata['LineCode']
            self.metadata[id] = {'Title':self.Data.metadata['Line'], 'Footnote': self.Data.metadata['LineFootnote']}
         

def construct_personal_income_parser():  
    D = [((get_footnotes,'footnotes'),()),
         ((get_line_codes,'lapi_line_codes'),()),
         ((process_line_codes,'process_line_codes'),()),
         ((SAPI_downloader,'sapi_raw'),()),
         ((SQPI_downloader,'sqpi_raw'),())] 
    D += [((SAPI_preparse,'sapi_preparse'),()),((SQPI_preparse,'sqpi_preparse'),())] 
    
    tables = ['CA1-3', 'CA04', 'CA05N', 'CA25N', 'CA05', 'CA06', 'CA06N', 'CA25', 'CA30', 'CA34', 'CA35','CA45']
    areatypes = ['ALLCOUNTY', 'STATE','METRO','MDIV','CSA']
    D += [((LAPI_downloader,'lapi_' + t.replace('-','_') + '_' + a + '_raw'),(t,a)) for t in tables for a in areatypes]
    D += [((LAPI_preparse,'lapi_' + t.replace('-','_') + '_' + a + '_preparse'),(t,a)) for t in tables for a in areatypes]
    
    D += [((PI_metadata,'make_metadata'),())]
    
    (downloader, downloadArgs) = zip(*D)
    downloader = list(downloader)
    downloadArgs = list(downloadArgs)
    
    return govdata.core.GovParser(PI_NAME,pi_parser,downloader = downloader, downloadArgs = downloadArgs,trigger = pi_trigger,incremental=True)
    
    
PI_PARSER = construct_personal_income_parser()

       
#=-=-=-=-=-=-=-=-=-=-=-=-International transactions

@activate(lambda x : 'http://www.bea.gov/international/xls/table1.xls',lambda x : x[0] + 'transactions.xls')
def get_intl_transactions(maindir):
    wget('http://www.bea.gov/international/xls/table1.xls',maindir + 'transactions.xls')
 
@activate(lambda x : 'http://www.bea.gov/international/bp_web/startDownload.asp?dlSelect=tables%2FCSV%2FITA-CSV.zip',lambda x : (x[0] + 'detailed_transactions.zip',x[0] + 'detailed_transactions/'))
def get_intl_transactions_detailed(maindir):
    wget('http://www.bea.gov/international/bp_web/startDownload.asp?dlSelect=tables%2FCSV%2FITA-CSV.zip',maindir + 'detailed_transactions.zip')
    os.system('unzip -d ' + maindir + 'detailed_transactions/ ' + maindir + 'detailed_transactions.zip')

@activate(lambda x : ('http://www.bea.gov/methodologies/_pdf/summary_of_major_revisions_to_the_us_international_accounts_1976_2009.pdf','http://www.bea.gov/scb/pdf/2010/02%20February/0210_guide.pdf'),lambda x : x[0] + '__FILES__/')
def get_intl_transactions_files(maindir):
    MakeDir(maindir + '__FILES__/')
    wget('http://www.bea.gov/methodologies/_pdf/summary_of_major_revisions_to_the_us_international_accounts_1976_2009.pdf',maindir + '__FILES__/international_transactions_revisions.pdf')
    wget('http://www.bea.gov/scb/pdf/2010/02%20February/0210_guide.pdf',maindir + '__FILES__/international_transactions_guide.pdf')

ITRANS_NAME = 'BEA_InternationalTransactions'
ITRANS_PARSER = govdata.core.GovParser(ITRANS_NAME,
                                       None,
                                       downloader = [(MakeDirs,'initialize'),
                                                     (get_intl_transactions,'get_transactions'),
                                                     (get_intl_transactions_detailed,'get_transactions_detailed'),
                                                     (get_intl_transactions_files,'get_files')]
                                       )



#=-=-=-=-=-=-=-=-=-=-=-=-International trade


@activate(lambda x : 'http://www.bea.gov/newsreleases/international/trade/trad_time_series.xls',lambda x : x[0] + 'trade.xls')
def get_intl_trade(maindir):
    wget('http://www.bea.gov/newsreleases/international/trade/trad_time_series.xls',maindir + 'trade.xls')

def get_intl_trade_services_detailed(maindir):
    pass
    #annoyingly dispersed:    http://www.bea.gov/international/international_services.htm#summaryandother
    
    
@activate(lambda x : 'http://www.bea.gov/international/zip',lambda x : x[0] + 'trade_detailed/')
def get_intl_trade_goods_detailed(maindir):
    MakeDirs(maindir + 'trade_detailed/')
    getzip('http://www.bea.gov/international/zip/IDS0008Hist.zip',maindir + 'trade_detailed/detailed_trade_goods_historical_quarterly.zip')
    getzip('http://www.bea.gov/international/zip/IDS0008.zip',maindir + 'trade_detailed/detailed_trade_goods_current_quarterly.zip')
    getzip('http://www.bea.gov/international/zip/IDS0182.zip',maindir + 'trade_detailed/detailed_trade_goods_current_monthly.zip')
    getzip('http://www.bea.gov/international/zip/IDS0182Hist.zip',maindir + 'trade_detailed/detailed_trade_goods_historical_monthly.zip')

ITRADE_NAME = 'BEA_InternationalTrade'
ITRADE_PARSER = govdata.core.GovParser(ITRADE_NAME,
                                       None,
                                       downloader = [(MakeDirs,'initialize'),
                                                     (get_intl_trade,'get_trade'),
                                                     (get_intl_trade_goods_detailed,'get_trade_goods_detailed')
                                                    ]
                                       )


#=-=-=-=-=-=-=-=-=-=-=-=-International investment aggregates

    
@activate(lambda x : 'http://www.bea.gov/international/xls',lambda x : x[0] + 'aggregate_data/')
def get_intl_investment(maindir):
    MakeDirs(maindir + 'aggregate_data/')
    wget('http://www.bea.gov/international/xls/intinv08_t2.xls',maindir + 'aggregate_data/investment_2.xls')
    wget('http://www.bea.gov/international/xls/intinv08_t3.xls',maindir + 'aggregate_data/investment_3.xls')


#=-=-=-=-=-=-=-=-=-=-=-=-International investment
from mechanize import Browser
from ClientForm import ControlNotFoundError


@activate(lambda x : x[0] + x[1] + '/Manifest.tsv',lambda x : x[0] + '__PARSE__/' + x[1] + '/')
def get_ii_data(maindir,tag):
    X = tb.tabarray(SVfile = maindir + tag + '/Manifest.tsv')
    target = maindir + '__PARSE__/' + tag + '/'
    MakeDirs(target)
    for x in X:
        wget('http://www.bea.gov/international/ii_web/' + x['URL'],target +  x['Entity'] + '_' + x['Series'] + '_' + x['RowType'] + '_' + x['IndType'] + '.csv')
    
    
@activate(lambda x : (x[0] + x[1] + '/urls/',x[0] + x[1] + '/RowTypeEncodings.tsv',x[0] + x[1] +'/SeriesEncodings.tsv'),lambda x : x[0] + x[1] + '/Manifest.tsv')
def make_ii_manifest(maindir,tag):
    sourcedir = maindir + tag + '/urls/'
    L = [x for x in listdir(sourcedir) if x.endswith('.txt')]
    Recs =  [l.split('.')[0].split('_') + [open(sourcedir + l).read().strip()] for l in L]
    R = tb.tabarray(records = Recs,names = ['Entity','Series','RowType','IndType','URL'])
    X = tb.tabarray(SVfile = maindir + tag + '/SeriesEncodings.tsv')
    X1 = tb.tabarray(SVfile = maindir + tag + '/RowTypeEncodings.tsv')
    Z = R.join(X1,keycols=['Series','RowType','Entity']).join(X,keycols=['Series','Entity'])
    Z.saveSV(maindir + tag + '/Manifest.tsv',metadata=True)


@activate(lambda x : x[1],lambda x : x[0] + x[2] + '/')
def get_ii_urls(maindir,url,tag):
    br = Browser()
    br.open(url)
    target = maindir + tag + '/'
    MakeDirs(target)
    MakeDirs(target + 'urls/')
    handle_ii_br(br,target)
    

def getfinaldata(br,url,maindir):
    try:
        entitytypeid = br['entitytypeid']
    except AttributeError:
        entitytypeid = ''
    seriesid = br['seriesid']
    rowtypeid = br['rowtypeid']
    indtypeid = br['indtypeid']
    
    F = open(maindir + '/urls/' + entitytypeid + '_' + seriesid + '_' + rowtypeid + '_' + indtypeid + '.txt','w')
    F.write(url)
    F.close()
     
def handle_ii_br(br,maindir):
    print br
    L = br.links()
    URL = [l.url for l in L]
    if any(['timeseries_to_csv.cfm?' in url for url in URL]):
        br.select_form(nr=1)
        getfinaldata(br,[url for url in URL if 'timeseries_to_csv.cfm?' in url][0],maindir)    
        return 
        
    br.select_form(nr=1)
    try:
        control = br.find_control(predicate=lambda x : x.name == 'seriesid' and not x.readonly)
    except ControlNotFoundError:
        try:
            control = br.find_control(predicate=lambda x : x.name == 'rowtypeid' and not x.readonly)
        except ControlNotFoundError:
            try:
                control = br.find_control(predicate=lambda x : x.name == 'indtypeid' and not x.readonly)
            except ControlNotFoundError:
                try:
                    control = br.find_control(predicate=lambda x : x.name == 'yearidall' and not x.readonly)              
                except ControlNotFoundError:
                    control = br.find_control(predicate=lambda x : x.name == 'rowid' and not x.readonly)
                    rowid_handler(br,maindir)
                else:
                    year_handler(br,control,maindir)
            else:
                indtype_handler(br,control,maindir)
        else:
            rowtype_handler(br,control,maindir)
    else:
        series_handler(br,control,maindir)

def series_handler(br,c,maindir):
    Soup = BeautifulSoup(br.response().read())
    
    #add parsing of entity and series definitions
    try:
        e = br.find_control(predicate=lambda x : x.name == 'entitytypeid' and not x.readonly)
    except ControlNotFoundError:
        ev = br['entitytypeid']
        Recs = [(ev,'',str(dict(l.findAll('input')[0].attrs)['value']),Contents(l).strip()) for l in Soup.findAll('label')]
        tb.tabarray(records = Recs,names = ['Entity','EntityName','Series','SeriesName']).saveSV(maindir + 'SeriesEncodings.tsv',metadata=True)
        values = [i.attrs['value'] for i in c.items]
        for v in values:
            br.select_form(nr=1)
            br['seriesid'] = [v]
            br.submit()
            handle_ii_br(br,maindir)
            br.back()
    else:
        Erecs = [(str(dict(l.findAll('input')[0].attrs)['value']),Contents(l).strip()) for l in Soup.findAll('div',id='menuitem1')[0].findAll('label')]
        evalues = [i.attrs['value'] for i in e.items]


        for (ev,evn) in Erecs:
            Recs = [(ev,evn,str(dict(a.attrs)['value']),Contents(a.findNext()).strip(),get_ser_descr(str(dict(a.attrs)['value']),br,ev)) for a in Soup.findAll('div',id='menusubitem' + ev)[0].findAll('input')]
            X = tb.tabarray(records = Recs,names = ['Entity','EntityName','Series','SeriesName','SeriesDescr'])
            tb.io.appendSV(maindir + 'SeriesEncodings.tsv',X,metadata=True)
            for v in X['Series']:
                br.select_form(nr=1)
                br['seriesid'] = [v]
                br['entitytypeid'] = [ev]
                br.submit()
                handle_ii_br(br,maindir)
                br.back()            

def get_ser_descr(v,br,ev):
    urlbase = br.geturl().split('?')[-1]
    def_br = Browser()
    def_br.open('http://www.bea.gov/international/ii_web/beadynamicseriesdefn.cfm?seriesid=' + v + '&EntityTypeID=' + ev + '&' + urlbase)
    Soup = BeautifulSoup(def_br.response().read())
    return Contents(Soup.findAll('body')[0]).strip()


@activate(lambda x  : 'http://www.bea.gov/international/ii_web/beaseriesclassifications.cfm?econtypeid=2&dirlevel1id=1',lambda x : x[0] + 'series_defs.html')
def get_series_defs(maindir):
    wget('http://www.bea.gov/international/ii_web/beaseriesclassifications.cfm?econtypeid=2&dirlevel1id=1',maindir + 'series_defs.html')
    
        
def rowtype_handler(br,c,maindir):
    values = [i.attrs['value'] for i in c.items]
    topurl = br.geturl()
    Soup = BeautifulSoup(br.response().read())
    series = br['seriesid']
    try:
        entitytypeid = br['entitytypeid']
    except AttributeError:
        entitytypeid = ''
    Recs = [(str(dict(l.findAll('input')[0].attrs)['value']),Contents(l).strip(),series,entitytypeid) for l in Soup.findAll('label')]
    X = tb.tabarray(records = Recs,names = ['RowType','RowTypeName','Series','Entity'])
    tb.io.appendSV(maindir + 'RowTypeEncodings.tsv',X,metadata=True)
    for v in values:
        br.select_form(nr=1)
        br['rowtypeid'] = [v]
        br.submit()
        handle_ii_br(br,maindir)
        br.back()
        
def year_handler(br,c,maindir):
    c.items[0].selected = True
    br.submit()
    handle_ii_br(br,maindir)
    br.back()

def indtype_handler(br,c,maindir):
    topurl = br.geturl()
    for v in ['1','2']:
        br.select_form(nr = 1)
        br['indtypeid'] = [v]
        br.find_control('newyearid' + ('' if v == '1' else '2') + 'all').items[0].selected = True
        br.submit()
        handle_ii_br(br,maindir)
        br.back()

def rowid_handler(br,maindir):
    try:
        c = br.find_control(predicate=lambda x : x.name == 'rowid' and not x.readonly)
    except ControlNotFoundError:
        print 'rowid control not found at ', br
    else:
        rvalues = [int(i.attrs['value']) for i in c.items]
        br['rowid'] = [str(min(rvalues))]
    try:
        c = br.find_control(predicate=lambda x : x.name == 'columnid' and not x.readonly)
    except ControlNotFoundError:
        print 'columnid control not found at ', br
    else:
        cvalues = [int(i.attrs['value']) for i in c.items]
        br['columnid'] = [str(min(cvalues))]
        
    br.submit()
    handle_ii_br(br,maindir)
    br.back()


def entity_descr(entity):

    if entity == 'U.S. Parent Companies':
        return 'A "US Parent Company" is the investor, resident in the United States, that owns or controls 10 percent or more of the voting securities of an incorporated foreign business enterprise or an equivalent interest in an unincorporated foreign business enterprise.'
    elif entity == 'All Foreign Affiliates':
        return 'A "Foreign Affiliate" is a foreign business enterprise in which there is U.S. direct investment, that is, in which a U.S. person owns or controls 10 percent of the voting securities or the equivalent.'
    elif entity == 'Majority-Owned Foreign Affiliates':
        return 'A "Majority-Owned Foreign Affiliate" is a foreign affiliate in which the combined direct and indirect ownership interest of all U.S. parents exceeds 50 percent. '
    elif entity == 'All U.S. Affiliates':
        return 'A U.S. affiliate is a U.S. business enterprise in which a single foreign investor owns at least 10 percent of the voting securities or the equivalent.'
    elif entity == 'Majority-Owned U.S. Affiliates':
        return 'A majority-owned U.S. affiliate is a U.S. affiliate that is owned more than 50 percent by a foreign direct investor.'
  
import csv
def parse_ii(infile,rowtypename):

    rtn = rowtypename.lower()
    
    if 'industry' in rtn and 'country' in rtn:
        aggtype = 'none'
    elif 'industry' in rtn:
        aggtype = 'industry'
    elif 'country' in rtn and 'state' not in rtn:
        aggtype = 'country'
    elif 'country' in rtn and 'state' in rtn:
        aggtype = 'state&country'
    elif 'state' in rtn:
        aggtype = 'state'
    else:
        aggtype = 'both'
    
    print 'Aggtype = ', aggtype
        
    #parse space and industry data properly into dictionaries as a function of rowtype 
    
    F = open(infile,'rU').read().strip().split('\n')
    headerlines = []
    while True:
        line = F.pop(0)
        if line:
            headerlines.append(line)
        else:
            break
    unit =  headerlines[-1].split('(')[-1].strip(') ')
    
    while True:
        line = F.pop(0)
        if line:
            break
            
    names = line.split(',')
    cross_vals = []
    for i in range(1,len(names)):
        cross_vals.append(names[i].split(' - ')[0].strip())
        names[i] = names[i].split(' - ')[-1].strip()
    if aggtype in ['both','industry','none']:
        names[0] = 'Industry'
    elif aggtype in ['country','state']:
        names[0] = 'Location'
    elif aggtype == 'state&country':
        names[0] = 'USState'

    data = []
    while True:
        line = F.pop(0)
        if line:
            rec = list(csv.reader([line],delimiter = ','))[0]
            recs = process_ii_rec(rec,aggtype,names,cross_vals)
            data += recs
        else:
            break

    if aggtype == 'industry':
        process_ii_industry(data)
        for r in data:
            r['Location'] = pm.son.SON([('c', r['Location'])])

    elif aggtype == 'country':
        process_ii_location(data)
        for r in data:
                r['Industry'] = pm.son.SON([('0', r['Industry'])])

    elif aggtype == 'state':
        process_ii_state(data,'Location')
        for r in data:
                r['Industry'] = pm.son.SON([('0', r['Industry'])])
    
    elif aggtype == 'both':
        for r in data:
            r['Location'] = pm.son.SON([('c', r['Location'])])
            r['Industry'] = pm.son.SON([('0', r['Industry'])])
    
    elif aggtype == 'none':
        process_ii_industry(data)
        process_ii_location(data)   

    elif aggtype == 'state&country':
        process_ii_state(data,'USState')
        process_ii_location(data)
        
    footerlines = F
   
    timecolnames = [x for x in uniqify(ListUnion([r.keys() for r in data])) if x not in ['Industry','Location','USState']]
    
  
    return [data,headerlines,footerlines,unit,timecolnames]

def process_ii_state(data,name):
    catcol = np.array([d[name] for d in data])
    [catcols,hl] = gethierarchy(catcol,hr,postprocessor = lambda x : x.strip(' \t:'))
    catnames = ['D','s']
    for (i,r) in enumerate(data):
        H = zip(catnames,[c[i] for c in catcols])
        H = [(k,v) for (k,v) in H if v]
        r[name] = pm.son.SON(H)
        
def process_ii_location(data):
    catcol = np.array([d['Location'] for d in data])
    [catcols,hl] = gethierarchy(catcol,hr,postprocessor = lambda x : x.strip())
    for (i,r) in enumerate(data):
        rec = [c[i] for c in catcols]

        if 'other' in rec[0].lower() and len(rec) > 2:
            if rec[2] == 'Other' and len(rec) > 3:
                H = [('C',rec[1]),('c',rec[3]),('other',True),('R',rec[0])]
            else:
                H = [('C',rec[1]),('c',rec[2]),('R',rec[0])]    
        else:
            if len(rec) > 1:
                if rec[1] == 'Other' and len(rec) > 2 and rec[2]:
                    H = [('C',rec[0]),('c',rec[2]),('other',True)]
                else:
                    H = [('C',rec[0]),('c',rec[1])]
            else:
                H = [('c',rec[0])]
    
        H = [(k,v) for (k,v) in H if v]
        r['Location'] = pm.son.SON(H)
        if r['Location'].get('C',None) in ['Canada','United States']:
            r['Location']['c'] = r['Location']['C']
            r['Location']['C'] = 'North America'

def process_ii_industry(data):
    catcol = np.array([d['Industry'] for d in data])
    [catcols,hl] = gethierarchy(catcol,hr,postprocessor = lambda x : x.strip())
    catnames = [str(i) for i in range(len(catcols))]
    for (i,r) in enumerate(data):
        H = zip(catnames,[c[i] for c in catcols])
        H = [(k,v) for (k,v) in H if v]
        r['Industry'] = pm.son.SON(H)

def process_ii_rec(rec,rtn,names,cross_vals):
    if rtn == 'both':
        rec = pm.son.SON(zip(names,rec))
        rec['Location'] = 'All countries total'
        return [rec]
    if rtn == 'industry':
        rec = pm.son.SON(zip(names,rec))
        rec['Location'] = 'All countries total'
        return [rec]
    elif rtn in ['country','state']:
        rec = pm.son.SON(zip(names,rec))
        rec['Industry'] = 'All industries total'
        return [rec]
    elif rtn == 'none':
        ival = rec[0]
        recs = []
        r = pm.son.SON([])
        cv_old = cross_vals[0]
        loc = ' '*2*len(cv_old.split('Of Which')) + cv_old.split(':')[-1].strip()
        for (n,v,cv) in zip(names[1:],rec[1:],cross_vals):
            if cv_old != cv:
                r['Industry'] = ival
                r['Location'] = loc
                recs.append(r)
                r = pm.son.SON([])
                cv_old = cv
                loc = ' '*2*len(cv.split('Of Which')) + cv.split(':')[-1].strip()
            
            r[n] = v
            
        r['Industry'] = ival
        r['Location'] = loc
        recs.append(r)

        return recs
    elif rtn == 'state&country':
        ival = rec[0]
        recs = []
        r = pm.son.SON([])
        cv_old = cross_vals[0]
        loc = ' '*2*len(cv_old.split('Of Which')) + cv_old.split(':')[-1].strip()
        for (n,v,cv) in zip(names[1:],rec[1:],cross_vals):
            if cv_old == cv:
                r[n] = v
                r['Location'] = loc     
            else:
                r['USState'] = ival
                recs.append(r)
                r = pm.son.SON([])
                
                cv_old = cv
                loc = ' '*2*len(cv.split('Of Which')) + cv.split(':')[-1].strip()
        r['USState'] = ival 
        recs.append(r)
        return recs

                    
def division_descrs_ii(tag):
    if tag == 'us_investment':
        return {'title': 'U.S. Direct Investment Abroad: Balance of payments and direct investment position data', 'description':'The balance of payments (international transactions) data cover the foreign affiliates\' transactions with their U.S. parent, so these data focus on the U.S. parent\'s share, or interest, in its affiliates rather than on the affiliates\' overall size or level of operations. These data are essential to the compilation of the U.S. international transactions accounts, the international investment position, and the national income and product accounts. The major data items include capital flows, which measure the funds that U.S. parents provide to their foreign affiliates, and income, which measures the return on those funds. The data also cover royalties and license fees and other service charges that parents receive from or pay to their affiliates. All of these items are flow data and provide measurement for a particular time frame, such as for a quarter or a year. \n\n Direct investment position data are stock data and are cumulative; they measure the total outstanding level of U.S. direct investment abroad at yearend. Estimates are provided both at historical cost and in terms of current-period prices. Two alternative official measures of the position are presented in current-period prices -- one with direct investment recorded at current cost, and the other with direct investment recorded at market value. For the historical-cost estimates, tables are published by country and by industry.', 'URL':'http://www.bea.gov/international/ii_web/timeseries2.cfm?econtypeid=1&dirlevel1id=1&Entitytypeid=1&stepnum=1'}
    elif tag == 'us_finance':
        return {'title': 'U.S. Direct Investment Abroad: Financial and operating data', 'description':'The financial and operating data provide a picture of the overall activities of foreign affiliates and U.S. parent companies using a wide variety of indicators of their financial structure and operations. The data on foreign affiliates cover the entire operations of the affiliate, irrespective of the percentage of U.S. ownership. These data cover items that are needed in analyzing the characteristics, performance, and economic impact of multinational companies, such as sales, gross product (value added), employment and compensation of employees, capital expenditures, exports and imports, and research and development expenditures. Separate tabulations are available for affiliates that are majority owned by their U.S. parent because the concept of majority control is often important in the analysis of multinational companies.', 'URL':'http://www.bea.gov/international/ii_web/timeseries1.cfm?econtypeid=1&dirlevel1id=2'}
    elif tag == 'foreign_investment':
        return {'title':'Foreign Direct Investment in the United States: Balance of payments and direct investment position data', 'description':' The balance of payments (international transactions) data cover the U.S. affiliates\' transactions with their foreign parents, so these data focus on the foreign parents\' share, or interest, in their U.S. affiliates rather than on the affiliates\' overall size or level of operations. These data are essential to the compilation of the U.S. international transactions accounts, the international investment position, and the national income and product accounts. The major data items include capital flows, which measure the funds that foreign parents provide to their U.S. affiliates, and income, which measures the return on those funds. The data also cover royalties and license fees and other service charges that affiliates receive from or pay to their parents. All of these items are flow data and provide measurement for a particular time frame, such as for a quarter or a year. \n\n Direct investment position data are stock data and are cumulative; they measure the total outstanding level of foreign direct investment in the United States at yearend. Estimates are provided both at historical cost and in terms of current-period prices. Two alternative official measures of the position are presented in current-period prices -- one with direct investment recorded at current cost, and the other with direct investment recorded at market value. For the historical-cost estimates, tables are published by country and by industry.','URL':'http://www.bea.gov/international/ii_web/timeseries2.cfm?econtypeid=2&dirlevel1id=1&Entitytypeid=1&stepnum=1'}
        
    elif tag == 'foreign_finance':
        return {'title':'Foreign Direct Investment in the United States: Financial and operating data', 'description':'The financial and operating data provide a picture of the overall activities of U.S. affiliates and contain a wide variety of indicators of their financial structure and operations. The data cover the entire operations of the U.S. affiliate, irrespective of the percentage of foreign ownership. These data cover items that are needed in analyzing the characteristics, performance, and economic impact of multinational companies, such as sales, gross product (value added), employment and compensation of employees, capital expenditures, exports and imports, and research and development expenditures. Tables are published by country, by industry, and (for selected items) by State. More detailed tables by industry and State on affiliate operations at the establishment level are available for selected years as a result of a special project that linked the Bureau\'s enterprise data for U.S. affiliates with the establishment data for all U.S. companies from the Bureau of the Census.','URL':'http://www.bea.gov/international/ii_web/timeseries1.cfm?econtypeid=2&dirlevel1id=2'}


class ii_parser(govdata.core.DataIterator):

    def __init__(self,source):
        self.metadata = M = {'':{}}
        D = M['']
        tags = ['us_investment','us_finance','foreign_investment','foreign_finance']
        self.manifests = dict([(tag, tb.tabarray(SVfile = source + tag + '/Manifest.tsv')) for tag in tags])
        self.tag_names = dict(zip(tags,['US Investment Abroad','US Financials Abroad','Foreign Direct Investment in US','Foreign Financials in US']))
        
        iCols = ['Division','Entity','Series','Aggregation','Location','USState','Industry']
                       
        D['dateFormat'] = 'YYYY'
        D['columnGroups'] = {'timeColNames':[],'labelColumns':iCols,'spaceColumns':['USState','Location']}
               
        D['sliceCols'] = [iCols]
        D['uniqueIndexes'] = ['Division','Entity','Series','Aggregation','Location','Line']
        
        D['description'] = 'Comprehensive data on inward and outward direct investment, including data on direct investment positions and transactions and on the financial and operating characteristics of the multinational companies involved.'
        D['keywords'] = 'multinational,foreign,direct investment'.split(',')
        
        D['note'] = 'The financial and operating data available from these interactive tables cover only nonbank parents and affiliates. Nonbank parents (affiliates) exclude parents (affiliates) engaged in deposit banking and closely related functions, including commercial banks, savings institutions, credit unions, and bank and financial holding companies.'

        D['url'] = 'http://www.bea.gov/international/'

        D['valueProcessors'] = {'Industry':'return require("underscore")._.values(value).join(", ");'}

        #add subgroups for division, entity, series, industryType from little saved metadata
        
        for t in tags:
            M[tag] = division_descrs_ii(tag) 
        
        entities = [en for en in ListUnion([uniqify(t['EntityName']) for t in self.manifests.values()]) if en]
        for entity in entities:
            M[entity] = {'description':entity_descr(entity)}
            
        seriesdict = dict(ListUnion([t[['SeriesName','SeriesDescr']].aggregate(On=['SeriesName'],AggFunc=lambda x : x[0]).tolist() for t in self.manifests.values() if 'SeriesDescr' in t.dtype.names]))
        Soup = BeautifulSoup(open(source + 'series_defs.html').read())
        SD0 = [BeautifulSoup(x) for x in  str(Soup.findAll('body')[0]).split('<p></p>') if '<strong>' in x]
        SD = [(' ; '.join(map(lambda x : Contents(x).strip(' .'),s.findAll('strong'))) , Contents(s).strip()) for s in SD0 if Contents(s)]
        for k in seriesdict.keys():
            if seriesdict[k]:
                md = seriesdict[k]
            else:
                md = ''
                k = set(k.lower().split(' '))
                for (b,p) in SD:
                    b = b.lower().split(' ')    
                    if any(k.intersection(b)):
                        md += '\n' + p
            M[k] = {'description':md}
            
        M['NAICS'] = {'description':'NAICS is the industry classification system of the United States, Canada, and Mexico. For U.S. direct investment abroad, industry classifications based on NAICS are used for estimates for 1999 forward. Industry classifications for estimates for earlier years are based on the SIC (Standard Industrial Classification) system. The United States adopted NAICS because it better reflects new and emerging industries, industries involved in production of advanced technologies, and the diversification of services industries.'}
        M['SIC'] = {'description':'For U.S. direct investment abroad, industry classifications based on the SIC are used for estimates for years prior to 1999. Industry classifications for estimates for 1999 forward are based on NAICS (North American Industry Classification System), which is the industry classification system currently used by the United States, Canada, and Mexico. The United States adopted NAICS because it better reflects new and emerging industries, industries involved in production of advanced technologies, and the diversification of services industries.'}
    

        
    def refresh(self,path):
        print '\n\nRefreshing', path
        tag,file = path.split('/')[-2:]
        pathtag = file.split('.')[0]
        [entity,series,rowtype,indtype] = pathtag.split('_')
        
        Mani = self.manifests[tag]
        row = Mani[(Mani['Series'] == series) & (Mani['Entity'] == entity) & (Mani['RowType'] == rowtype) & (Mani['IndType'] == indtype)][0]
        rowtypename, entityname,seriesname = [row[k] for k in ['RowTypeName','EntityName', 'SeriesName']]
        if indtype == '1':
            indtypename = 'NAICS'
        elif indtype == '2':
            indtypename = 'SIC'
        else:
            indtypename = ''

        [data,headerlines,footerlines,unit,timecolnames] = parse_ii(path,rowtypename)
        
        self.metadata[pathtag] = {'header':headerlines,'footer':footerlines,'unit':unit}
        self.metadata['']['ColumnGroups']['TimeColNames'] = uniqify(self.metadata['']['ColumnGroups']['timeColNames'] + timecolnames)
            
        for (i,r) in enumerate(data):
            r['subcollections'] = [tag, seriesname,entityname,indtypename,pathtag]
            r['Division'] = tag
            r['Entity'] = entityname
            r['Series'] = seriesname
            r['Aggregation'] = rowtypename
            r['IndustryClassification'] = indtypename
            r['Line'] = i
            
        self.data = data
                        
        self.IND = 0    


    def next(self):
        if self.IND < len(self.data):
            
            r = self.data[self.IND]    
            self.IND += 1
            return r
        else:
            raise StopIteration
                             
  
II_NAME = 'BEA_InternationalInvestment'
def construct_international_investment_parser():

    base = 'http://www.bea.gov/international/ii_web/timeseries'
    
    URLS = [base + x for x in ['2.cfm?econtypeid=1&dirlevel1id=1&Entitytypeid=1&stepnum=1',
                               '1.cfm?econtypeid=1&dirlevel1id=2',
                               '2.cfm?econtypeid=2&dirlevel1id=1&Entitytypeid=1&stepnum=1',
                               '1.cfm?econtypeid=2&dirlevel1id=2']
           ]
    
    T = ['us_investment','us_finance','foreign_investment','foreign_finance']
    
    D = [(get_series_defs,'get_series_defs',())] + ListUnion([[(get_ii_urls,'urls_' +t,(u,t)),(make_ii_manifest,'manifest_' + t,(t,)),(get_ii_data,'data_' + t,(t,))] for (u,t) in zip(URLS,T)] )
    
    [fs, ts, args] = zip(*D)
    downloader = zip(fs,ts)
    
    return govdata.core.GovParser(II_NAME,ii_parser,downloader = downloader,downloadArgs = args)
    
II_PARSER = construct_international_investment_parser()


    

    
