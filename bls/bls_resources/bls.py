import os
import time
import re
import cPickle as pickle
import hashlib

import tabular as tb
import numpy as np

from BeautifulSoup import BeautifulSoup,BeautifulStoneSoup
from starflow.metadata import AttachMetaData, loadmetadata
from starflow.protocols import actualize
from starflow.utils import activate, MakeDir,Contents,listdir,PathExists, strongcopy,uniqify,ListUnion,Rename, delete, MakeDirs, is_string_like

import govdata.core    
import pymongo as pm

from utils.basic import wget

resource_root = '../parsers/bls_resources/'

def WgetMultiple(link, fname, maxtries=10):
    link = link if is_string_like(link) else link['URL']
    opstring = '--user-agent="Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7"'
    time.sleep(5)
    for i in range(maxtries):
        wget(link, fname, opstring)
        F = open(fname,'r').read().strip()
        if F.startswith('<!DOCTYPE HTML'):
            return
        else:
            print 'download of ' + link + ' failed: ' + F[:20]
            time.sleep(15)
            
    print 'download of ' + link + ' failed after ' + str(maxtries) + ' attempts'
    return


MAIN_SPLITS = ['cu', 'cw', 'su', 'ap', 'li', 'pc', 'wp', 'ei', 'ce', 'sm', 'jt', 'bd', 'oe', 'lu', 'la', 'ml', 'nw', 'ci', 'cm', 'eb', 'ws', 'le', 'cx', 'pr', 'mp', 'ip', 'in', 'fi', 'ch', 'ii']

@activate(lambda x : "ftp://ftp.bls.gov/pub/time.series/" + x[1], lambda x : x[0])
def bls_downloader(download_dir,code):

    MakeDirs(download_dir)
    download_dir += ('/' if download_dir[-1] != '/' else '')

    MakeDir(download_dir + 'RawDownloads/')
    
    get = "ftp://ftp.bls.gov/pub/time.series/" + code + '/'
    
    WgetMultiple(get,download_dir + 'RawDownloads/index.html')
    Soup = BeautifulSoup(open(download_dir + 'RawDownloads/index.html'))
    A = Soup.findAll('a')
    Records = [(Contents(a),str(dict(a.attrs)['href'])) for a in A]
    Records = [r for r in Records if 'Current' not in r[0].split('.')]
    RecordsR = [r for r in Records if 'AllData' in r[0]]
    if RecordsR:
        Records = RecordsR + [r for r in Records if not '.data.' in r[0]]
    T = tb.tabarray(records = Records,names = ['File','URL'])
    for (f,u) in T:
        wget(u,download_dir + 'RawDownloads/' + f + '.txt')

    makemetadata(code,download_dir + 'RawDownloads/',download_dir + 'metadata.pickle',download_dir + 'filenames.tsv')
    
    MakeDir(download_dir + '__FILES__')
    
    processtextfile(download_dir + 'RawDownloads/',download_dir + '/__FILES__/documentation.txt')
    
    MakeDir(download_dir + '__PARSE__')
    for l in listdir(download_dir  + 'RawDownloads/'):
        if '.data.' in l:
            Rename(download_dir + 'RawDownloads/' + l, download_dir + '__PARSE__/' + l)

    SPs = [download_dir + 'RawDownloads/' + l for l in listdir(download_dir + 'RawDownloads/') if l.endswith('.series.txt')]
    assert len(SPs) == 1, 'Wrong number of series paths.'
    serpath = SPs[0]    
    parse_series(download_dir + 'RawDownloads/',download_dir + 'series.txt')
    delete(serpath)


def CleanOpen(path):

    try:
        [recs,md] = tb.io.loadSVrecs(path,delimiter = '\t',formats='str',headerlines=1,linefixer = lambda x : x.replace('\x00',''))
    except:
        print 'Problem reading records from', path
    else:
        ln = len(md['names'])
        recs = [r[:ln] for r in recs if r != []]
        
        if len(recs) > 0:
            X = tb.tabarray(recs,names = md['names'])
            return X
        
def main_splitfunc(x):
    return x['URL'].strip(' /').split('/')[-2]


    
def identifybasepath(base,datadir):
    L = listdir(datadir)
    L1 = [x.split('.')[-2] for x in L]
    L2 = [x.split('.')[-2].replace('_','') for x in L]
    if base in L1:
        return datadir + L[L1.index(base)]
    elif base in L2:
        return datadir + L[L2.index(base)]
    elif base.replace('_','') in L1:
        return datadir + L[L1.index(base.replace('_',''))]
    elif base.replace('_','') in L2:
        return datadir + L[L2.index(base.replace('_',''))]

        
def identifycode(name,names):
    
    tries = [name + '_code', name + '_codes',name.replace('_','') + '_code',name.replace('_','') + '_codes',name,name.replace('_','')]

    for t in tries:
        if t in names:
            return t
    
def identifybase(name):
    if name.endswith('_code'):
        return name[:-5]
    elif name.endswith('_codes'):
        return name[:-6]
    else:   
        return name


@activate(lambda x : x[0], lambda x :x[1])      
def parse_series(datadir,outpath,units=''):
    SPs = [datadir + l for l in listdir(datadir) if l.endswith('.series.txt')]
    assert len(SPs) == 1, 'Wrong number of series paths.'
    serpath = SPs[0]
    F = open(serpath,'rU')
    names = F.readline().rstrip('\n').split('\t')

    codefiles = {}
    codenames = {}
    bases = {}
    for name in names:
        base = identifybase(name)
        basepath = identifybasepath(base,datadir)
        if basepath != None:
            print name, basepath
            codefile = CleanOpen(basepath)
            if codefile != None:
                codename = identifycode(base,codefile.dtype.names)
                if codename != None:        
                    codenames[name] = codename
                    codefiles[name] = codefile[[n for n in codefile.dtype.names if n.startswith(base)]]
                    bases[name] = base
                else:
                    print '\n\nWARNING: Problem with code for' , name , 'in file', basepath, '\n\n'
            else:
                print '\n\nWARNING: Can\'t seem to open', basepath
        else:
            print '\n\nWARNING: Problem with finding basepath for ', name , 'in', datadir                   
    
    blocksize = 750000


    done = False

    while not done:
        lines = [F.readline().rstrip('\n').split('\t') for i in range(blocksize)]
        lines = [l for l in lines if l != ['']]
        if len(lines) > 0:
            X = tb.tabarray(records = lines,names = names)
            NewCols = []
            NewNames = []
            for name in names:
                if name in codenames.keys():
                    codefile = codefiles[name]
                    base = bases[name]
                    codename = codenames[name]
                    Xn = np.array([xx.strip() for xx in X[name]])
                    Cn = np.array([xx.strip() for xx in codefile[codename]])
                    [S1,S2] = tb.fast.equalspairs(Xn,Cn)
        
                    NewCols += [codefile[n][S1] for n in codefile.dtype.names if n != codename]
                    NewNames += [n for n in codefile.dtype.names if n != codename]
            X = X.addcols(NewCols,  names = NewNames)
            X.coloring['NewNames'] = NewNames
            
            if units != '':
                if ' ' not in units:
                    if units:
                        X.coloring['Units'] = [units]
                elif not units.startswith('if '):
                    X = X.addcols([[units]*len(X)], names=['Units'])
                else:
                    X = X.addcols([[rec['earn_text'] if rec['tdata_text'] == 'Person counts (number in thousands)' else rec['pcts_text'] for rec in X]], names=['Units'])

            tb.io.appendSV(outpath,X,metadata=True)
        else:
            done = True
    

@activate(lambda x : tuple([x[1],x[2],x[3]]), lambda x : (x[4],x[5]))
def makemetadata(code,datadir,outfile1,outfile2,depends_on = (resource_root + 'ProcessedManifest_2_HandAdditions.tsv',resource_root + 'Keywords.txt')):

    Z  = {}

    keyword_file = depends_on[1]
    Y = tb.tabarray(SVfile = keyword_file)[['Code','Keywords']]
    y = Y[Y['Code'] == code]
    Z['keywords'] = [x.strip() for x in str(y['Keywords'][0]).split(',')]
    
    
    dirl = np.array(listdir(datadir))
    
    pr = lambda x : x.split('!')[-1][:-4]
    p=re.compile('\([^\)]*\)')
    
    tps = [l for l in dirl if l.endswith('.txt.txt')]
    if tps:
        textpath = datadir + tps[0]
        [SD,things] = ParseTexts(textpath,code)
        FNs = [p.sub('',things[pr(y).lower()]).replace(' ,',',').replace(',,',',') if pr(y).lower() in things.keys() else '' for y in dirl]
        FNs = [z.split('=')[1] if '=' in z and not ' =' in z else z for z in FNs]
    else:
        SD = ''
        FNs = len(dirl)*['']
        
    Z['description'] = SD
    
    cfs = [l for l in dirl if l.endswith('.contacts.txt')]
    if cfs:
        contactfile = datadir + cfs[0]
        ctext = open(contactfile,'rU').read().strip()
        if '<html>' in ctext.lower():
            clines = ctext.split('\n')
            fb = [i for i in range(len(clines)) if clines[i].strip() == ''][0]
            ctext = '\n'.join(clines[fb+1:])
        ctext = ctext.strip(' *\n').replace('\n\n','\n')    
    else:
        ctext = ''
        
    Z['contactInfo'] = ctext
    f = open(outfile1,'w')
    pickle.dump(Z,f)
    f.close()

    Y = tb.tabarray(SVfile = depends_on[0])
    Y.sort(order = ['File'])

    
    dirlp = np.array([pr(y) for y in dirl])
    [A,B] = tb.fast.equalspairs(dirlp,Y['File'])
    if (B>A).any():
        print 'adding hand-made content to', dirlp[B>A]
        for k in (B>A).nonzero()[0]:
            FNs[k] = Y['FileName'][A[k]]    
    
    D = tb.tabarray(columns=[dirl,FNs], names = ['Path','FileName'])
    
    D.saveSV(outfile2,metadata = True)  
    

def ParseTexts(textpath,code):

    F = open(textpath,'rU').read().strip(' \n*').split('\n')
    bs = [i for i in range(len(F)) if 'Survey Description:' in F[i] or 'Program Description:' in F[i]]
    if bs:
        b = bs[0]
        if F[b].split(':')[1].strip() == '':
            bb = b + 2
        else:
            bb = b

        e = [i for i in range(bb,len(F)) if F[i].strip() == ''][0]
        SD  = ' '.join(F[b:e+1])
    else:
        SD = ''
        print 'No description in ', textpath.split('/')[-1]

    pb = [i for i in range(len(F)) if 'Section 2' in F[i]][0] 
    b = [i for i in range(pb,len(F)) if F[i].strip().startswith(code)][0]
    e = [i for i in range(b,len(F)) if F[i].strip() == '' or F[i].startswith('==')][0]
    things = {}
    p = re.compile('- ')
    for x in F[b:e]:
        if x.strip().startswith(code):
            v = p.split(x.strip())[0].strip().replace(' ','').lower()
            k = '-'.join(x.strip().split('-')[1:]).strip().replace('\t',' ')
            things[v] = k
        else:
            things[v] += ' ' + x.strip().replace('\t',' ')

            
    return [SD,things]
    

@activate(lambda x : x[0], lambda x : x[1]) 
def processtextfile(datadir,outfile):
    dirl = listdir(datadir)
    tps = [l for l in dirl if l.endswith('.txt.txt')]
    if tps:
        textpath = datadir + tps[0]
        strongcopy(textpath,outfile)

    else:
        F = open(outfile,'w')
        F.write('No documentation file found in.')
        F.close()
            
            
def tval(year,per):
    if per.startswith('M'):
        num = int(per[1:])
        assert num <= 13
        if num < 13:
            return year + 'X' + 'X' + per[1:]
        else:
            return year + 'X' + 'X' + 'XX'
    elif per.startswith('Q'):
        num = int(per[1:])
        assert len(str(num)) == 1 and num <= 5
        if num < 5:
            return year + 'X' + str(num) + 'XX'
        else:
            return year + 'X' + 'X' + 'XX' 
    elif per.startswith('S'):
        num = int(per[1:])
        assert len(str(num)) == 1 and num <= 3
        if num < 3:
            return year + str(num) + 'X' + 'XX'
        else:
            return year + 'X' + 'X' + 'XX' 
    elif per.startswith('A'):
        return  year + 'X' + 'X' + 'XX' 
    else:
        raise ValueError, 'Time period format of ' + per + ' not recognized.'

def nameProcessor(g):

    g = g.split('_name')[0]
    g = g.split('_text')[0]
    g = g[0].upper() + g[1:]
    return g



def inferSpaceCode(name):
    parts = uniqify(name.lower().split('_') + name.lower().split(' '))
    if 'msa' in parts and not 'code' in parts:
        return 'm'
    elif 'state' in parts and not 'code' in parts:
        return 's'
    elif 'county' in parts and not 'code' in parts:
        return 'c'
    elif 'fips' in parts and 'text' in parts:
        return 'X'
    elif 'area' in parts and 'code' not in parts:
        return 'X'
    elif 'fips' in parts and 'state' not in parts and 'county' not in parts:
        return 'f.X'
    elif 'state' in parts and ('code' in parts or 'fips' in parts):
        return 'f.s'
    elif 'county' in parts and ('code' in parts or 'fips' in parts):
        return 'f.c'
    elif 'msa' in parts and ('code' in parts or 'fips' in parts):
        return 'f.m'
    
class bls_parser(govdata.core.DataIterator):

    def __init__(self,source,sliceCols = None):
    
        self.metafile = metafile = source + 'metadata.pickle'
        self.docfile = docfile = source + 'documentation.txt'
        self.seriesfile = seriesfile = source + 'series.txt'
        self.filelistfile = filelistfile = source + 'filenames.tsv'
  
        
        self.metadata = {}
        self.metadata[''] = D = {}
        
        M = pickle.load(open(metafile))
        for x in ['contactInfo','description','keywords']:
            D[x] = M[x]
        D['dateFormat'] = 'YYYYhqmm'

        M = tb.io.getmetadata(seriesfile)[0]
        self.headerlines = M['headerlines']
        getnames = M['coloring']['NewNames']
        names = M['names']
        spaceCodes = [inferSpaceCode(n) for n in names]
        self.getcols = [names.index(x) for (x,y) in zip(names,spaceCodes) if y == None and x in getnames]
        self.spacecols = [(names.index(x),y) for (x,y) in zip(names,spaceCodes) if y != None]
        self.fipscols = [(j,y) for (j,y) in self.spacecols  if y.startswith('f.')]
        self.nonfipscols = [(j,y) for (j,y) in self.spacecols  if not y.startswith('f.')]
        goodNames = [nameProcessor(x) for x in names]
        self.NAMES = ['subcollections', 'Series'] + [goodNames[i] for i in self.getcols] + (['Location'] if self.spacecols else [])
        
        labelcols = [goodNames[i] for i in self.getcols] + (['Location'] if self.spacecols else [])

        self.TIMECOLS = []
        D['columnGroups'] = {'timeColNames': self.TIMECOLS, 'labelColumns': labelcols }
        if self.spacecols:
            D['columnGroups']['spaceColumns'] = ['Location']
        D['uniqueIndexes'] = ['Series']
        if sliceCols:
            D['sliceCols'] = sliceCols
        else:
            D['sliceCols'] = [[g for g in labelcols if g.lower().split('.')[0] not in ['footnote','seasonal','periodicity','location']] + (['Location'] if self.spacecols else [])]

        print 'Added general metadata.'
        
        
    def refresh(self,file):
  
        x = file
        ColNo = x.split('!')[-1].split('.')[x.split('!')[-1].split('.').index('data') + 1]
  
        self.ColNo = ColNo
        FLF = tb.tabarray(SVfile = self.filelistfile)
        Paths = FLF['Path'].tolist()
        self.metadata[ColNo] = {'title':FLF['FileName'][Paths.index(file.split('/')[-1])]}

        print 'Initializing for ', self.metadata[ColNo]['title']                
    
        self.G = open(self.seriesfile,'rU')
        for i in range(self.headerlines):
            self.G.readline()
        self.sline = self.G.readline().strip('\n')
            
        self.F = open(file,'rU')
        self.dnames = self.F.readline().strip().split('\t')
        self.dline = self.F.readline().strip('\n') 
    

    def next(self):

        if self.dline:
            dlinesplit = [x.strip() for x in self.dline.split('\t')]
            ser = dlinesplit[0]
    
            found = False
            while not found:
                if self.sline.split('\t')[0].strip() == ser:
                    found = True
                else:
                    self.sline = self.G.readline().strip('\n')
            slinesplit = self.sline.split('\t')
                        
            Vals = [[self.ColNo],ser] + [slinesplit[j].strip() for j in self.getcols] + ([dict(([(y,slinesplit[j])  for (j,y) in self.nonfipscols] if self.nonfipscols else []) + ([('f',dict([(y.split('.')[1],slinesplit[j])  for (j,y) in self.fipscols]))]  if self.fipscols else []))] if self.spacecols else [])
            
            servals = pm.son.SON(zip(self.NAMES,Vals))
            
            while dlinesplit[0] == ser and self.dline:

                if dlinesplit[3]:
                    t = tval(dlinesplit[1],dlinesplit[2])
                    if not t in self.TIMECOLS:
                        self.TIMECOLS.append(t)
                
                    servals[t] = float(dlinesplit[3] )
                    
                self.dline = self.F.readline().strip('\n')
                dlinesplit = [x.strip() for x in self.dline.split('\t')]

            return servals
        else:
            raise StopIteration
                    
                        

#actual creators =-=-=-=-=-=-=-=-=
    
PARSER_NAMES = ['ap','bd','cw','li','pc','wp','ce','sm','jt','la']

PARSER_DICT = dict([(name,govdata.core.GovParser('BLS_' + name,bls_parser,downloader = bls_downloader,downloadArgs = (name,))) for name in PARSER_NAMES])

LU_PARSER = govdata.core.GovParser('BLS_lu',bls_parser,downloader = bls_downloader,downloadArgs = ('lu',),parserKwargs = {'sliceCols':['Indy','Occupation','Education', 'Ages', 'Race', 'Orig', 'Sexs', 'Location.X']}) 
    
 