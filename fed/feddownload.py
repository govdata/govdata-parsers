import os
import shutil
import time
from BeautifulSoup import BeautifulStoneSoup

ddl_choose_url_prefix = "http://www.federalreserve.gov/datadownload/Output.aspx?filetype=zip&rel="

# copied from utils.basic (it doesn't seemed to be checked into git yet)
# todo: i don't feel wget'ing to localdisk is safe; find an in memory alternative.
def wget(getpath,savepath,opstring=''):
        os.system('wget ' + opstring + ' "' + getpath + '" -O "' + savepath + '"')

def GrabSDMX(dataid):
        # defining where things go
        url = ddl_choose_url_prefix + dataid
        tempZipPrefix = '/tmp/frb_ddl_' + dataid
        tempZip = tempZipPrefix + '.zip'
        tempUnzipDir = tempZipPrefix
        tempXmlDataFile = tempUnzipDir + '/' + dataid + '_data.xml'

        # cleaning up previous runs if they exist
        if os.path.exists(tempUnzipDir):
                shutil.rmtree(tempUnzipDir,True)
        if os.path.exists(tempZip):
                os.remove(tempZip)

        # get
        getOpString = '--user-agent="Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7"'
        wget(url, tempZip, getOpString)
        os.system('unzip ' + tempZip + ' -d ' + tempUnzipDir)
        os.listdir(tempUnzipDir)

        # open the xml; we can etree this if this is a performance problem later
        startFileOpenTime = time.time()
        xml = BeautifulStoneSoup(open(tempXmlDataFile)
        endFileOpenTime = time.time()

GrabSDMX("G19")
GrabSDMX("H41")
GrabSDMX("G17")
GrabSDMX("H3")
GrabSDMX("H8")
GrabSDMX("CHGDEL")
GrabSDMX("CP")
GrabSDMX("G20")
GrabSDMX("H10") # G5/H10?!
