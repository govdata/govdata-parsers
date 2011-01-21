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




@activate(lambda x : 'http://www.google.com/googlebooks/uspto-patents-grants-biblio.html',lambda x : x[0])
def USPTO_downloader(maindir):
	MakeDirs(maindir)
	MakeDir(maindir + 'raw/')
	# URL format between 2001 - 2004
	URL_google_patents = 'http://www.google.com/googlebooks/uspto-patents-grants-biblio.html'
	google_patents = urllib.urlopen(URL_google_patents)
	zip_files = re.findall(r'<a href="([:/\._\-a-z0-9]*.zip)"',google_patents.read())
	MakeDir(maindir + "tmp")
	for z in zip_files:
		os.system("wget " + z + " -O " + maindir + "tmp/down.zip")
		os.system("unzip " + maindir + "tmp/down.zip -d " + maindir + "tmp/")
		os.system("cp " + maindir + "tmp/*.xml " + maindir + "raw")
		os.system("rm " + maindir + "tmp/*")
	os.system("rm -r " + maindir + "tmp/")
	#print(zip_files)
	#os.system("rm -r things")


if __name__ == "__main__":
	USPTO_downloader("things/")
