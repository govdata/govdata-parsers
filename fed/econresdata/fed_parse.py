import re
from BeautifulSoup import BeautifulSoup
from starflow import utils

##checks whether it is available in the data download program of the fed site
def hasDataDownloadProgram(li):
    x = re.compile('.*\.aspx.*')
    return li.findAll('a',{'href':x}) != []

##extacts the rel from the actual links
def extractRel(urlString):
    regex = re.compile('.*\.aspx\?rel=(.*)')
    return regex.match(urlString).groups()[0]

def getDataCodes(string):
    pattern = '([A-Z](\.[0-9]+)+)+'
    all = re.findall(pattern,string)
    return [match[0] for match in all]

##apply this to an element of the list, to get all the Data codes it refers to in the content sections
def getContentDataCodes(li):
    return set(getDataCodes(getContentStringFromLi(li)))

##looks at both the contents and the contents of the children
def getContentStringFromLi(li):
    return reduce(lambda x,y: x + y, [utils.Contents(li)] + [utils.Contents(achild) for achild in li.findAll('a')])

file = open('statisticsdata.htm')
linkTable =  BeautifulSoup(file,convertEntities='html').find('table',{'class':'stats'})
headers = linkTable.findAll('h2')
links = [header.findNext().findAll('li') for header in headers]

res = [[(utils.Contents(h), getContentStringFromLi(l), getContentDataCodes(l), hasDataDownloadProgram(l)) for l in L] for (L,h) in zip(links,headers)]
