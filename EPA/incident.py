#!/opt/bin/python
from os import system
from os.path import exists, basename
import sys
import tabular as tb


headers_list=[]

##fnames
contact_name='NATIONAL_CONTACT_FILE.CSV'
single_name='NATIONAL_SINGLE.CSV'
interest_name='NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV'
#supp_interest='AL_SUPP_INTEREST_FILE.CSV'
name_lst=[single_name,interest_name,contact_name]

dic_tab={}
dic_head={}

headers=[]
for name in name_lst :
  F = open(name,'r')
  header = F.readline()
  headersplit = header.split(',')
  headersplit[-1]=headersplit[-1].strip('/\n')
  headersplit=[h.strip('"') for h in headersplit]
  tbarr = tb.tabarray(SVfile = name,skiprows=0,names=headersplit)
  dic_tab[name]=tbarr
  dic_head[name]=headersplit
  headers=headers+headersplit

dic_tab[single_name].renamecol('CREATE_DATE','CREATE_DATE_SINGLE')
dic_tab[single_name].renamecol('UPDATE_DATE','UPDATE_DATE_SINGLE')

tb_final=dic_tab[interest_name].join(dic_tab[single_name],keycols=['REGISTRY_ID'])







############ BELOW is to handle loading contacts  SKELETON SHOWED
Z=dic_tab[contact_name]

# THIS WORKS BUT NEED TO DO IT FOR ALL NOT JUST THE FIRST 100 ROWS and for all the columns not just MAILING_ADDRESS and EMAIL_ADDRESS
H = Z[:100].aggregate(On=['PGM_SYS_ID','INTEREST_TYPE'],AggList=[('CONTACT',lambda x : '|'.join( [repr({'EMAIL_ADDRESS':y['EMAIL_ADDRESS'],'MAILING_ADDRESS':y['MAILING_ADDRESS']}) for y in x]), ['EMAIL_ADDRESS','MAILING_ADDRESS'])],AggFunc=lambda x : 'Ignored')

### THIS WAS MY ATTEMPT TO DO THAT BUT FAILED
#cf=dic_tab[contact_name].dtype.names[4:]
#Aggreageted = (dic_tab[contact_name])[['PGM_SYS_ID','INTEREST_TYPE','MAILING_ADDRESS','EMAIL_ADDRESS']][:100].aggregate(On=['PGM_SYS_ID','INTEREST_TYPE'],AggList=[('CONTACT',lambda x : '|'.join([repr({cf[0]:y[cf[0]],cf[1]:y[cf[1]],cf[2]:y[cf[2]],cf[3]:y[cf[3]],cf[4]:y[cf[4]],cf[5]:y[cf[5]],cf[6]:y[cf[6]],cf[7]:y[cf[7]],cf[8]:y[cf[8]],cf[9]:y[cf[9]],cf[10]:y[cf[10]],cf[11]:y[cf[11]],cf[12]:y[cf[12]],cf[13]:y[cf[13]],cf[14]:y[cf[14]],cf[15]:y[cf[15]],cf[16]:y[cf[16]]}) for y in x]), cf)],AggFunc=lambda x : 'Ingored')


## NEXT STEP IS TO DELETE ALL COLUMNS IN AGGREGATED EXCEPT FOR ['PGM_SYS_ID','INTEREST_TYPE','CONTACT']
# FINAL_CONTACT=Aggreageted([['PGM_SYS_ID','INTEREST_TYPE','CONTACT']])

## AND THEN JUST DO JOIN AGAIN
# tb_final_with_contact=tb_final.join(FINAL_CONTACT,keycols=['PGM_SYS_ID','INTEREST_TYPE'])


