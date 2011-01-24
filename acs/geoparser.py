import os
PATH = "/home/acs/govdata-parsers/acs/"

def geoparser():
	geo_dict = {}
	cc = {
	"LOGRECNO": [ 14, 7, ""],
	"US": [ 21, 1, "C"],
	"REGION": [ 22, 1, "r"],
	"DIVISION": [ 23, 1, "D"],
	"STATECE": [ 24, 2, ""],
	"STATE": [ 26, 2, "s"],
	"COUNTY": [ 28, 3, "c"],
	"COUSUB": [ 31, 5, "j"],
	"PLACE": [ 36, 5, "i"],
	"TRACT": [ 41, 6, "T"],
	"BLKGRP": [ 47, 1, "G"],
	"CONCIT": [ 48, 5, "W"],
	"AIANHH": [ 53, 4, ""],
	"AIANHHFP": [ 57, 5, ""],
	"AIHHTLI": [ 62, 1, ""],
	"AITSCE": [ 63, 3, ""],
	"AITS": [ 66, 5, ""],
	"ANRC": [ 71, 5, ""],
	"CBSA": [ 76, 5, "m"],
	"CSA": [ 81, 3, "b"],
	"METDIV": [ 84, 5, "B"],
	"MACC": [ 89, 1, ""],
	"MEMI": [ 90, 1, ""],
	"NECTA": [ 91, 5, "n"],
	"CNECTA": [ 96, 3, ""],
	"NECTADIV": [ 99, 5, ""],
	"UA": [ 104, 5, "u"],
	"CDCURR": [ 114, 2, "g"],
	"SLDU": [ 116, 3, "L"],
	"SLDL": [ 119, 3, "l"],
	"SUBMCD": [ 136, 5, ""],
	"SDELM": [ 141, 5, "q"],
	"SDSEC": [ 146, 5, "Q"],
	"SDUNI": [ 151, 5, "k"],
	"UR": [ 156, 1, ""],
	"PCI": [ 157, 1, ""],
	"PUMA5": [ 169, 5, "e"],
	"GEOID": [ 179, 40, ""],
	"NAME": [ 219, 200, ""]
	}
	DATAPATH = 'data/Massachusetts_Tracts_Block_Groups_Only/'
	filename = [file for file in os.listdir(PATH + DATAPATH) if file[0] == 'g']
	fid = open(PATH + DATAPATH + filename[0], 'rU')
	
	for L in fid:
		logrecno = L[cc['LOGRECNO'][0]-1:cc['LOGRECNO'][0]-1 + cc['LOGRECNO'][1]]
		geo_dict[logrecno] = {}
		
		for key in cc.keys():
			if cc[key][2] != '':
				geo_dict[logrecno][cc[key][2]] = L[cc[key][0]-1:cc[key][0]-1+cc[key][1]]
		
		
	return geo_dict

g = geoparser()
print g.keys()[1]
print g[g.keys()[1]]
