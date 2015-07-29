import bdmanager
reload(bdmanager)
from bdmanager import bdmanager

import sys
import traceback
from collections import defaultdict
import pandas as pd
import csv

class analyzer():
	bdm = None
	zonelist = None
	geniezonelist = None
	
	def __init__ (self):
		self.bdm = bdmanager()
		self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList

# Wrapper: Download and store raw data 
# Necessary data: 
#	1) Actuate history per zone
#	2) Occupance history per zone (for several zones)
#	3-1) Setpoint count per zone temperature
#	3-2) Warm-Cool Adjust count per zone temperature
#	4) Energy consumption history per zone 
#	5) Actuate history per zone
	def collect_all_data(self):

#	1) Actuate history per zone
		template = 'Genie HVAC Control'
		sensorpoint = 'Actuate'
