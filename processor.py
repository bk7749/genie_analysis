import bdmanager

reload(bdmanager)
from bdmanager import bdmanager
from localdb import localdb

import sys
import csv
import traceback
from collections import defaultdict, OrderedDict
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import operator
		

class processor:

	bdm = None
	zonelist = None
	geniezonelist = None
	genieprocdb = None
	thermprocdb = None
	genierawdb = None
	thermrawdb = None
	beginTime = datetime(2013,12,1,0,0,0)
	endTime = datetime(2015,8,1,0,0,0)
	confrooms = ['2109', '2217', '3109', '3127', '4109', '4217']
	normrooms = ['2150']

	def __init__(self):
		self.bdm = bdmanager()
		self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
		#self.zonelist = self.csv2list('metadata\zonelist.csv')
		#self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
		self.genieprocdb = localdb('genieprocessed.shelve')
		self.thermprocdb = localdb('thermprocessed.shelve')
	
	def proceedCheck(self, db, tag, forceFlag):
		if not forceFlag:
			if db.check(tag):
				return False
			else:
				return True
		else:
			try:
				db.remove(tag)
			except:
				pass
			return True
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList


	def process_all_data(self):
		print "Start processing all data"
		ForceFlag = True
		GenieFlag = True
		ThermFlag = False
		#1-1
		self.proc_genie_setdev(True)
		print "Finish processing genie setpoint deviation"
		#1-2
		self.proc_therm_setdev(True)
		print "Finish processing thermostat setpoint deviation"

		#2-1
		self.proc_energy(True, GenieFlag)
		print "Finish processing genie energy analysis"
		#2-2
		self.proc_energy(True, ThermFlag)
		print "Finish processing thermostat energy analysis"
	
	def proc_genie_setdev(self, forceFlag):
		db = self.genieprocdb
		rawdb = self.genierawdb
		if not self.proceedCheck(db,'setpoint_dev_zone', forceFlag) and not self.proceedCheck(db, 'setpoint_dev_hour', forceFlag) and not self.proceedCheck(db,'setpoint_dev_month'):
			return None

		setpoints = rawdb.load('setpoint_per_zone')
		setpntdevZone = defaultdict(list)
		setpntdevHour = defaultdict(list)
		setpntdevMonth = defaultdict(list)
		commonsetpntTemplate = 'HVAC Zone Information'
		commonsetpntSensorpoint = 'default_common_setpoint'
		commonsetpntBeginTime = datetime(2013,10,1,0,0,0)
		commonsetpntEndTime = datetime(2014,10,1,0,0,0)
		for zone in setpoints.keys():
			commonsetpnt = self.bdm.download_dataframe(commonsetpntTemplate, commonsetpntSensorpoint, zone, commonsetpntBeginTime, commonsetpntEndTime)
			if len(commonsetpnt)>0:
				commonsetpnt = commonsetpnt['value'][0]
			else:
				continue
			setpntList = setpoints[zone]
			for i in range(0,len(setpntList)):
				setdiff = float(setpntList['value'][i]) - float(commonsetpnt) 
				tp = setpntList['timestamp'][i]
				setpntdevZone[zone].append(setdiff)
				setpntdevHour[tp.hour].append(setdiff)
				setpntdevMonth[(tp.year-2013)*12 + tp.month].append(setdiff)

		db.store('setpoint_dev_zone', setpntdevZone)
		db.store('setpoint_dev_hour', setpntdevHour)
		db.store('setpoint_dev_month', setpntdevMonth)
	
	def proc_therm_setdev(self, forceFlag):
		if not self.proceedCheck(self.thermprocdb, 'wcad_dev_zone', forceFlag) and not self.proceedCheck(self.thermprocdb, 'setpoint_dev_hour', forceFlag) and not self.proceedCheck(self.thermprocdb, 'setpoint_dev_mont'):
			return None
		wcads = self.thermrawdb.load('wcad_per_zone')
		wcadDevZone = defaultdict(list)
		wcadDevMonth = defaultdict(list)
		wcadDevHour = defaultdict(list)
		for zone in wcads.keys():
			wcadList = wcads[zone]
			for i in range(0,len(wcadList)):
				tp = wcadList['timestamp'][i]
				wcad = wcadList['value'][i]
				wcadDevZone[zone].append(wcad)
				wcadDevHour[tp.hour].append(wcad)
				wcadDevMonth[(tp.year-2013)*12+tp.month].append(wcad)

		self.thermprocdb.store('setpoint_dev_zone', wcadDevZone)
		self.thermprocdb.store('setpoint_dev_hour', wcadDevHour)
		self.thermprocdb.store('setpoint_dev_month', wcadDevMonth)

	def proc_energy(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb

		if not self.proceedCheck(db,'energy_save_month', forceFlag) and not self.proceedCheck(db, 'energy_save_zone', foceFlag) and not self.proceedCheck(db,'energy_waste_month', forceFlag) and not self.proceedCheck(db, 'energy_waste_zone', foceFlag):
			return None

		actuateEnergy = rawdb.load('actuate_energy')
		setpntEnergy= rawdb.load('setpoint_energy')
		energySaveMonth= defaultdict(float)
		energySaveZone = defaultdict(float)
		energyWasteMonth = defaultdict(float)
		energyWasteZone = defaultdict(float)

		for row in actuateEnergy+setpntEnergy:
			zone = row[0]
			tp = row[1]
			beforeEnergy = row[2]
			afterEnergy = row[3]
			if afterEnergy>beforeEnergy:
				energyWasteMonth[(tp.year-2013)*12+tp.month] += afterEnergy - beforeEnergy
				energyWasteZone[zone] += afterEnergy - beforeEnergy
			else:
				energySaveMonth[(tp.year-2013)*12+tp.month] -= afterEnergy - beforeEnergy
				energySaveZone[zone] -= afterEnergy - beforeEnergy
		
#		for row in setpntEnergy:
#			zone = row[0]
#			tp = row[1]
#			beforeEnergy = row[2]
#			afterEnergy = row[2]
#			if afterEnergy>beforeEnergy:
#				energyWasteMonth[(tp.year-2013)*12+tp.month] += afterEnergy - beforeEnergy
#				energyWasteZone[zone] += afterEnergy - beforeEnergy
#			else:
#				energySaveMonth[(tp.year-2013)*12+tp.month] -= afterEnergy - beforeEnergy
#				energySaveZone[zone] -= afterEnergy - beforeEnergy

		db.store('energy_save_month', energySaveMonth)
		db.store('energy_save_zone', energySaveZone)
		db.store('energy_waste_month', energyWasteMonth)
		db.store('energy_waste_zone', energyWasteZone)
