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
from pytz import timezone
		

class processor:

	bdm = None
	zonelist = None
	geniezonelist = None
	notgenielist = None
	genieprocdb = None
	thermprocdb = None
	generaldb = None
	genierawdb = None
	thermrawdb = None
	beginTime = datetime(2013,12,1,0,0,0)
	endTime = datetime(2015,6,25,23,0,0)
	confrooms = ['2109', '2217', '3109', '3127', '4109', '4217']
	normrooms = ['2150']
	pst = timezone('US/Pacific')

	def __init__(self):
		self.bdm = bdmanager()
		#self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		#self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
		self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
		self.genieprocdb = localdb('genieprocessed.shelve')
		self.thermprocdb = localdb('thermprocessed.shelve')
		self.generaldb = localdb('general.shelve')
		self.notgenielist = list()
		for zone in self.zonelist:
			if zone not in self.geniezonelist:
				self.notgenielist.append(zone)
		self.beginTime = self.pst.localize(datetime(2013,12,1,0,0,0),is_dst=True)
		self.endTime = self.pst.localize(datetime(2015,6,25,0,0,0),is_dst=True)
		
	
	def proceedCheck(self, db, tag, forceFlag):
		if not forceFlag:
			if db.check(tag):
				return False
			else:
				return True
		else:
			return True
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList

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
		for zone, setpntList in setpoints.iteritems():
			commonsetpnt = self.bdm.download_dataframe(commonsetpntTemplate, commonsetpntSensorpoint, zone, commonsetpntBeginTime, commonsetpntEndTime)
			if len(commonsetpnt)>0:
				commonsetpnt = commonsetpnt['value'][0]
			else:
				continue
#			setpntList = setpoints[zone]
			for i in range(0,len(setpntList)):
				setdiff = float(setpntList['value'][i]) - float(commonsetpnt) 
				tp = setpntList['timestamp'][i]
				setpntdevZone[zone].append(setdiff)
				setpntdevHour[tp.hour].append(setdiff)
				setpntdevMonth[(tp.year-2013)*12 + tp.month].append(setdiff)

		for zone in self.geniezonelist:
			if not zone in setpntdevZone.keys():
				setpntdevZone[zone] = 0

		db.store('setpoint_dev_zone', setpntdevZone)
		db.store('setpoint_dev_hour', setpntdevHour)
		db.store('setpoint_dev_month', setpntdevMonth)

	def proc_genie_weighted_setpnt_dev(self, forceFlag, genieFlag):
		if genieFlag:
			rawdb = self.genierawdb
			procdb = self.genieprocdb
			setpntDict = rawdb.load('setpoint_per_zone')
			localZoneList = self.geniezonelist
		else:
			rawdb = self.thermrawdb
			procdb = self.thermprocdb
			setpntDict = rawdb.load('wcad_per_zone')
			localZoneList = self.zonelist

		setpntDevDict = dict()
		commonsetpntTemplate = 'HVAC Zone Information'
		commonsetpntSensorpoint = 'default_common_setpoint'
		commonsetpntBeginTime = datetime(2013,10,1,0,0,0)
		commonsetpntEndTime = datetime(2014,10,1,0,0,0)

		for zone in localZoneList:
			print zone
			try:
				setpnts = setpntDict[zone]	
			except:
				setpnts = []
			if len(setpnts)==0:
				if genieFlag:
					commonsetpnt = self.bdm.download_dataframe(commonsetpntTemplate, commonsetpntSensorpoint, zone, commonsetpntBeginTime, commonsetpntEndTime)
					if len(commonsetpnt)>0:
						commonsetpnt = commonsetpnt['value'][0]
						setpntDevDict[zone] = [commonsetpnt,0]
				else:
					setpntDevDict[zone] = [0,0]
				continue
			total=0.0
			sqTotal = 0.0
			setpntIter = setpnts.iterrows()
			prevRow = setpntIter.next()
			while prevRow[1]['timestamp'] < self.beginTime:
				prevRow = setpntIter.next()
			currVal = prevRow[1]['value']
			dt = (prevRow[1]['timestamp']-self.beginTime).total_seconds()
			total += dt * currVal
			sqTotal += dt * currVal * currVal
			for row in setpntIter:
				if row[1]['timestamp']>=self.endTime:
					break
				currVal = row[1]['value']
				dt = (row[1]['timestamp']-prevRow[1]['timestamp']).total_seconds()
				total += dt * currVal 
				sqTotal += dt * currVal * currVal
				prevRow = row
			dt = (self.endTime-prevRow[1]['timestamp']).total_seconds()
			val = prevRow[1]['value']
			total += dt * val
			sqTotal += dt * val * val
			totalDT = (self.endTime-self.beginTime).total_seconds()
			avg = total / totalDT
			std = sqTotal/totalDT - avg*avg
			setpntDevDict[zone] = [avg, std]

		if genieFlag:
			procdb.store('weighted_setpoint_dev_zone', setpntDevDict)
		else:
			procdb.store('weighted_wcad_dev_zone', setpntDevDict)

	
	def proc_genie_setpnt_diff(self, forceFlag, genieFlag):
		if genieFlag:
			procdb = self.genieprocdb
			rawdb = self.genierawdb
			setpntDiffs = rawdb.load('setpoint_diff_per_zone')
			localZoneList = self.geniezonelist
		else:
			procdb = self.thermprocdb
			rawdb = self.thermrawdb
			setpntDiffs = rawdb.load('wcad_diff_per_zone')
			localZoneList = self.zonelist

		setpntDiffZone = defaultdict(list)
		setpntDiffMonth= defaultdict(list)
		setpntDiffHour = defaultdict(list)
		for zone in localZoneList:
			setpntDiffZone[zone] = []

		for zone, setpntDiff in setpntDiffs.iteritems():
			for row in setpntDiff.iterrows():
				tp = row[1]['timestamp']
				val = row[1]['value']
				setpntDiffZone[zone].append(val)
				setpntDiffMonth[(tp.year-2013)*12+tp.month].append(val)
				setpntDiffHour[tp.hour].append(val)

		if genieFlag:
			procdb.store('setpoint_diff_per_zone', setpntDiffZone)
			procdb.store('setpoint_diff_per_month', setpntDiffMonth)
			procdb.store('setpoint_diff_per_hour', setpntDiffHour)
		else:
			procdb.store('wcad_diff_per_zone', setpntDiffZone)
			procdb.store('wcad_diff_per_month', setpntDiffMonth)
			procdb.store('wcad_diff_per_hour', setpntDiffHour)
	
	def proc_therm_setdev(self, forceFlag):
		wcads = self.thermrawdb.load('wcad_per_zone_filtered')
		wcadDevZone = defaultdict(list)
		wcadDevMonth = defaultdict(list)
		wcadDevHour = defaultdict(list)

		#for zone in self.notgenielist:
		for zone in self.zonelist:
			wcadDevZone[zone] = list()
		for hour in range(0,24):
			wcadDevHour[hour] = list()
		for month in range(12,31):
			wcadDevMonth[month] = list()
		#for zone in wcads.keys():
		#for zone in self.notgenielist:
		for zone in self.zonelist:
			if zone in wcads.keys():
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

	def proc_setpoint_energy(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
			localZoneList = self.geniezonelist
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb
			#localZoneList = self.zonelist
			localZoneList = self.notgenielist
		
		setpntEnergy= rawdb.load('setpoint_energy')
		totalEnergyMonth = defaultdict(float)
		totalEnergyHour = defaultdict(float)
		totalEnergyZone = defaultdict(float)
		# List init
		for zone in localZoneList:
			totalEnergyZone[zone] = 0
		for month in range(12,31):
			totalEnergyMonth[month] = 0
		for hour in range(0,24):
			totalEnergyHour[hour] = 0

		for row in setpntEnergy:
			zone = row[0]
			tp = row[1]
			beforeEnergy = row[2]
			afterEnergy = row[3]
			if zone=='3242' or not zone in localZoneList:
				continue
			if (tp.year-2013)*12+tp.month == 31:
				continue
			localMonth = (tp.year-2013)*12+tp.month
			energyDiff  = afterEnergy - beforeEnergy
			totalEnergyMonth[localMonth] += energyDiff
			totalEnergyZone[zone] += energyDiff
			totalEnergyHour[tp.hour] += energyDiff 

		sortedTotalEnergyZone = OrderedDict(sorted(totalEnergyZone.items(), key=operator.itemgetter(1)))
		db.store('setpnt_energy_diff_zone', sortedTotalEnergyZone)
		db.store('setpnt_energy_diff_month', totalEnergyMonth)
		db.store('setpnt_energy_diff_hour', totalEnergyHour)

	def proc_energy_change(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
			localZoneList = self.geniezonelist
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb
			localZoneList = self.notgenielist
			#localZoneList = self.zonelist

		if not self.proceedCheck(db,'energy_save_month', forceFlag) and not self.proceedCheck(db, 'energy_save_zone', foceFlag) and not self.proceedCheck(db,'energy_waste_month', forceFlag) and not self.proceedCheck(db, 'energy_waste_zone', foceFlag):
			return None

		actuateEnergy = rawdb.load('actuate_energy')
		setpntEnergy= rawdb.load('setpoint_energy')
		energySaveMonth= defaultdict(float)
		energySaveZone = defaultdict(float)
		energyWasteMonth = defaultdict(float)
		energyWasteZone = defaultdict(float)
		totalEnergyMonth = defaultdict(float)
		totalEnergyZone = defaultdict(float)

		for row in actuateEnergy+setpntEnergy:
			zone = row[0]
			tp = row[1]
			beforeEnergy = row[2]
			afterEnergy = row[3]
			if zone not in localZoneList:
				continue
			if zone == '3242':
				continue
			if (tp.year-2013)*12+tp.month == 31:
				continue
			localMonth = (tp.year-2013)*12+tp.month
			if afterEnergy>beforeEnergy:
				energyWasteMonth[localMonth] += afterEnergy - beforeEnergy
				energyWasteZone[zone] += afterEnergy - beforeEnergy

			else:
				energySaveMonth[(tp.year-2013)*12+tp.month] -= afterEnergy - beforeEnergy
				energySaveZone[zone] -= afterEnergy - beforeEnergy
			totalEnergyMonth[localMonth] += afterEnergy - beforeEnergy
			totalEnergyZone[zone] += afterEnergy - beforeEnergy

		for zone in localZoneList:
			if not zone in energySaveZone:
				energySaveZone[zone] = 0
			if not zone in energyWasteZone:
				energyWasteZone[zone] = 0
			if not zone in totalEnergyZone:
				totalEnergyZone[zone] = 0

		sortedTotalEnergyZone = OrderedDict(sorted(totalEnergyZone.items(), key=operator.itemgetter(1)))
		sortedEnergySaveZone = OrderedDict(sorted(energySaveZone.items(), key=operator.itemgetter(1)))
		sortedEnergyWasteZone = dict()
		for zone in sortedEnergySaveZone.keys():
			if zone in energyWasteZone:
				sortedEnergyWasteZone[zone] = energyWasteZone[zone]
			else:
				sortedEnergyWasteZone[zone] = 0

		db.store('energy_save_month', energySaveMonth)
		db.store('energy_waste_month', energyWasteMonth)
		#db.store('energy_save_zone', energySaveZone)
		#db.store('energy_waste_zone', energyWasteZone)
		db.store('energy_save_zone', sortedEnergySaveZone)
		db.store('energy_waste_zone', sortedEnergyWasteZone)
		db.store('energy_diff_zone', sortedTotalEnergyZone)
		db.store('energy_diff_month', totalEnergyMonth)

	def proc_freq(self, forceFlag, genieFlag, dataType):
		if genieFlag:
			procdb = self.genieprocdb
			rawdb = self.genierawdb
			localZoneList = self.geniezonelist
			if dataType=='actuate':
				#print "Wrong function is used to claculate Genie\'s actuate frequency"
				#print "Please use proc_genie_actu_freq"
				print "Should I use proc_genie_actu_freq??"
#				return None
		else:
#			localZoneList = self.notgenielist
			localZoneList = self.zonelist
			procdb = self.thermprocdb
			rawdb = self.thermrawdb
		if dataType == 'wcad':
			dataDict = rawdb.load(dataType+"_per_zone_filtered")
		else:
			dataDict = rawdb.load(dataType+"_per_zone")
		
		# Init dicts.
		monthDict = defaultdict(int)
		hourDict = defaultdict(int)
		zoneDict = defaultdict(int)
		notgenieMonthDict = defaultdict(int)
		genieMonthDict = defaultdict(int) # This is for thermostats data in genie zones.
		notgenieHourDict = defaultdict(int)
		genieHourDict = defaultdict(int) # This is for thermostats data in genie zones.
		for i in range(12,31):
			monthDict[i] = 0
			genieMonthDict[i] = 0
			notgenieMonthDict[i] = 0
		for zone in localZoneList:
			zoneDict[zone] = 0
		for hour in range(0,24):
			hourDict[hour] = 0
			genieHourDict[hour] = 0
			notgenieHourDict[hour] = 0
		for zone, data in dataDict.iteritems():
			if not zone in localZoneList:
				continue
			for tp in data['timestamp']:
				monthDict[(tp.year-2013)*12+tp.month] += 1
				hourDict[tp.hour] += 1
				if zone in self.geniezonelist:
					genieMonthDict[(tp.year-2013)*12+tp.month] += 1
					genieHourDict[tp.hour] += 1
				else:
					notgenieMonthDict[(tp.year-2013)*12+tp.month] += 1
					notgenieHourDict[tp.hour] += 1
				zoneDict[zone] += 1

		procdb.store(dataType+'_per_month', monthDict)
		procdb.store(dataType+'_per_hour', hourDict)
		procdb.store(dataType+'_per_zone', zoneDict)
		if not genieFlag:
			procdb.store('genie_'+dataType+'_per_month', genieMonthDict)
			procdb.store('genie_'+dataType+'_per_hour', genieHourDict)
			procdb.store('notgenie_'+dataType+'_per_month', notgenieMonthDict)
			procdb.store('notgenie_'+dataType+'_per_hour', notgenieHourDict)

	
	def proc_total_count(self, forceFlag):
		# TODO: IMPLEMENT THIS!!
		# calculating entire energy consumptions
		zoneEnergies = self.generaldb.load('zone_energy_per_month')
		genieEnergies = defaultdict(float)
		thermEnergies = defaultdict(float)
		genieTotalEnergy = 0
		thermTotalEnergy = 0
		for zone in self.zonelist:
			if zone in self.geniezonelist:
				for month, energy in zoneEnergies[zone].iteritems():
					genieEnergies[month] += energy
					genieTotalEnergy += energy
			else:
				for month, energy in zoneEnergies[zone].iteritems():
					thermEnergies[month] += energy
					thermTotalEnergy += energy

		self.generaldb.store('genie_zone_energy_per_month', genieEnergies)
		self.generaldb.store('therm_zone_energy_per_month', thermEnergies)
		self.generaldb.store('genie_total_energy', genieTotalEnergy)
		self.generaldb.store('therm_total_energy', thermTotalEnergy)

	def proc_energy(self, forceFlag, genieFlag):
		weekendEnergies = self.generaldb.load('zone_weekend_energy_per_month')
		weekdayEnergies = self.generaldb.load('zone_weekday_energy_per_month')
		area = self.generaldb.load('area')

		if genieFlag:
			localZoneList = self.geniezonelist
			procdb = self.genieprocdb
		else:
			localZoneList = self.notgenielist
			procdb = self.thermprocdb

		for zone, energies in weekendEnergies.iteritems():
			for month, energy in energies.iteritems():
				if month>=31:
					continue
				energies[month] = energy / area[zone]
			weekendEnergies[zone] = energies
		for zone, energies in weekdayEnergies.iteritems():
			for month, energy in energies.iteritems():
				if month>=31:
					continue
				energies[month] = energy / area[zone]
			weekdayEnergies[zone] = energies

		for zone in weekendEnergies.keys():
			if not zone in localZoneList:
				del weekendEnergies[zone]
		for zone in weekdayEnergies.keys():
			if not zone in localZoneList:
				del weekdayEnergies[zone]

		endArray = pd.DataFrame(weekendEnergies)
		dayArray = pd.DataFrame(weekdayEnergies)

		endAvgs = np.mean(endArray, axis=1)
		dayAvgs = np.mean(dayArray, axis=1)
		endStds = np.std(endArray, axis=1)
		dayStds = np.std(dayArray, axis=1)

		procdb.store('zone_weekend_energy_per_month_avg', endAvgs)
		procdb.store('zone_weekday_energy_per_month_avg', dayAvgs)
		procdb.store('zone_weekend_energy_per_month_std', endStds)
		procdb.store('zone_weekday_energy_per_month_std', dayStds)
	
	def process_all_data(self):
		print "Start processing all data"
		ForceFlag = True
		GenieFlag = True
		ThermFlag = False
		#1-1
		self.proc_genie_setdev(ForceFlag)
		print "Finish processing genie setpoint deviation"
		#1-2
		self.proc_therm_setdev(ForceFlag)
		print "Finish processing thermostat setpoint deviation"

		#2-1
		self.proc_energy_change(ForceFlag, GenieFlag)
		print "Finish processing genie energy analysis"
		#2-2
		self.proc_energy_change(ForceFlag, ThermFlag)
		print "Finish processing thermostat energy analysis"
		
		self.proc_setpoint_energy(ForceFlag, GenieFlag)
		self.proc_setpoint_energy(ForceFlag, ThermFlag)

		#3-1
		self.proc_freq(ForceFlag, GenieFlag, 'actuate')
		self.proc_freq(ForceFlag, GenieFlag, 'setpoint')
		self.proc_freq(ForceFlag, ThermFlag, 'actuate')
		self.proc_freq(ForceFlag, ThermFlag, 'wcad')
	
		self.proc_genie_setpnt_diff(ForceFlag, GenieFlag)
		self.proc_genie_setpnt_diff(ForceFlag, ThermFlag)
	
		self.proc_energy(ForceFlag, GenieFlag)
		self.proc_energy(ForceFlag, ThermFlag)
	
		self.proc_total_count(ForceFlag)
	
		self.proc_genie_weighted_setpnt_dev(ForceFlag, GenieFlag)
		self.proc_genie_weighted_setpnt_dev(ForceFlag, ThermFlag)
