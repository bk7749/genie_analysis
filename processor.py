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
import math
import scipy.stats as stats
		

class processor:

	bdm = None
	zonelist = None
	geniezonelist = None
	notgenielist = None
	genieprocdb = None
	thermprocdb = None
	generaldb = None
	genierawdb = None
	genielogdb = None
	thermrawdb = None
	beginTime = None
	endTime = None
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
		self.genielogdb = localdb('genielog.shelve')
		self.notgenielist = list()
		for zone in self.zonelist:
			if zone not in self.geniezonelist:
				self.notgenielist.append(zone)
		self.beginTime = self.pst.localize(datetime(2013,10,15,4,0,0),is_dst=True)
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
				setpntdevZone[zone] = []

		db.store('setpoint_dev_zone', setpntdevZone)
		db.store('setpoint_dev_hour', setpntdevHour)
		db.store('setpoint_dev_month', setpntdevMonth)
	
	def proc_setdev_anova(self):
		genieSetdevDict = self.genieprocdb.load('setpoint_dev_zone')
		thermSetdevDict = self.thermprocdb.load('setpoint_dev_zone')
		genieSetdevList = list()
		thermSetdevList = list()
		for zone, setdevList in genieSetdevDict.iteritems():
			genieSetdevList = genieSetdevList + setdevList
		for zone, setdevList in thermSetdevDict.iteritems():
			thermSetdevList = thermSetdevList + setdevList

		fVal, pVal = stats.f_oneway(genieSetdevList, thermSetdevList)

		print "setdev f-value and pvalue are: ", fVal, pVal



	def proc_genie_weighted_setpnt_dev(self, forceFlag, genieFlag):
		if genieFlag:
			rawdb = self.genierawdb
			procdb = self.genieprocdb
			setpntDict = rawdb.load('setpoint_per_zone')
			localZoneList = self.geniezonelist
		else:
			rawdb = self.thermrawdb
			procdb = self.thermprocdb
			setpntDict = rawdb.load('wcad_per_zone_filtered')
			localZoneList = self.zonelist

		setpntDevDict = dict()
		commonsetpntTemplate = 'HVAC Zone Information'
		commonsetpntSensorpoint = 'default_common_setpoint'
		commonsetpntBeginTime = datetime(2013,10,1,0,0,0)
		commonsetpntEndTime = datetime(2014,10,1,0,0,0)

		for zone in localZoneList:
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
			errorZones = []
		else:
			procdb = self.thermprocdb
			rawdb = self.thermrawdb
			setpntDiffs = rawdb.load('wcad_diff_per_zone')
			localZoneList = self.zonelist
			errorZones = rawdb.load('wcad_error_zone_list')

		setpntDiffZone = defaultdict(list)
		setpntDiffMonth= defaultdict(list)
		setpntDiffHour = defaultdict(list)
		for zone in localZoneList:
			setpntDiffZone[zone] = []

		total = 0
		sqrTotal = 0
		cnt = 0

		for zone, setpntDiff in setpntDiffs.iteritems():
			if zone in errorZones:
				continue
			for row in setpntDiff.iterrows():
				tp = row[1]['timestamp']
				val = row[1]['value']
				setpntDiffZone[zone].append(val)
				setpntDiffMonth[(tp.year-2013)*12+tp.month].append(val)
				setpntDiffHour[tp.hour].append(val)
				total += val
				sqrTotal += val*val
				cnt += 1

		avg = total/cnt
		std = math.sqrt((sqrTotal/cnt)-avg*avg)

		if genieFlag:
			print "Genie's setdev stat: ", avg, std
		else:
			print "Therm's setdev stat: ", avg, std


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
	
	def proc_setpoint_relative_energy(self, forceFlag, genieFlag, timeDiff):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
			localZoneList = self.geniezonelist
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb
			#localZoneList = self.zonelist
			localZoneList = self.notgenielist

		entireEnergyDiffList = list()
		
		setpntEnergy= rawdb.load('setpoint_energy_'+str(timeDiff.total_seconds()/3600))
		totalEnergyZone = defaultdict(float)
		sqrTotalEnergyZone = defaultdict(float)
		cntZone = defaultdict(float)
		# List init
		for zone in localZoneList:
			totalEnergyZone[zone] = 0

		for row in setpntEnergy:
			zone = row[0]
			tp = row[1]
			beforeEnergy = row[2]
			afterEnergy = row[3]
			if beforeEnergy==0 or afterEnergy==0:
				continue
			if zone=='3242' or not zone in localZoneList:
				continue
			if (tp.year-2013)*12+tp.month == 31:
				continue
			energyDiff  = (afterEnergy-beforeEnergy) / beforeEnergy * 100
			entireEnergyDiffList.append(energyDiff)
			totalEnergyZone[zone] += energyDiff
			sqrTotalEnergyZone[zone] += energyDiff * energyDiff
			cntZone[zone] += 1
		avgEnergyZone = defaultdict(float)
		stdEnergyZone = defaultdict(float)
		for zone in localZoneList:
			if cntZone[zone]==0:
				avgEnergyZone[zone] = 0
				stdEnergyZone[zone] = 0
			else:
				avgEnergyZone[zone] = totalEnergyZone[zone] / cntZone[zone]
				stdEnergyZone[zone] = math.sqrt(sqrTotalEnergyZone[zone]/cntZone[zone] - avgEnergyZone[zone]*avgEnergyZone[zone])


		entireAvg = np.mean(entireEnergyDiffList)
		entireStd = np.std(entireEnergyDiffList)

		if genieFlag:
			print "Genie's energy change mean, std: "
		else:
			print "Therm's energy change mean, std: "
		print entireAvg, entireStd

		sortedAvgEnergyZone = OrderedDict(sorted(avgEnergyZone.items(), key=operator.itemgetter(1)))
		sortedStdEnergyZone = OrderedDict()
		for zone in sortedAvgEnergyZone.keys():
			sortedStdEnergyZone[zone] = stdEnergyZone[zone]
		db.store('setpnt_relative_energy_diff_zone_'+str(timeDiff.total_seconds()/3600), sortedAvgEnergyZone)
		db.store('setpnt_relative_energy_diff_std_zone_'+str(timeDiff.total_seconds()/3600), sortedStdEnergyZone)

	def proc_setpoint_energy(self, forceFlag, genieFlag, timeDiff):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
			localZoneList = self.geniezonelist
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb
			#localZoneList = self.zonelist
			localZoneList = self.notgenielist
		
		setpntEnergy = rawdb.load('setpoint_energy_'+str(timeDiff.total_seconds()/3600))
		totalEnergyMonth = defaultdict(float)
		totalEnergyHour = defaultdict(float)
		totalEnergyZone = defaultdict(float)
		sqrTotalEnergyMonth = defaultdict(float)
		sqrTotalEnergyHour = defaultdict(float)
		sqrTotalEnergyZone = defaultdict(float)
		cntMonth = defaultdict(float)
		cntHour = defaultdict(float)
		cntZone = defaultdict(float)
		# List init
		for zone in localZoneList:
			totalEnergyZone[zone] = 0
		for month in range(10,31):
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
			sqrTotalEnergyMonth[localMonth] += energyDiff * energyDiff
			sqrTotalEnergyZone[zone] += energyDiff * energyDiff
			sqrTotalEnergyHour[tp.hour] += energyDiff * energyDiff
			cntMonth[localMonth] += 1
			cntZone[zone] += 1
			cntHour[tp.hour] += 1
		avgEnergyMonth = defaultdict(float)
		avgEnergyHour = defaultdict(float)
		avgEnergyZone = defaultdict(float)
		stdEnergyMonth = defaultdict(float)
		stdEnergyHour = defaultdict(float)
		stdEnergyZone = defaultdict(float)
		for month in range(10,31):
			if cntMonth[month]==0:
				avgEnergyMonth[month] = 0
				stdEnergyMonth[month] = 0
			else:
				avgEnergyMonth[month] = totalEnergyMonth[month] / cntMonth[month]
				stdEnergyMonth[month] = math.sqrt(sqrTotalEnergyMonth[month]/cntMonth[month] - avgEnergyMonth[month]*avgEnergyMonth[month])
		for hour in range(0,24):
			if cntHour[hour]==0:
				avgEnergyHour[hour] = 0 
				stdEnergyHour[hour] = 0
			else:
				avgEnergyHour[hour] = totalEnergyHour[hour] / cntHour[hour]
				stdEnergyHour[hour] = math.sqrt(sqrTotalEnergyHour[hour]/cntHour[hour] - avgEnergyHour[hour]*avgEnergyHour[hour])
		for zone in localZoneList:
			if cntZone[zone]==0:
				avgEnergyZone[zone] = 0
				stdEnergyZone[zone] = 0
			else:
				avgEnergyZone[zone] = totalEnergyZone[zone] / cntZone[zone]
				stdEnergyZone[zone] = math.sqrt(sqrTotalEnergyZone[zone]/cntZone[zone] - avgEnergyZone[zone]*avgEnergyZone[zone])

		sortedAvgEnergyZone = OrderedDict(sorted(avgEnergyZone.items(), key=operator.itemgetter(1)))
		sortedStdEnergyZone = OrderedDict()
		for zone in sortedAvgEnergyZone.keys():
			sortedStdEnergyZone[zone] = stdEnergyZone[zone]
		db.store('setpnt_energy_diff_zone_'+str(timeDiff.total_seconds()/3600), sortedAvgEnergyZone)
		db.store('setpnt_energy_diff_month_'+str(timeDiff.total_seconds()/3600), avgEnergyMonth)
		db.store('setpnt_energy_diff_hour_'+str(timeDiff.total_seconds()/3600), avgEnergyHour)
		db.store('setpnt_energy_diff_std_zone_'+str(timeDiff.total_seconds()/3600), sortedStdEnergyZone)
		db.store('setpnt_energy_diff_std_month_'+str(timeDiff.total_seconds()/3600), stdEnergyMonth)
		db.store('setpnt_energy_diff_std_hour_'+str(timeDiff.total_seconds()/3600), stdEnergyHour)

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
	
	def proc_freq_err_wcads(self, forceFlag):
		errorZoneList = self.thermrawdb.load('wcad_error_zone_list')
		wcadDict = self.thermrawdb.load('wcad_per_zone')
		localZoneList = self.zonelist
		zoneDict = dict()
		for zone in localZoneList:
			zoneDict[zone] = 0
		for zone, data in wcadDict.iteritems():
			if not zone in localZoneList:
				continue
			for tp in data['timestamp']:
				zoneDict[zone] += 1
		self.thermprocdb.store('wcad_per_zone_include_errors', zoneDict)

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
			errorZoneList = []
		else:
#			localZoneList = self.notgenielist
			localZoneList = self.zonelist
			procdb = self.thermprocdb
			rawdb = self.thermrawdb
			errorZoneList = self.thermrawdb.load('wcad_error_zone_list')
		if dataType == 'wcad':
			dataDict = rawdb.load(dataType+"_per_zone_filtered")
#			dataDict = rawdb.load(dataType+"_per_zone")
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
				zoneDict[zone] += 1
				if zone in errorZoneList:
					continue
				monthDict[(tp.year-2013)*12+tp.month] += 1
				hourDict[tp.hour] += 1
				if zone in self.geniezonelist:
					genieMonthDict[(tp.year-2013)*12+tp.month] += 1
					genieHourDict[tp.hour] += 1
				else:
					notgenieMonthDict[(tp.year-2013)*12+tp.month] += 1
					notgenieHourDict[tp.hour] += 1


		# Reflecting erroneous zones with average
#		genieErrorZoneCnt = 0
#		notgenieErrorZoneCnt = 0
#		for zone in errorZoneList:
#			if zone in self.geniezonelist:
#				genieErrorZoneCnt += 1
#			else:
#				notgenieErrorZoneCnt += 1
#		errorZoneCnt = len(errorZoneList)
#		normalZoneCnt = len(localZoneList)
#		genieNormalZoneCnt = len(self.geniezonelist)
#		notgenieNormalZoneCnt = normalZoneCnt - genieNormalZoneCnt
#		for key, value in monthDict.iteritems():
#			monthDict[key] = value * normalZoneCnt / (normalZoneCnt-errorZoneCnt)
#		for key, value in hourDict.iteritems():
#			hourDict[key] = value * normalZoneCnt / (normalZoneCnt-errorZoneCnt)
#		if not genieFlag:
#			for key, value in genieHourDict.iteritems():
#				genieHourDict[key] = value * genieNormalZoneCnt / (genieNormalZoneCnt-genieErrorZoneCnt)
#			for key, value in genieMonthDict.iteritems():
#				genieMonthDict[key] = value * genieNormalZoneCnt / (genieNormalZoneCnt-genieErrorZoneCnt)
#			for key, value in notgenieHourDict.iteritems():
#				notgenieHourDict[key] = value * notgenieNormalZoneCnt / (notgenieNormalZoneCnt-notgenieErrorZoneCnt)
#			for key, value in notgenieMonthDict.iteritems():
#				notgenieMonthDict[key] = value * notgenieNormalZoneCnt / (notgenieNormalZoneCnt-notgenieErrorZoneCnt)
#
#		avgZone = np.mean(zoneDict.values())
#		for zone in errorZoneList:
#			zoneDict[zone] = avgZone

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
				energies[month] = energy / area[zone] / 3600
			weekendEnergies[zone] = energies
		for zone, energies in weekdayEnergies.iteritems():
			for month, energy in energies.iteritems():
				if month>=31:
					continue
				energies[month] = energy / area[zone] / 3600
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

		dayZoneAvgs = np.mean(dayArray, axis=0)
		endZoneAvgs = np.mean(endArray, axis=0)
		dayZoneStd = np.std(dayArray, axis=0)
		endZoneStd = np.std(endArray, axis=0)

		if genieFlag:
			print "Genie's energy averages (day, end) are: "
		else:
			print "Therm's energy averages (day, end) are: "
		print np.mean(dayZoneAvgs), np.mean(endZoneAvgs)
		
		if genieFlag:
			print "Genie's energy std (day, end) are: "
		else:
			print "Therm's energy std (day, end) are: "
		print np.std(dayZoneAvgs), np.std(endZoneAvgs)

		procdb.store('zone_weekend_energy_per_month_avg', endAvgs)
		procdb.store('zone_weekday_energy_per_month_avg', dayAvgs)
		procdb.store('zone_weekend_energy_per_month_std', endStds)
		procdb.store('zone_weekday_energy_per_month_std', dayStds)
		return dayZoneAvgs, endZoneAvgs

	def proc_genie_actuated_hours(self):
		hours = self.genierawdb.load('actuated_hours_per_zone')
		monthDict = dict()
		zoneDict = dict()
		for i in range(10,31):
			monthDict[i] = 0
		for zone in self.geniezonelist:
			zoneDict[zone] = 0

		for zone, zoneHour in hours.iteritems():
			zoneDict[zone] = sum(zoneHour.values())
			for month, hour in zoneHour.iteritems():
				monthDict[month] += hour

		self.genieprocdb.store('actuated_hours_per_zone', zoneDict)
		self.genieprocdb.store('actuated_hours_per_month', monthDict)

	def proc_therm_redundant_temp_occ(self):
		redTempOccDict= self.thermrawdb.load('redundant_temp_occ_sts')
		tempOccDict = self.thermrawdb.load('temp_occ_sts')
		redOccCnt = 0
		occCnt = 0
		for occ in tempOccDict.values():
			occCnt += len(occ)
		for redOcc in redTempOccDict.values():
			redOccCnt += len(redOcc)
		
		print "redundant temp occ ratio: ", str(float(redOccCnt)/float(occCnt))
	
	def proc_range_wcad_active_therm_zones(self):
		wcads = self.thermrawdb.load('wcad_diff_per_zone')
		errorZones = self.thermrawdb.load('wcad_error_zone_list')
		largeRangeCnt = 0
		wcadsNum = self.thermprocdb.load('wcad_per_zone')
		activeZones = list()
		unstableActiveZones = 0
		for zone, num in wcadsNum.iteritems():
			if zone in errorZones or num==0:
				continue
			if num>=105:
				activeZones.append(zone)
				continue
			wcad = wcads[zone]['value']
			if len(wcad)==0:
				continue
			minVal = min(wcad)
			maxVal = max(wcad)
			if maxVal-minVal>2:
				largeRangeCnt += 1
		minSum = 0
		maxSum = 0
		for zone in activeZones:
			minVal = min(wcads[zone]['value'])
			maxVal = max(wcads[zone]['value'])
			newMin = minVal - (maxVal+minVal)/2
			newMax = maxVal - (maxVal+minVal)/2
			if newMax>=1:
				print newMax
				unstableActiveZones +=1 
			maxSum += newMax
			minSum += newMin
		print "# of active zones are: ", len(activeZones)
		print "# of unstable active zones are: ", unstableActiveZones
		print "Thermostat range is between: ", minSum/len(activeZones), maxSum/len(activeZones)
		print "Large Range therm zones: ", largeRangeCnt
	
	def proc_genie_feedback(self):
		feedbackDict = self.genierawdb.load('user_feedback')
		avgDict = defaultdict(float)
		sqrDict = defaultdict(float)
		cntDict = defaultdict(float)
		stdDict = defaultdict(float)
		posAvgDict = defaultdict(float)
		negAvgDict = defaultdict(float)
		posSqrDict = defaultdict(float)
		posCntDict = defaultdict(float)
		posStdDict = defaultdict(float)
		negSqrDict = defaultdict(float)
		negCntDict = defaultdict(float)
		negStdDict = defaultdict(float)
		totalNeg = 0
		totalPos = 0

		for username, feedbacks in feedbackDict.iteritems():
			for row in feedbacks.iterrows():
				comfort = row[1]['value']
				avgDict[username] += comfort
				sqrDict[username] += comfort * comfort
				cntDict[username] += 1.0
				if comfort>0:
					posAvgDict[username] += comfort
					posSqrDict[username] += comfort * comfort
					posCntDict[username] += 1.0
					totalPos += comfort
				elif comfort<0:
					negAvgDict[username] += comfort
					negSqrDict[username] += comfort * comfort
					negCntDict[username] += 1.0
					totalNeg += comfort
		print 'avg of pos: ', str(totalPos/sum(posCntDict.values()))
		print 'avg of neg: ', str(totalNeg/sum(negCntDict.values()))
		print 'avg of total: ', str(sum(avgDict.values())/sum(cntDict.values()))
		print '# of feedback', str(sum(cntDict.values()))
		print '# of pos feedback: ', str(sum(posCntDict.values()))
		print '# of neg feedback: ', str(sum(negCntDict.values()))
					
		for username, value in avgDict.iteritems():
			avgDict[username] = value / cntDict[username]
			stdDict[username] = sqrDict[username]/cntDict[username] - avgDict[username]*avgDict[username]
		for username, value in posAvgDict.iteritems():
			posAvgDict[username] = value / posCntDict[username]
			posStdDict[username] = posSqrDict[username]/posCntDict[username] - posAvgDict[username]*posAvgDict[username]
		for username, value in negAvgDict.iteritems():
			negAvgDict[username] = value / negCntDict[username]
			negStdDict[username] = negSqrDict[username]/negCntDict[username] - negAvgDict[username]*negAvgDict[username]

		inactiveUsers = 0
		for cnt in cntDict.values():
			if cnt==1:
				inactiveUsers += 1


		print "Total users: ", len(cntDict)

		print "Inactive users: ", inactiveUsers
		
		self.genielogdb.store('feedback_avg_per_user', avgDict)
		self.genielogdb.store('feedback_std_per_user', stdDict)
		self.genielogdb.store('feedback_cnt_per_user', cntDict)
		self.genielogdb.store('feedback_pos_avg_per_user', posAvgDict)
		self.genielogdb.store('feedback_pos_std_per_user', posStdDict)
		self.genielogdb.store('feedback_pos_cnt_per_user', posCntDict)
		self.genielogdb.store('feedback_neg_avg_per_user', negAvgDict)
		self.genielogdb.store('feedback_neg_std_per_user', negStdDict)
		self.genielogdb.store('feedback_neg_cnt_per_user', negCntDict)

	def proc_feedback_anova(self):
		feedbackTable = pd.read_excel(open('metadata/feedback_interview.xlsx','rb'))
		fVal, pVal = stats.f_oneway(feedbackTable['therm'], feedbackTable['genie'])
		print "Avg of therm feedback: ", np.mean(feedbackTable['therm'])
		print "Std of therm feedback: ", np.std(feedbackTable['therm'])
		print "Avg of genie feedback: ", np.mean(feedbackTable['genie'])
		print "Std of genie feedback: ", np.std(feedbackTable['genie'])
		print "Feedback Fval, Pval: ", fVal, pVal
	
	def proc_entire_energy(self):
		energyDict = self.generaldb.load('zone_entire_energy')
		areaDict = self.generaldb.load('area')
		genieList = list()
		thermList = list()
		for zone in self.geniezonelist:
			genieList.append(np.array(energyDict[zone])/3600/20/areaDict[zone])
		for zone in self.notgenielist:
			thermList.append(np.array(energyDict[zone])/3600/20/areaDict[zone])

		print "Avg of genie energy: ", np.mean(genieList)
		print "Std of genie energy: ", np.std(genieList)
		print "Avg of therm energy: ", np.mean(thermList)
		print "Std of therm energy: ", np.std(thermList)
		print "fVal, pVal: ", stats.f_oneway(genieList, thermList)
	
	def proc_zone_temp(self):
		startDate = datetime(2015,4,13,8)
		dateList = [startDate + timedelta(days=0),
					startDate + timedelta(days=1),
					startDate + timedelta(days=2),
					startDate + timedelta(days=3),
					startDate + timedelta(days=4)]
		beginHour = timedelta(hours=7)
		endHour = timedelta(hours=18)
		tempDict = dict()
		meanDict = dict()
		stdDict = dict()
		for zone in self.zonelist:
			frames = list()
			for date in dateList:
				frames.append(self.bdm.download_dataframe('Zone Temperature', 'PresentValue', zone, date+beginHour, date+endHour))
			temp = pd.concat(frames)
			meanDict[zone] = float(np.mean(temp))
			stdDict[zone] = float(np.std(temp))
		
		self.thermprocdb.store('zone_temperature_week_avg', meanDict)
		self.thermprocdb.store('zone_temperature_week_std', stdDict)

		
	
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
		
		self.proc_setpoint_energy(ForceFlag, GenieFlag, timedelta(hours=2))
		self.proc_setpoint_energy(ForceFlag, ThermFlag, timedelta(hours=2))
		self.proc_setpoint_relative_energy(ForceFlag, GenieFlag, timedelta(hours=2))
		self.proc_setpoint_relative_energy(ForceFlag, ThermFlag, timedelta(hours=2))

		#3-1
		self.proc_freq(ForceFlag, GenieFlag, 'actuate')
		self.proc_freq(ForceFlag, GenieFlag, 'setpoint')
		self.proc_freq(ForceFlag, ThermFlag, 'actuate')
		self.proc_freq(ForceFlag, ThermFlag, 'wcad')
	
		self.proc_genie_setpnt_diff(ForceFlag, GenieFlag)
		self.proc_genie_setpnt_diff(ForceFlag, ThermFlag)
	
		genieDayEnergy, genieEndEnergy = self.proc_energy(ForceFlag, GenieFlag)
		thermDayEnergy, thermEndEnergy = self.proc_energy(ForceFlag, ThermFlag)
		dayF, dayP = stats.f_oneway(genieDayEnergy, thermDayEnergy)
		endF, endP = stats.f_oneway(genieEndEnergy, thermEndEnergy)
		print "day fVal, pVal: ", dayF, dayP
		print "end fVal, pVal: ", endF, endP
	
		self.proc_total_count(ForceFlag)
	
		self.proc_genie_weighted_setpnt_dev(ForceFlag, GenieFlag)
		self.proc_genie_weighted_setpnt_dev(ForceFlag, ThermFlag)
		self.proc_freq_err_wcads(ForceFlag)
		self.proc_genie_actuated_hours()
		self.proc_therm_redundant_temp_occ()
		self.proc_range_wcad_active_therm_zones()
		self.proc_genie_feedback()
		self.proc_setdev_anova()
		self.proc_feedback_anova()
		self.proc_entire_energy()
