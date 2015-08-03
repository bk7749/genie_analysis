import bdmanager
reload(bdmanager)
from bdmanager import bdmanager
from localdb import localdb

import sys
import traceback
from collections import defaultdict
import pandas as pd
import csv
from datetime import datetime, timedelta
import numpy as np

class analyzer():
	bdm = None
	zonelist = None
	geniezonelist = None
	genierawdb = None
	thermrawdb = None
	genieprocdb = None
	thermprocdb = None
	beginTime = datetime(2013,12,01,0,0,0)
	endTime = datetime(2015,8,1,0,0,0)
	
	confrooms = ['2109','2217','3109','3217','4109','4217']
	normrooms = ['2150']
	
	def __init__ (self):
		self.bdm = bdmanager()
		self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		#self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
		#self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
		self.genieprocdb = localdb('genieprocessed.shelve')
		self.thermprocdb = localdb('thermprocessed.shelve')
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList

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

	def collect_genie_actuate_per_zone(self, forceFlag):
		if not self.proceedCheck(self.genierawdb,'actuate_per_zone', forceFlag):
			return None

		template = 'Genie HVAC Control'
		sensorpoint = 'Actuate'
		genieActuateData = dict()
		for zone in self.geniezonelist:
			rawzone = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			processedzone = self.diff_list(rawzone, False, True, 0.1)
			genieActuateData[zone] = rawzone
		self.genierawdb.store('actuate_per_zone', genieActuateData)

	def diff_list(self, data, descendFlag, ascendFlag, threshold):
		prevDatum = data['value'][0]
		tsList = list()
		diffList = list()
		for i in range(0,len(data)):
			ts = data['timestamp'][i]
			datum = data['value'][i]
			if (descendFlag and datum < prevDatum - threshold) or \
				(ascendFlag and datum > prevDatum + threshold) or \
				( (not descendFlag and not ascendFlag) and (datum > prevDatum + threshold or datum < prevDatum - threshold) ):
				tsList.append(ts)
				diffList.append(datum)
		d = {'timestamp': tsList, 'value':diffList}
		return pd.DataFrame(d)
			

	def check_near_index_linear(self, targettp, timelist):
		for tp in timelist:
			if abs(tp-targettp)<timedelta(minutes=10):
				return True
		return False

	def check_near_index(self, targettp, timelist):
		fullLen = len(timelist)
		i1 = 0
		i2 = 1
		i3 = fullLen-1
		while i2 < fullLen:
			if abs(timelist[i2]-targettp)<timedelta(minutes=10):
				return True
			elif targettp > timelist[i2]:
				i1 = i2
				i2 = (i2+i3+1)/2
			elif targettp < timelist[i2]:
				i3 = i2
				i2 = (i1+i2)/2
			
			if i1==i2 or i2==i3:
				if abs(timelist[i2]-targettp)<timedelta(minutes=10):
					return True
				else:
					return False


# actual cooling point (pd.DataFrame), occupied command (pd.DataFrmae) -> actuate (pd.DataFrame)
	def calc_therm_actuate(self, acs, oc):
		acsDiff = self.diff_list(acs, True, False, 3)
		ocDiff = self.diff_list(oc, False, False, 0.1)
		timelist = list()
		actuate = list()
		for i in range(0,len(acsDiff)):
			tp = acsDiff['timestamp'][i]
			oneacs = acsDiff['value'][i]
			if not self.check_near_index(tp,ocDiff['timestamp']):
				actuate.append(tp)
				timelist.append(tp)
		return pd.DataFrame({'timestamp':timelist, 'value':actuate})

	def collect_therm_actuate_per_zone(self, forceFlag):
		if not self.proceedCheck(self.thermrawdb,'actuate_per_zone', forceFlag):
			return None

		thermActuateData = dict()
		for zone in self.zonelist:
			actuate = self.bdm.download_dataframe('Temp Occ Sts', 'PresentValue', zone, self.beginTime, self.endTime)
			if len(actuate)<=0:
				print("No temp occ sts at: ", zone)
				acs = self.bdm.download_dataframe('Actual Cooling Setpoint', 'PresentValue', zone, self.beginTime, self.endTime)
				oc = self.bdm.download_dataframe('Occupied Command', 'PresentValue', zone, self.beginTime, self.endTime)
				actuate = self.calc_therm_actuate(acs,oc)
			else:
				actuate = self.diff_list(actuate,False,True, 0.1)
			thermActuateData[zone] = actuate

		self.thermrawdb.store('actuate_per_zone', thermActuateData)
	
	def collect_genie_setpoint_per_zone(self, forceFlag):
		if not self.proceedCheck(self.genierawdb,'setpoint_per_zone', forceFlag):
			return None
		
		template = 'Genie HVAC Control'
		sensorpoint = 'Temperature'
		genieSetpointData = dict()
		for zone in self.geniezonelist:
			rawZoneSetpoint = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			genieSetpointData[zone] = rawZoneSetpoint
		self.genierawdb.store('setpoint_per_zone', genieSetpointData)
		
	def collect_therm_setpoint_per_zone(self, forceFlag):
		if not self.proceedCheck(self.thermrawdb,'setpoint_per_zone', forceFlag):
			return None

		template = 'Warm Cool Adjust'
		sensorpoint = 'PresentValue'
		wcadData = dict()
		for zone in self.zonelist:
			print zone
			wcad = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			if len(wcad)<=0:
				continue
			wcad = self.diff_list(wcad, False, False, 0.5)
			wcadData[zone] = wcad
		self.thermrawdb.store('setpoint_per_zone', wcadData)
		
	def calc_energy(self, ts):
		totalEnergy = 0
		for i in range(1,len(ts)):
			beforeTime = ts['timestamp'][i-1]
			afterTime = ts['timestamp'][i]
			beforePower = ts['value'][i-1]
			afterPower = ts['value'][i]
			energyBlock = (afterTime-beforeTime).total_seconds()*(afterPower+beforePower)/2
			totalEnergy += energyBlock
		return totalEnergy

	def calc_energy_diff(self, beforeTime, zone):
		template = 'HVAC Zone Power'
		sensorpoint = 'total_zone_power'
		if beforeTime.weekday()==4 or beforeTime.weekday()==6:
			afterTime = beforeTime - timedelta(days=1)
		else:
			afterTime = beforeTime + timedelta(days=1)
		beforeTimeEnd = beforeTime + timedelta(days=1)
		afterTimeEnd = afterTime + timedelta(days=1)
		beforePower = self.bdm.download_dataframe(template, sensorpoint, zone, beforeTime, beforeTimeEnd)
		afterPower = self.bdm.download_dataframe(template, sensorpoint, zone, afterTime, afterTimeEnd)
		beforeEnergy = self.calc_energy(beforePower)
		afterEnergy = self.calc_energy(afterPower)
		energyDiff = list()
		energyDiff.append(zone)
		energyDiff.append(beforeTime)
		energyDiff.append(beforeEnergy)
		energyDiff.append(afterEnergy)
		return energyDiff


# Store data structure: list of list. each list contains zone, timestamp, beforeEnergy, afterEnergy
	def collect_energy_diff(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genierawdb
		else:
			db = self.thermrawdb

		if not self.proceedCheck(db,'actuate_energy', forceFlag) and not self.proceedCheck(db, 'setpoint_energy', forceFlag):
			return None
		
		actuateData = db.load('actuate_per_zone')
		actuateEnergyData = list()
		for zone in actuateData.keys():
			print zone
			if zone!='4223':
				continue
			zoneActuate = actuateData[zone]
#			for tp in zoneActuate:
			for i in range(0,len(zoneActuate)):
				tp = zoneActuate['timestamp'][i]
				#tp = zoneActuate[i]
				beforeTime = tp.replace(hour=0,minute=0,second=0)
				energyDiff = self.calc_energy_diff(beforeTime, zone)
				actuateEnergyData.append(energyDiff)
		db.store('actuate_energy', actuateEnergyData)

		setpointData = db.load('setpoint_per_zone')
		setpointEnergyData = list()
		for zone in setpointData.keys():
			print zone
			zoneSetpoint = setpointData[zone]
			for i in range(0, len(zoneSetpoint)):
				tp = zoneSetpoint['timestamp'][i]
				beforeTime = tp.replace(hour=0,minute=0,second=0)
				energyDiff = self.calc_energy_diff(beforeTime, zone)
				setpointEnergyData.append(energyDiff)
		db.store('setpoint_energy', setpointEnergyData)
		
	def collect_occ_samples(self, forceFlag):
		if not self.proceedCheck(self.genierawdb, 'occ_samples', forceFlag):
			return None

		template = 'Occupied Command'
		sensorpoint = 'PresentValue'
		beforeBeginTime = datetime(2014,5,12,0,0,0)
		beforeEndTime = datetime(2014,5,19,0,0,0)
		afterBeginTime = datetime(2015,5,11,0,0,0)
		afterEndTime = datetime(2015,5,18,0,0,0)
		occ_samples = defaultdict(list)
		for zone in self.confrooms + self.normrooms:
			beforeOCC = self.bdm.download_dataframe(template, sensorpoint, zone, beforeBeginTime, beforeEndTime)
			afterOCC = self.bdm.download_dataframe(template, sensorpoint, zone, afterBeginTime, afterEndTime)
			occ_samples[zone].append(beforeOCC)
			occ_samples[zone].append(afterOCC)

		self.genierawdb.store('occ_samples', occ_samples)


	#return current temperature near tp)
	def get_temp_point(self, zone,tp):
		beginTime = tp - timedelta(minutes=10)
		endTime = tp + timedelta(minutes=10)
		tempData = self.bdm.download_dataframe('Zone Temperature', 'PresentValue', zone, beginTime, endTime)
		if len(tempData)<=0:
			return None
		else:
			middleIndex = len(tempData)/2
			return tempData['value'][middleIndex]

		

	def collect_temp_vs_setpnt(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genierawdb
		else:
			db = self.thermrawdb

		if not self.proceedCheck(db,'temp_vs_setpnt', forceFlag):
			return None

		tempdict = list()
		zoneSetpoints= db.load('setpoint_per_zone')
		for zone in zoneSetpoints.keys():
			print zone
			setpointList = zoneSetpoints[zone]
			for i in range(0,len(setpointList)):
				tp = setpointList['timestamp'][i]
				setpoint = setpointList['value'][i]
				currTemp = self.get_temp_point(zone, tp) # TODO: Implement this
				if currTemp == None:
					continue
				tempdict.append({currTemp:setpoint})
		db.store('temp_vs_setpnt', tempdict)

# Download Wrapper: Download and store raw data 
# Necessary data: 
#	1-1) Genie Actuate history per zone
#	1-2) Thermostat Actuate history per zone
#	2) Occupance history per zone (for several zones)
#	3-1) Setpoint count per zone temperature
#	3-2) Warm-Cool Adjust count per zone temperature
#	4) Energy consumption history per zone 
#	5) Actuate history per zone

	def collect_all_data(self):
		GenieFlag = True
		ThermFlag = False
#	1-1) Genie Actuate history per zone
		#self.collect_genie_actuate_per_zone(True)
		print("1-1 Complete")
#	1-2) Thermostat Actuate history per zone
		#self.collect_therm_actuate_per_zone(True)
		print("1-2 Complete")

#	2) Occupance history per zone (for several zones) 
#		self.collect_occ_samples(True)
		print("2 Complete")

#	3-1) Setpoint per zone temperature
#		self.collect_genie_setpoint_per_zone(True)
		print("3-1 Complete")
#	3-2) Warm-Cool Adjust per zone temperature
		#self.collect_therm_setpoint_per_zone(True)
		print("3-2 Complete")

#	4-1) Genie Energy consumption history per zone
		#self.collect_energy_diff(True, GenieFlag)
		print("4-1 Complete")
#	4-2) Thermostat Energy consumption history per zone
		#self.collect_energy_diff(True, ThermFlag)
		print("4-2 Complete")
	
#	5-1) Genie Temperature vs Setpoint map (Colormap)
		#self.collect_temp_vs_setpnt(True, GenieFlag)
		print("5-1 Complete")
#	5-1) Thermostat Temperature vs Setpoint map (Colormap)
		#self.collect_temp_vs_setpnt(True, ThermFlag)
		print("5-2 Complete")


	
	def proc_setdev(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb

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
				setdiff = float(commonsetpnt) - float(setpntList['value'][i])
				tp = setpntList['timestamp'][i]
				setpntdevZone[zone].append(setdiff)
				setpntdevHour[tp.hour].append(setdiff)
				setpntdevMonth[(tp.year-2013)*12 + tp.month].append(setdiff)

		db.store('setpoint_dev_zone', setpntdevZone)
		db.store('setpoint_dev_hour', setpntdevHour)
		db.store('setpoint_dev_month', setpntdevMonth)
	
	def proc_energy(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genieprocdb
			rawdb = self.genierawdb
		else:
			db = self.thermprocdb
			rawdb = self.thermrawdb

		if not self.proceedCheck(db,'energy_diff_month', forceFlag) and not self.proceedCheck(db, 'energy_diff_zone', foceFlag):
			return None

		actuateEnergy = rawdb.load('actuate_energy')
		setpntEnergy= rawdb.load('setpoint_energy')
		setpntEnergyMonth = defaultdict(list)
		setpntEnergyZone = defaultdict(list)
		actuateEnergyMonth = defaultdict(list)
		actuateEnergyZone = defaultdict(list)

		for row in actuateEnergy:
			zone = row[0]
			tp = row[1]
			beforeEnergy = row[2]
			afterEnergy = row[2]
		
		for row in setpntEnergy:
			zone = row[0]
			tp = row[1]
			beforeEnergy = row[2]
			afterEnergy = row[2]

		db.store('setpnt_energy_diff_month', setpntEnergyMonth)
		db.store('setpnt_energy_diff_zone', setpntEnergyZone)
		db.store('actuate_energy_diff_month', actuateEnergyMonth)
		db.store('actuate_energy_diff_zone', actuateEnergyZone)

	

# Processing Wrapper: process stored raw data for grap
# Necessary processing:
#	1) 	setpoint deviation 
#	2)	classify energy

	def process_all_data(self):
		ForceFlag = True
		GenieFlag = True
		ThermFlag = False
		#1-1
		self.proc_setdev(True, GenieFlag)
		#1-2
		self.proc_setdev(True, ThermFlag)

		#2-1
		self.proc_energy(True, GenieFlag)
		#2-2
		self.proc_energy(True, ThermFlag)

	def plot_setpnt_dev(self):
		pass

# Plotting Wrapper
# 	1)	setpoint_dev
	def plot_all_data(self):
		self.plot_setpnt_dev()
