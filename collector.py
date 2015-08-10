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

class collector():
	bdm = None
	zonelist = None
	geniezonelist = None
	genierawdb = None
	thermrawdb = None
	beginTime = datetime(2013,12,1,0,0,0)
	endTime = datetime(2015,7,31,23,0,0)
	confrooms = ['2109', '2217', '3109', '3127', '4109', '4217']
	normrooms = ['2150']

	def __init__(self):
		self.bdm = bdmanager()
		self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
		self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList

# if forceFlag is turned on, db deletes data and collect again
# tag means data name inside the db
	def proceedCheck(self, db, tag, forceFlag):
		if not forceFlag:
			if db.check(tag):
				return False
			else:
				return True
		else:
			if db.check(tag):
				db.remove(tag)
				pass
			return True

	def collect_genie_actuate_per_zone(self, forceFlag):
		if not self.proceedCheck(self.genierawdb,'actuate_per_zone', forceFlag):
			return None
		genieActuateData = dict()
		template = 'Genie HVAC Control'
		sensorpoint = 'off-time'
		for zone in self.geniezonelist:
			offtime = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			if len(offtime)<=0:
				continue
			newofftimeTime = list()
			newofftimeVal = list()
			for i, value in enumerate(offtime['value']):
				if value != None:
					newofftimeTime.append(offtime['timestamp'][i])
					newofftimeVal.append(3)
			newofftime = self.bdm.twolist2pddf(newofftimeTime, newofftimeVal)
			genieActuateData[zone] = newofftime
		self.genierawdb.store('actuate_per_zone', genieActuateData)
	
	def diff_list_deprecated(self, data, descendFlag, ascendFlag, threshold):
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
			prevDatum = datum
		d = {'timestamp': tsList, 'value':diffList}
		return pd.DataFrame(d)
		
	def diff_list(self, data, descendFlag, ascendFlag, threshold):
		if descendFlag and ascendFlag:
			print "wrong semantic to use diff_list"
			return None
		afterData = data['value'][1:]
		beforeData = data['value'][:-1]
		beforeData.index = range(1,len(beforeData)+1)
		afterData.index = range(1,len(afterData)+1)
		if descendFlag:
			diffIdx = pd.concat([pd.Series((False)), afterData<=beforeData-threshold])
		elif ascendFlag:
			diffIdx = pd.concat([pd.Series((False)), afterData>=beforeData+threshold])
		elif not descendFlag and not ascendFlag:
			diffIdx = pd.concat([pd.Series((False)), abs(afterData-beforeData)>=threshold])
		diffVals = data['value'][diffIdx]
		diffTime = data['timestamp'][diffIdx]
		diffDF = pd.DataFrame({'timestamp':diffTime, 'value':diffVals})
		diffDF.index = range(0,len(diffDF))
		return diffDF
	
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
	
	# Actual Cooling Setpoint: acs (DataFrame); Occupied Command: oc(DataFrame)
	# -> Actuate (DataFrame)
	def calc_therm_actuate(self, acs, oc):
		acsDiff = self.diff_list(acs, True, False, 3)
		ocDiff = self.diff_list(oc, False, False, 0.1)
#		acsDiffIdx = pd.concat([pd.Series((False)), acs['value'][1:]<acs['value'][:-1]-3])
#		acsDiffVals = acs['value'][acsDiffIdx]
#		acsDiffTime = acs['timestamp'][acsDiffIdx]
#		acsDiff = pd.DataFrame({'timestamp':acsDiffTime, 'value':acsDiffVals})
#		acsDiff.index = range(0,len(acsDiff))
#		ocDiffIdx = pd.concat([pd.Series((False)), oc['value'][1:]<oc['value'][:-1]-3])
#		ocDiffVals = oc['value'][ocDiffIdx]
#		ocDiffTime = oc['timestamp'][ocDiffIdx]
#		ocDiff = pd.DataFrame({'timestamp':ocDiffTime, 'value':ocDiffVals})
#		acsDiff.index = range(0,len(acsDiff))
		
		timelist = list()
		actuate = list()
		for i in range(0,len(acsDiff)):
			tp = acsDiff['timestamp'][i]
			oneacs = acsDiff['value'][i]
			#if not self.check_near_index(tp,ocDiff['timestamp']):
			if len(ocDiff['timestamp'][abs(ocDiff['timestamp']-tp)<timedelta(minutes=10)])>0:
				actuate.append(oneacs)
				timelist.append(tp)
		return pd.DataFrame({'timestamp':timelist, 'value':actuate})

	def collect_therm_actuate_per_zone(self, forceFlag):
		#if not self.proceedCheck(self.thermrawdb,'actuate_per_zone', forceFlag):
		#	return None

		thermActuateData = dict()
		for zone in self.zonelist:
			actuate = self.bdm.download_dataframe('Temp Occ Sts', 'PresentValue', zone, self.beginTime, self.endTime)
			if len(actuate)<=0:
				print("No temp occ sts at: ", zone)
				acs = self.bdm.download_dataframe('Actual Cooling Setpoint', 'PresentValue', zone, self.beginTime, self.endTime)
				oc = self.bdm.download_dataframe('Occupied Command', 'PresentValue', zone, self.beginTime, self.endTime)
				if len(acs)<=0 or len(oc)<=0:
					continue
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
			if len(rawZoneSetpoint)<=0:
				continue
			genieSetpointData[zone] = rawZoneSetpoint
		self.genierawdb.store('setpoint_per_zone', genieSetpointData)
		
	def wcad_error_filter(self, wcads):
		errorzoneList = list()
		cntList = list()
		cntList.append(0)
		basedate = datetime(2013,10,1,0,0,0,tzinfo=timezone('US/Pacific'))
		for zone, wcad in wcads.iteritems():
			if zone=='3242':
				continue
			if len(wcad)<=0:
				continue
			prevweek = int((wcad['timestamp'][0] -basedate).days/7)
			for idx, row in wcad.iterrows():
				tp = row['timestamp']
				currweek = int((tp-basedate).days/7)
				if currweek !=prevweek:
					cntList.append(1)
				else:
					cntList[len(cntList)-1] += 1
				prevweek = currweek

		avg = np.mean(np.array(cntList))
		std = np.std(np.array(cntList))
		print "wcad's avg and std: ", avg, std

		for zone in self.zonelist:
			if zone == '3242' or not (zone in wcads):
				continue
			wcad = wcads[zone]
			if len(wcad)<=0:
				continue
			prevweek = int((wcad['timestamp'][0]-basedate).days/7)
			wcadPerZoneCnt = 1
			for idx, row in wcad.iterrows():
				tp = row['timestamp']
				currweek = int((tp-basedate).days/7)
				if currweek == prevweek:
					wcadPerZoneCnt += 1
				else:
					wcadPerZoneCnt = 1
				prevweek = currweek
				if wcadPerZoneCnt >= avg+std:
					errorzoneList.append(zone)
					break

		for zone in errorzoneList:
			del wcads[zone]
		return wcads
	
	def collect_therm_wcad_per_zone(self, forceFlag):
		if not self.proceedCheck(self.thermrawdb,'wcad_per_zone', forceFlag) and not self.proceedCheck(self.thermrawdb,'wcad_per_zone_filtered', forceFlag): 
			return None

		template = 'Warm Cool Adjust'
		sensorpoint = 'PresentValue'
		wcadData = dict()
		for zone in self.zonelist:
			print zone
			if zone!='3242':
				continue
			wcad = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			if len(wcad)<=0:
				continue
			wcad = self.diff_list(wcad, False, False, 0.5)
			wcadData[zone] = wcad

		self.thermrawdb.store('wcad_per_zone', wcadData)
		wcadDataFiltered = self.wcad_error_filter(wcadDict)
		self.thermrawdb.store('wcad_per_zone_filtered', wcadDataFiltered)
		
	#TODO: Should be changed to use np semantics
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
			if zone!='3242':
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

		if genieFlag:
			setpointData = db.load('setpoint_per_zone')
		else:
			setpointData = db.load('wcad_per_zone_filtered')

		setpointEnergyData = list()
		for zone in setpointData.keys():
			print zone
			#if zone!='3242':
			#	continue
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
			if not self.proceedCheck(db,'temp_vs_setpnt', forceFlag):
				return None
		else:
			db = self.thermrawdb
			if not self.proceedCheck(db,'temp_vs_wcad', forceFlag):
				return None


		tempdict = list()
		if genieFlag: 
			zoneSetpoints = db.load('setpoint_per_zone')
		else:
			zoneSetpoints = db.load('wcad_per_zone_filtered')
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
		if genieFlag:
			db.store('temp_vs_setpnt', tempdict)
		else:
			db.store('temp_vs_wcad', tempdict)

	def collect_all_data(self, forceFlag):
#		forceFlag = True
		GenieFlag = True
		ThermFlag = False
		print "Start collecting data from BulidingDepot"

		#self.collect_genie_actuate_per_zone(forceFlag)
		print "Finish collecting genie actuation data"

		#self.collect_therm_actuate_per_zone(forceFlag)
		print "Finish collecting thermostat actuation data"
		
		#self.collect_genie_setpoint_per_zone(forceFlag)
		print "Finish collecting genie setpoint data"
		
		#self.collect_therm_wcad_per_zone(forceFlag)
		print "Finish collecting thermostat warm-cool adjust data"

		#self.collect_energy_diff(forceFlag, GenieFlag)
		print "Finish calculating genie energy differnce"

		self.collect_energy_diff(forceFlag, ThermFlag)
		print "Finish calculating thermostat energy difference"

		self.collect_temp_vs_setpnt(forceFlag, GenieFlag)
		print "Finish collecting genie's zone temperature information at each temperature setpoints"

		self.collect_temp_vs_setpnt(forceFlag, ThermFlag)
		print "Finish collecting Thermostat's zone temperature information at warm coold adjust points"
		
		self.collect_occ_samples(forceFlag)
		print "FInish collect6ing samples of OCC to compare calendar"
