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
import dateutil
from copy import deepcopy
import ast

# Description for each data in db
# GENIE
# 1. actuate_per_zone: entire raw actuate per zone. each actuate is template:"Genie HVAC Control", sensorpoint:"Actuate". This is not actual Actuate because raw data contains redundant data from normal schedule. It will be removed at processor.proc_genie_actu_freq().
# 2. setpoint_per_zone: entire raw setpoint per zone. each setpoint is template:"Genie HVAC Control", sensorpoint:"Temperature"
# 3. setpoint_diff_per_zone: It shows how each setpoint changes from its previous one. This is for analyzing user behavior.
# 4. actuate_energy: energy saving and wastage for each actuate activity
# 5. setpoint_energy: energy saving and wastage for each setpoint activity
# 6. occ_samples: Samples of several zones' occupied command. This is for showing effects of Calendar system
# 7. temp_vs_setpnt: context information of zone temperature for each setpoint activity. Map from current temperature to corresponding setpoints.
# 8. temp_vs_setpnt_diff: context information of zone temperature for each setpoint diff. Map from current temperature to corresponding setpoint diffs.

class collector():
	bdm = None
	zonelist = None
	geniezonelist = None
	genierawdb = None
	thermrawdb = None
	generaldb = None
	beginTime = datetime(2013,10,15,0,0,0)
	endTime = datetime(2015,6,25,22,0,0)
	confrooms = ['2109', '2217', '3109', '3127', '4109', '4217']
	normrooms = ['2150']
	errorZones = ['3242']
	resetTPs = None
	offtimeFormat = '%Y-%m-%dT%H:%M:%S'
	pst = timezone('US/Pacific')

	# Input: tp (datetime), offset (timedelta(hours=7 or 8))
	# Output tp (datetime) but timezone (PST) is added
	# Note: Offset stands for summer time offset (know this is not good)
	# 		To add timezone to datetime. Just adding naive timezone does not work. Should study why this happens
	def add_timezone(self, tp, offset):
		return (tp+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific'))
	def __init__(self):
		self.bdm = bdmanager()
		#self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		#self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
		self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
		self.generaldb = localdb('general.shelve')
		# TODO: Should be change to use a function. remove repetition
		self.resetTPs = [(datetime(2014,5,15,20,46,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,7,11,8,31,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,7,30,11,35,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,5,17,7,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,5,17,14,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,12,15,40,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,13,13,4,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,18,14,3,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,18,14,9,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,8,19,13,20,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,10,12,7,2,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2014,10,31,14,18,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2015,6,29,18,49,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),\
				(datetime(2015,7,13,17,5,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
		#TODO Change following dates
				(datetime(2014,2,23,19,0,0)+timedelta(hours=8)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,6,28,19,0,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,6,29,12,0,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,7,6,12,1,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,7,6,12,30,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,7,6,12,59,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,10,12,19,0,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,11,16,19,0,0)+timedelta(hours=8)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2014,11,16,19,37,0)+timedelta(hours=8)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific')),
				(datetime(2015,7,12,19,1,0)+timedelta(hours=7)).replace(tzinfo=timezone('UTC')).astimezone(timezone('US/Pacific'))]
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList

# if forceFlag is turned on, db deletes data and collect again
# tag means data name inside the db
	def proceed_check(self, db, tag, forceFlag):
		if not forceFlag:
			if db.check(tag):
				return False
			else:
				return True
		else:
			if db.check(tag):
				#db.remove(tag)
				pass
			return True

	#def collect_genie_actuate_per_zone_deprecated(self, forceFlag):
	def collect_genie_offtime_per_zone(self, forceFlag):
		if not self.proceed_check(self.genierawdb,'actuate_per_zone', forceFlag):
			return None
		genieActuateData = dict()
		template = 'Genie HVAC Control'
		sensorpoint = 'off-time'
		for zone in self.geniezonelist:
			print zone
			offtime = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			if len(offtime)<=0:
				newofftime = pd.DataFrame({"timestamp":[],"value":[]})
			else:
				newofftimeTime = list()
				newofftimeVal = list()
				#for i, value in enumerate(offtime['value']):
				for row in offtime.iterrows():
					tp = row[1]['timestamp']
					val = row[1]['value']
					if val != 'None':
						val = val.split('-07:00')[0]
						val = val.split('-08:00')[0]
						val = val.split('.')[0]
						parsedVal = datetime.strptime(val, self.offtimeFormat)
#						parsedVal = dateutil.parser.parse(val).replace(tzinfo=timezone('US/Pacific'))
						#parsedVal = parsedVal.replace(tzinfo=timezone('US/Pacific'))
						parsedVal = self.pst.localize(parsedVal, is_dst=True)
						
						newofftimeTime.append(parsedVal)
						newofftimeVal.append(1)
				newofftime = self.bdm.twolist2pddf(newofftimeTime, newofftimeVal)
			genieActuateData[zone] = newofftime
		self.genierawdb.store('offtime_per_zone', genieActuateData)

	def calc_genie_actuate_deprecated2(self, gacts, starts):
		actuates = gacts
		for row in gacts.iterrows():
			idx = row[0]
			tp = row[1]['timestamp']
			start = datetime.strptime(starts[starts['timestamp']<=tp].tail(1)['value'][0], '%I:%M%p')
			start = start.replace(tzinfo= timezone('US/Pacific'))
			dt = tp.replace(year=1900, month=1, day=1) - start
			if abs(dt) < timedelta(minutes=10):
				actuates = actuates.drop(idx)
		actuates.index = range(0,len(actuates))
		return actuates

	def collect_genie_actuate_per_zone_deprecated2(self, forceFlag):
		if not self.proceed_check(self.genierawdb, 'actuate_per_zone', forceFlag):
			return None
		genieActuateData = dict()
		for zone in self.geniezonelist:
			print zone
			genieActuate = self.bdm.download_dataframe('Genie HVAC Control', 'Actuate', zone, self.beginTime, self.endTime)
			schedStart = self.bdm.download_dataframe('HVAC Zone Control Schedule Weekday', 'start', zone, self.beginTime, self.endTime)
			if len(genieActuate)<=0 or len(schedStart)<=0:
				continue
			genieActuate = genieActuate[genieActuate['value']==3]
			genieActuate.index = range(0,len(genieActuate))
#			schedStart['value'] = datetime.strptime(schedStart['value'], '%I:%M%p')

			actuate = self.calc_genie_actuate_deprecated2(genieActuate, schedStart)
			genieActuateData[zone] = actuate

		self.genierawdb.store('actuate_per_zone', genieActuateData)

	def check_system_reset_timing(self, tp):
		# Those should not be included
		for resetTP in self.resetTPs:
			if abs(tp-resetTP)<timedelta(minutes=10):
				return True
		return False

	def calc_genie_actuate(self, actuate, offtime):
#		offtime = self.genierawdb.load('offtime_per_zone')
#		offtime = offtime[zone]
		oneFlag = False
		threeFlag = False
		dropList = list()
		if len(actuate)==0:
			return actuate
#		actIter = actuate.iterrows()
#		prevDay = actIter.next()[1]['timestamp'].day
		prevDay = 0
		for rows in actuate.iterrows():
			tp = rows[1]['timestamp']
			val = rows[1]['value']
			idx = rows[0]
			#if not tp.day in [5,6] and tp.day == prevDay:
			if self.check_system_reset_timing(tp):
				dropList.append(idx)
			if tp.day!=prevDay:
				oneFlag = False
				threeFlag = False

			if val==1 and (tp.hour<7 or tp.hour>18):
				if len(offtime['timestamp'][abs(offtime['timestamp']-tp)<timedelta(minutes=10)]) > 0:
					dropList.append(idx)
					prevDay = tp.day
					continue
			if val==-1:
				dropList.append(idx)
				continue
				
			if not tp.weekday() in [5,6]:
				if not oneFlag and val==1:
					dropList.append(idx)
					oneFlag = True
				if not threeFlag and val==3:
					dropList.append(idx)
					threeFlag = True
			else:
				if val==1:
					if len(offtime['timestamp'][abs(offtime['timestamp']-tp)<timedelta(minutes=10)]) > 0:
						dropList.append(idx)
			prevDay = tp.day

		newActuate = actuate.drop(dropList)			
		newActuate.index = range(0,len(newActuate))
		return newActuate

	def collect_genie_actuate_per_zone(self, forceFlag):
		offtimes = self.genierawdb.load('offtime_per_zone')
		if not self.proceed_check(self.genierawdb, 'actuate_per_zone', forceFlag):
			return None
		genieActuates = dict()
		for zone in self.geniezonelist:
			print zone
			offtime = offtimes[zone]
			genieActuate = self.bdm.download_dataframe('Genie HVAC Control', 'Actuate', zone, self.beginTime, self.endTime)
			genieActuates[zone] = self.calc_genie_actuate(genieActuate, offtime)

		self.genierawdb.store('actuate_per_zone', genieActuates)
	
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
	
	def find_near_index(self, targettp, timelist):
		fullLen = len(timelist)
		i1 = 0
		i2 = 1
		i3 = fullLen-1
		while i2 < fullLen:
			if abs(timelist[i2]-targettp)<timedelta(minutes=10):
				return i2
			elif targettp > timelist[i2]:
				i1 = i2
				i2 = (i2+i3+1)/2
			elif targettp < timelist[i2]:
				i3 = i2
				i2 = (i1+i2)/2
			
			if i1==i2 or i2==i3:
				if abs(timelist[i2]-targettp)<timedelta(minutes=10):
					return i2
				else:
					return None
		return None

	# Input: targettp (datetime), data (pd.DataFrame)
	def find_closest_left_index(self, targettp, data):
		if len(data)==0:
			return None
		newData = data.iloc[(data['timestamp']<=targettp).values.tolist()]
		if len(newData)==0:
			newData = data.iloc[(data['timestamp']>=targettp).values.tolist()]
			if len(newData)==0:
				return None
			lastRow = newData.head(1)
		else:
			lastRow = newData.tail(1)
		return lastRow.index[0]

	
	# Actual Cooling Setpoint: acs (DataFrame); Occupied Command: oc(DataFrame)
	# -> Actuate (DataFrame)
	def calc_therm_actuate(self, acs, oc):
		acsDiff = self.diff_list(acs, True, False, 3)
		oc = oc[oc['value']!=2]
		oc.index = range(0,len(oc))
		ocDiff = self.diff_list(oc, False, True, 1.5)
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
			if len(ocDiff['timestamp'][abs(ocDiff['timestamp']-tp)<timedelta(minutes=10)])==0:
				actuate.append(oneacs)
				timelist.append(tp)
		return pd.DataFrame({'timestamp':timelist, 'value':actuate})

	def collect_therm_actuate_per_zone(self, forceFlag):
		if not self.proceed_check(self.thermrawdb,'actuate_per_zone', forceFlag):
			return None

		thermActuateData = dict()
		for zone in self.zonelist:
			print zone
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
	
	# Input: dict of setpoint (dict({zone:pd.DataFrame}))
	# Output: average, std
	def calc_avg_std(self, setpntDict):
		zoneCnt = 0 # to exclude zones without any data
		weightedSum = 0.0
		weightedSqSum = 0.0
		localBeginTime = self.add_timezone(self.beginTime,8)
		localEndTime = self.add_timezone(self.endTime,7)

		for setpoint in setpntDict.itervalues():
			if len(setpoint)==0:
				continue
			zoneCnt += 1
			setpntIter = setpoint.iterrows()
			row = setpntIter.next()
			prevTP = row[1]['timestamp']
			prevVal = row[1]['value']
			while prevTP < localBeginTime:
				row = setpntIter.next()
				prevTP = row[1]['timestamp']
				prevVal = row[1]['value']
			dt = (prevTP-localBeginTime).total_seconds()
			weightedSum += dt * prevVal
			weightedSqSum += dt * prevVal * prevVal
			for row in setpntIter:
				tp = row[1]['timestamp']
				val = row[1]['value']
				if tp>=localEndTime:
					break
				dt = (tp-prevTP).total_seconds()
				weightedSum += dt * val
				weightedSqSum += dt * val * val
				prevVal = val
				prevTP = tp
			dt = (localEndTime-prevTP).total_seconds()
			weightedSum += dt * prevVal
			weightedSqSum += dt * prevVal * prevVal

		totalSec = (localEndTime-localBeginTime).total_seconds()
		average = weightedSum / totalSec / zoneCnt
		std = math.sqrt(weightedSqSum / totalSec / zoneCnt - average*average)
		return average, std
	
	def collect_genie_setpoint_per_zone(self, forceFlag):
		if not self.proceed_check(self.genierawdb,'setpoint_per_zone', forceFlag):
			return None
		
		template = 'Genie HVAC Control'
		sensorpoint = 'Temperature'
		genieSetpointData = dict()
		genieSetpointDiffData = dict()
		for zone in self.geniezonelist:
			print zone
			rawZoneSetpoint = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			if len(rawZoneSetpoint)<=0:
				continue
			genieSetpointData[zone] = rawZoneSetpoint
			setpointDiff = pd.DataFrame({'timestamp':rawZoneSetpoint['timestamp'][1:], 'value':np.diff(rawZoneSetpoint['value'], axis=0)})
			setpointDiff = setpointDiff.drop(setpointDiff[setpointDiff['value']==0.0].index)
			setpointDiff.index = range(0,len(setpointDiff))
			genieSetpointDiffData[zone] = setpointDiff

		avg, std = self.calc_avg_std(genieSetpointData)
		print avg, std
		self.generaldb.store('genie_setpnt_dev_avg', avg)
		self.generaldb.store('genie_setpnt_dev_std', std)
		self.genierawdb.store('setpoint_per_zone', genieSetpointData)
		self.genierawdb.store('setpoint_diff_per_zone', genieSetpointDiffData)
		
	def wcad_error_filter(self, wcads):
		errorZoneList = list()
		cntList = list()
		cntList.append(0)
		basedate = datetime(2013,10,1,0,0,0,tzinfo=timezone('US/Pacific'))
		for zone, wcad in wcads.iteritems():
			if zone in self.errorZones:
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
#		print "wcad's avg and std: ", avg, std

		for zone in self.zonelist:
			if zone in self.errorZones or not (zone in wcads):
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
				if wcadPerZoneCnt >= avg+2*std:
					errorZoneList.append(zone)
					break

		for zone in errorZoneList:
			del wcads[zone]
		return wcads, errorZoneList

	def wcad_to_setpnt(self, wcadDict):
		setpntDict = dict()
		for zone, wcads in wcadDict.iteritems():
			print zone
			if len(wcads)<=0:
				continue
			commonSetpnts = self.bdm.download_dataframe('Common Setpoint', 'PresentValue', zone, self.beginTime, self.endTime)
			if len(commonSetpnts)<=0:
				continue
			setpnts = list()
			for row in wcads.iterrows():
				tp = row[1]['timestamp']
				wcad = row[1]['value']
				nearIdx = self.find_closest_left_index(tp,commonSetpnts)
				if nearIdx != None:
					setpnts.append(wcad+commonSetpnts['value'][nearIdx])
				else:
					print tp
					print nearIdx
			setpnt = pd.DataFrame({"timestamp":wcads['timestamp'], "value":setpnts})
			setpntDict[zone] = setpnt
		return setpntDict
					
	def collect_therm_wcad_per_zone(self, forceFlag):
		if not self.proceed_check(self.thermrawdb,'wcad_per_zone', forceFlag) and not self.proceed_check(self.thermrawdb,'wcad_per_zone_filtered', forceFlag): 
			return None

		template = 'Warm Cool Adjust'
		sensorpoint = 'PresentValue'
		wcadData = dict()
		entireWcadData = dict()
		for zone in self.zonelist:
			print zone
			if zone in self.errorZones:
				continue
			wcad = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			entireWcadData[zone] = wcad
			if len(wcad)<=0:
				continue
			relativeThreshold = (max(wcad['value'])-min(wcad['value']))/10
			if relativeThreshold<=0.2:
				relativeThreshold = 0.2
#			wcad = self.diff_list(wcad, False, False, 0.3)
			wcad = self.diff_list(wcad, False, False, relativeThreshold)
			for row in wcad.iterrows():
				tp = row[1]['timestamp']
				if tp>=datetime(2014,6,30,3,0,0,tzinfo=self.pst) and tp <=datetime(2014,6,30,9,0,0,0,tzinfo=self.pst):
					wcad = wcad.drop(row[0])
			wcad.index = range(0,len(wcad))
			#wcad = self.diff_list(wcad, False, False, 0.5)
			wcadData[zone] = wcad

		self.thermrawdb.store('wcad_per_zone', wcadData)
		wcadDataFiltered, errorZoneList = self.wcad_error_filter(wcadData)
		self.thermrawdb.store('wcad_error_zone_list', errorZoneList)
		entireWcadDataFiltered = {zone: entireWcadData[zone] for zone in list(set(entireWcadData.keys())-set(errorZoneList))}
		
		wcadAvg, wcadStd = self.calc_avg_std(entireWcadDataFiltered)
		print wcadAvg, wcadStd
		self.generaldb.store('therm_wcad_dev_avg', wcadAvg)
		self.generaldb.store('therm_wcad_dev_std', wcadStd)
		
		wcadDiffFiltered = dict()
		for zone, wcad in wcadDataFiltered.iteritems():
			wcadDiffFiltered[zone] = pd.DataFrame({'timestamp':wcad['timestamp'][1:], 'value':np.diff(wcad['value'], axis=0)})
		
		self.thermrawdb.store('wcad_per_zone_filtered', wcadDataFiltered)
		self.thermrawdb.store('wcad_diff_per_zone', wcadDiffFiltered)

#		entireSetpntData = self.wcad_to_setpnt(entireWcadDataFiltered)
		setpntAvg, setpntStd = self.calc_avg_std(wcadData)
		print setpntAvg, setpntStd
		self.generaldb.store('therm_setpoint_dev_avg', setpntAvg)
		self.generaldb.store('therm_setpoint_dev_std', setpntStd)

		setpntData = self.wcad_to_setpnt(wcadDataFiltered)
		self.thermrawdb.store('estimated_setpoint_per_zone', setpntData)

			
		
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

	def calc_energy_diff_deprecated(self, beforeTime, zone):
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


# Input: tp (datetime: indicating when the actuation happens), zone (string)
# Output: (list: containing energy difference information, which will be added to a list)
# Note: 1) each prev and post ranges are time1~time2 and time3~time4 to skip weekends.
#       2) tp should not be a weekend
	def calc_energy_diff(self, tp, zone, diffTime):
		template = 'HVAC Zone Power'
		sensorpoint = 'total_zone_power'

		if diffTime!=timedelta(days=1) and ((tp.weekday==0 and tp.hour<diffTime.total_seconds()/3600) or (tp.weekday==4 and tp.hour>24-diffTime.total_seconds()/3600)):
			return None

		#prevTime1 = tp - timedelta(days=1)
		prevTime1 = tp - diffTime
		prevTime2 = tp
		prevTime3 = tp
		prevTime4 = tp
		postTime1 = tp
		#postTime2 = tp+timedelta(days=1)
		postTime2 = tp+diffTime
		postTime3 = postTime2
		postTime4 = postTime2 
		if tp.weekday()==0 and diffTime==timedelta(days=1):
			prevTime1 = tp - timedelta(days=3)
			prevTime2 = (tp-timedelta(days=2)).replace(hour=0,minute=0,second=0)
			prevTime3 = tp.replace(hour=0,minute=0,second=0)
			prevTime4 = tp
		elif tp.weekday()==4 and diffTime==timedelta(days=1):
			postTime1 = tp
			postTime2 = (tp+timedelta(days=1)).replace(hour=0,minute=0,second=0)
			postTime3 = postTime2 + timedelta(days=2)
			postTime4 = tp+timedelta(days=3)
		prevPower1 = self.bdm.download_dataframe(template, sensorpoint, zone, prevTime1, prevTime2)
		prevPower2 = self.bdm.download_dataframe(template, sensorpoint, zone, prevTime3, prevTime4)
		postPower1 = self.bdm.download_dataframe(template, sensorpoint, zone, postTime1, postTime2)
		postPower2 = self.bdm.download_dataframe(template, sensorpoint, zone, postTime3, postTime4)
		prevEnergy = self.calc_energy(prevPower1) + self.calc_energy(prevPower2)
		postEnergy = self.calc_energy(postPower1) + self.calc_energy(postPower2)
		return [zone, tp, prevEnergy, postEnergy]


# Store data structure: list of list. each list contains zone, timestamp, beforeEnergy, afterEnergy
	def collect_energy_diff(self, forceFlag, genieFlag, timeDiff):
		if genieFlag:
			db = self.genierawdb
		else:
			db = self.thermrawdb

		if not self.proceed_check(db,'actuate_energy', forceFlag) and not self.proceed_check(db, 'setpoint_energy', forceFlag):
			return None
		
		actuateData = db.load('actuate_per_zone')
		actuateEnergyData = list()
		for zone in actuateData.keys():
			print zone
			if zone in self.errorZones:
				continue
			for tp in actuateData[zone]['timestamp']:
				if tp.weekday()==5 or tp.weekday()==6:
					continue
				energyDiff = self.calc_energy_diff(tp, zone, timeDiff)
				if energyDiff:
					actuateEnergyData.append(energyDiff)
		db.store('actuate_energy_'+str(timeDiff.total_seconds()/3600), actuateEnergyData)

		if genieFlag:
			setpointData = db.load('setpoint_per_zone')
		else:
			setpointData = db.load('wcad_per_zone_filtered')

		setpointEnergyData = list()
		for zone in setpointData.keys():
			print zone
			if zone in self.errorZones:
				continue
			for tp in setpointData[zone]['timestamp']:
				if tp.weekday()==5 or tp.weekday()==6:
					continue
				energyDiff = self.calc_energy_diff(tp, zone, timeDiff)
				if energyDiff:
					setpointEnergyData.append(energyDiff)
		db.store('setpoint_energy_'+str(timeDiff.total_seconds()/3600), setpointEnergyData)
		
	def collect_occ_samples(self, forceFlag):
		if not self.proceed_check(self.genierawdb, 'occ_samples', forceFlag):
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

	#return current temperature or flow near tp)
	# sensorType should be one of 'Zone Temperature' and 'Supply Air Flow'
	def get_temp_flow_point(self, zone, tp, sensorType):
		beginTime = tp - timedelta(minutes=10)
		endTime = tp + timedelta(minutes=10)
		tempData = self.bdm.download_dataframe(sensorType, 'PresentValue', zone, beginTime, endTime)
		if len(tempData)<=0:
			return None
		else:
			middleIndex = len(tempData)/2
			return tempData['value'][middleIndex]

	def collect_temp_vs_setpnt(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genierawdb
			if not self.proceed_check(db,'temp_vs_setpnt', forceFlag):
				return None
		else:
			db = self.thermrawdb
			if not self.proceed_check(db,'temp_vs_wcad', forceFlag):
				return None


		tempDiffDict = list()
		tempDict = list()
		if genieFlag: 
			zoneSetpoints = db.load('setpoint_per_zone')
			zoneSetpntDiffs = db.load('setpoint_diff_per_zone')
		else:
			zoneSetpoints = db.load('wcad_per_zone_filtered')
			zoneSetpntDiffs = db.load('wcad_diff_per_zone')
		for zone, zoneSetpoint in zoneSetpoints.iteritems():
			print zone
			for row in zoneSetpoint.iterrows():
				tp = row[1]['timestamp']
				setpoint = row[1]['value']
				currTemp = self.get_temp_flow_point(zone, tp, 'Zone Temperature') # TODO: Implement this
				if currTemp == None:
					continue
				tempDict.append({currTemp:setpoint})
		for zone, zoneSetpntDiff in zoneSetpntDiffs.iteritems():
			print zone
			for row in zoneSetpntDiff.iterrows():
				tp = row[1]['timestamp']
				setpntDiff = row[1]['value']
				currTemp = self.get_temp_flow_point(zone, tp, 'Zone Temperature')
				if currTemp == None:
					continue
				tempDiffDict.append({currTemp:setpntDiff})

		if genieFlag:
			db.store('temp_vs_setpnt', tempDict)
			db.store('temp_vs_setpnt_diff', tempDiffDict)
		else:
			db.store('temp_vs_wcad', tempDict)
			db.store('temp_vs_wcad_diff', tempDiffDict)
	
	def collect_area(self, forceFlag):
		template = "HVAC Zone Information"
		dataType = "area"
		beginTime = datetime(2013,7,1,0,0,0)
		endTime = datetime(2013,7,31,0,0,0)
		areas = dict()
		for zone in self.zonelist:
			print zone
			ts = self.bdm.download_dataframe(template, dataType, zone, beginTime, endTime)
			areas[zone] = float(ts['value'][0])

		self.generaldb.store('area', areas)

	def collect_power(self, forceFlag):
		if not self.proceed_check(self.generaldb, 'zone_energy_per_month', forceFlag):
			return None

		defaultZoneEnergy = dict()
		for i in range(10,31): # dec/2013 is 12, jul/2015 is 31
			defaultZoneEnergy[i] = 0

		zoneEnergies = dict()
		zoneWeekdayEnergies = dict()
		zoneWeekendEnergies = dict()

		for zone in self.zonelist:
			print zone
			zoneEnergy = deepcopy(defaultZoneEnergy)
			zoneWeekendEnergy = deepcopy(defaultZoneEnergy)
			zoneWeekdayEnergy = deepcopy(defaultZoneEnergy)
			zonePower = self.bdm.download_dataframe('HVAC Zone Power', 'total_zone_power', zone, self.beginTime, self.endTime)
			if len(zonePower)<=0:
				zoneEnergies[zone] = zoneEnergy
				continue
			zonePowerIter = zonePower.iterrows()
			prevTP = zonePowerIter.next()[1]['timestamp']
			for row in zonePower.iterrows():
				tp = row[1]['timestamp']
				val = row[1]['value']
				monthCnt = (tp.year-2013)*12 + tp.month
				if monthCnt == 31:
					pass
				zoneEnergy[monthCnt] += (tp-prevTP).total_seconds()*val
				if tp.weekday()<=4 and tp.hour>=7 and tp.hour<=18:
					if prevTP.hour<7 or prevTP.hour>18:
						prevTP = tp.replace(hour=7,minute=0,second=0)
					zoneWeekdayEnergy[monthCnt] += (tp-prevTP).total_seconds()*val
				elif tp.weekday()>=5 or tp.hour>19 or tp.hour<6:
					if prevTP.hour>6 and prevTP.hour<19 and prevTP.weekday()<5:
						prevTP = prevTP.replace(hour=19, minute=0, second=0)
					zoneWeekendEnergy[monthCnt] += (tp-prevTP).total_seconds()*val
				prevTP = tp
			zoneEnergies[zone] = zoneEnergy
			zoneWeekendEnergies[zone] = zoneWeekendEnergy
			zoneWeekdayEnergies[zone] = zoneWeekdayEnergy

		self.generaldb.store('zone_energy_per_month', zoneEnergies)
		self.generaldb.store('zone_weekend_energy_per_month', zoneWeekendEnergies)
		self.generaldb.store('zone_weekday_energy_per_month', zoneWeekdayEnergies)
		
	def collect_genie_actuated_hours(self, forceFlag):
		genieActuateData = dict()
		template = 'Genie HVAC Control'
		sensorpoint = 'off-time'
		
		actuHoursDict = dict()
		defaultActuHours = dict()
		for i in range(10,31):
			defaultActuHours[i] = 0.0

		for zone in self.geniezonelist:
			print zone
			offtimes = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			actuHours = deepcopy(defaultActuHours)
			if len(offtimes)>0:
				actuate = self.genierawdb.load('actuate_per_zone')[zone]
				for row in offtimes.iterrows():
					tp = row[1]['timestamp']
					val = row[1]['value']
#					if not (tp.year==2013 and tp.hour==20 and tp.minute==20 and tp.second==58):
#						continue
					if val != 'None':
						val = val.split('-07:00')[0]
						val = val.split('-08:00')[0]
						val = val.split('.')[0]
						offTime = datetime.strptime(val, self.offtimeFormat)
						offTime = self.pst.localize(offTime, is_dst=True)
						if len(actuate)>0:
							print tp
							filteredActuIdx = np.logical_and(np.logical_and(actuate['timestamp']<=offTime, actuate['timestamp']>tp), actuate['value']==1)
							newActu = actuate.iloc[filteredActuIdx.values.tolist()]
							if len(newActu)>0:
								offTime = newActu.tail(1)['timestamp'].values[0]

						hours = (offTime-tp).total_seconds()/3600
						actuHours[(tp.year-2013)*12+tp.month] += hours
			actuHoursDict[zone] = actuHours

		self.genierawdb.store('actuated_hours_per_zone', actuHoursDict)
	
	def collect_flow_temp_vs_setpoint_zone(self, forceFlag, genieFlag, zone):
		if genieFlag:
			db = self.genierawdb
			if not self.proceed_check(db,'flow_vs_setpnt', forceFlag):
				return None
		else:
			db = self.thermrawdb
			if not self.proceed_check(db,'flow_vs_wcad', forceFlag):
				return None

		tempDiffDict = list()
		tempDict = list()
		flowDiffDict = list()
		flowDict = list()
		if genieFlag: 
			zoneSetpoints = db.load('setpoint_per_zone')
			zoneSetpntDiffs = db.load('setpoint_diff_per_zone')
		else:
			zoneSetpoints = db.load('wcad_per_zone_filtered')
			zoneSetpntDiffs = db.load('wcad_diff_per_zone')

		zoneSetpoint = zoneSetpoints[zone]
		for row in zoneSetpoint.iterrows():
			tp = row[1]['timestamp']
			setpoint = row[1]['value']
			currFlow = self.get_temp_flow_point(zone, tp, 'Actual Supply Flow') # TODO: Implement this
			currTemp = self.get_temp_flow_point(zone, tp, 'Zone Temperature') # TODO: Implement this
			if currTemp==None or currFlow==None:
				continue
			tempDict.append({currTemp:setpoint})
			flowDict.append({currFlow:setpoint})

		zoneSetpointDiff = zoneSetpntDiffs[zone]
		for row in zoneSetpointDiff.iterrows():
			tp = row[1]['timestamp']
			setpoint = row[1]['value']
			currFlow = self.get_temp_flow_point(zone, tp, 'Actual Supply Flow') # TODO: Implement this
			currTemp = self.get_temp_flow_point(zone, tp, 'Zone Temperature') # TODO: Implement this
			if currTemp==None or currFlow==None:
				continue
			tempDiffDict.append({currTemp:setpoint})
			flowDiffDict.append({currFlow:setpoint})

		if genieFlag:
			db.store('flow_vs_setpnt_'+zone, flowDict)
			db.store('flow_vs_setpnt_diff_'+zone, flowDiffDict)
			db.store('temp_vs_setpnt_'+zone, tempDict)
			db.store('temp_vs_setpnt_diff_'+zone, tempDiffDict)
		else:
			db.store('flow_vs_wcad_'+zone, flowDict)
			db.store('flow_vs_wcad_diff_'+zone, flowDiffDict)
			db.store('temp_vs_wcad_'+zone, tempDict)
			db.store('temp_vs_wcad_diff_'+zone, tempDiffDict)
	
	def collect_flow_vs_setpoint(self, forceFlag, genieFlag):
		if genieFlag:
			db = self.genierawdb
			if not self.proceed_check(db,'flow_vs_setpnt', forceFlag):
				return None
		else:
			db = self.thermrawdb
			if not self.proceed_check(db,'flow_vs_wcad', forceFlag):
				return None

		flowDiffDict = list()
		flowDict = list()
		if genieFlag: 
			zoneSetpoints = db.load('setpoint_per_zone')
			zoneSetpntDiffs = db.load('setpoint_diff_per_zone')
		else:
			zoneSetpoints = db.load('wcad_per_zone_filtered')
			zoneSetpntDiffs = db.load('wcad_diff_per_zone')
		for zone, zoneSetpoint in zoneSetpoints.iteritems():
			print zone
			for row in zoneSetpoint.iterrows():
				tp = row[1]['timestamp']
				setpoint = row[1]['value']
				currtemp = self.get_temp_flow_point(zone, tp, 'Actual Supply Flow') # TODO: Implement this
				if currtemp == None:
					continue
				flowDict.append({currtemp:setpoint})
		for zone, zoneSetpntDiff in zoneSetpntDiffs.iteritems():
			print zone
			for row in zoneSetpntDiff.iterrows():
				tp = row[1]['timestamp']
				setpntDiff = row[1]['value']
				currtemp = self.get_temp_flow_point(zone, tp, 'Actual Supply Flow')
				if currtemp == None:
					continue
				flowDiffDict.append({currtemp:setpntDiff})

		if genieFlag:
			db.store('flow_vs_setpnt', flowDict)
			db.store('flow_vs_setpnt_diff', flowDiffDict)
		else:
			db.store('flow_vs_wcad', flowDict)
			db.store('flow_vs_wcad_diff', flowDiffDict)

	def collect_genie_zone_activity(self):
		genieActivityBegin = dict()
		for zone in self.geniezonelist:
			actuBegin = self.bdm.download_begintime('Genie HVAC Control', 'Actuate', zone, self.beginTime, self.endTime)
			if actuBegin:
				genieActivityBegin[zone] = datetime.strptime(actuBegin, '%Y-%m-%dT%H:%M:%S+00:00') - timedelta(hours=7)		
			else:
				genieActivityBegin[zone] = datetime(2015,8,1,0,0,0)
		self.genierawdb.store('genie_activity_begin', genieActivityBegin)

	def collect_temp_occ(self):
		tempOccDict = dict()
		for zone in self.zonelist:
			print zone
			tempOcc = self.bdm.download_dataframe("Temp Occ Sts", 'PresentValue', zone, self.beginTime, self.endTime)
			if len(tempOcc)>0:
				tempOccDiff = self.diff_list(tempOcc, False, True, 0.5)
				tempOccDict[zone] = tempOccDiff
		self.thermrawdb.store('temp_occ_sts', tempOccDict)
	
	def collect_temp_occ_while_oc(self):
		tempOccDict = self.thermrawdb.load('temp_occ_sts')
		redundantTempOccDict = dict()
		for zone in self.zonelist:
			if zone in tempOccDict.keys():
				oc = self.bdm.download_dataframe('Occupied Command', 'PresentValue', zone, self.beginTime, self.endTime)
				tempOcc = tempOccDict[zone]
				redundantTempOccList = list()
				for row in tempOcc.iterrows():
					tp = row[1]['timestamp']
					ocIdx = self.find_closest_left_index(tp, oc)
					if oc['value'][ocIdx]==3:
						redundantTempOccList.append(tp)
				redundantTempOccDict[zone] = redundantTempOccList
		self.thermrawdb.store('redundant_temp_occ_sts', redundantTempOccDict)
	
	def collect_user_feedback(self):
		feedbackDict = dict()
		userDict = dict()
		for zone in self.geniezonelist:
			userDict[zone] = self.bdm.download_sensorpoints(zone)

		self.genierawdb.store('user_id_list', userDict)

		for zone, userlist in userDict.iteritems():
			for user in userlist:
				feedback = self.bdm.download_dataframe('Occupant Sensation', user, zone, self.beginTime, self.endTime)
				g = lambda val:int(ast.literal_eval(val)['feeling'])
				try:
					feedback['value'] = feedback['value'].apply(g)
					feedbackDict[user] = feedback
				except:
					pass

		self.genierawdb.store('user_feedback', feedbackDict)

	
	def collect_all_data(self, forceFlag):
#		forceFlag = True
		GenieFlag = True
		ThermFlag = False
		print "Start collecting data from BulidingDepot"
		#self.collect_genie_offtime_per_zone(forceFlag)

		#self.collect_genie_actuate_per_zone(forceFlag)
		print "Finish collecting genie actuation data"

		#self.collect_therm_actuate_per_zone(forceFlag)
		print "Finish collecting thermostat actuation data"
		
		#self.collect_genie_setpoint_per_zone(forceFlag)
		print "Finish collecting genie setpoint data"
		
		#self.collect_therm_wcad_per_zone(forceFlag)
		print "Finish collecting thermostat warm-cool adjust data"

		#self.collect_energy_diff(forceFlag, GenieFlag, timedelta(days=1))
		print "Finish calculating genie energy differnce"

		#self.collect_energy_diff(forceFlag, ThermFlag, timedelta(days=1))
		print "Finish calculating thermostat energy difference"

		#self.collect_power(forceFlag)
		
		#self.collect_occ_samples(forceFlag)
		print "FInish collect6ing samples of OCC to compare calendar"

#TODO Debug this function
		#self.collect_genie_actuated_hours(forceFlag)

		#self.collect_temp_vs_setpnt(forceFlag, GenieFlag)
		#self.collect_temp_vs_setpnt(forceFlag, ThermFlag)
		#self.collect_flow_vs_setpoint(forceFlag, GenieFlag)
		#self.collect_flow_vs_setpoint(forceFlag, ThermFlag)
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, GenieFlag, '2140')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, GenieFlag, '2150')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, GenieFlag, '2272')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, GenieFlag, '3202')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, ThermFlag, '2146')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, ThermFlag, '4114')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, ThermFlag, '3140')
		#self.collect_flow_temp_vs_setpoint_zone(forceFlag, ThermFlag, '4150')

		#self.collect_genie_zone_activity()
		#self.collect_energy_diff(forceFlag, GenieFlag, timedelta(hours=2))
		#self.collect_energy_diff(forceFlag, ThermFlag, timedelta(hours=2))
		#self.collect_temp_occ()
		#self.collect_temp_occ_while_oc()
