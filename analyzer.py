import bdmanager
reload(bdmanager)
from bdmanager import bdmanager
from localdb import localdb

import sys
import traceback
from collections import defaultdict, OrderedDict
import pandas as pd
import csv
from datetime import datetime, timedelta
import numpy as np
import operator
import matplotlib
import matplotlib.colors as col
import matplotlib.cm as cm
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

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
	figdir = 'figs/'
	
	confrooms = ['2109','2217','3109','3217','4109','4217']
	normrooms = ['2150']
	
	def __init__ (self):
		self.bdm = bdmanager()
		self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
		#self.zonelist = self.csv2list('metadata\zonelist.csv')
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

	def collect_genie_actuate_per_zone_deprecated(self, forceFlag):
		if not self.proceedCheck(self.genierawdb,'actuate_per_zone', forceFlag):
			return None

		template = 'Genie HVAC Control'
		sensorpoint = 'Actuate'
		genieActuateData = dict()
		for zone in self.geniezonelist:
			print zone
			rawzone = self.bdm.download_dataframe(template, sensorpoint, zone, self.beginTime, self.endTime)
			if len(rawzone)<=0:
				continue
			processedzone = self.diff_list(rawzone, False, True, 0.1)
			genieActuateData[zone] = rawzone
		self.genierawdb.store('actuate_per_zone', genieActuateData)

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
			prevDatum = datum
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
		
	def collect_therm_wcad_per_zone(self, forceFlag):
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
		self.thermrawdb.store('wcad_per_zone', wcadData)
		
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
			if zone!='3127':
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
			setpointData = db.load('wcad_per_zone')

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
		if genieFlag: 
			zoneSetpoints = db.load('setpoint_per_zone')
		else:
			zoneSetpoints = db.load('wcad_per_zone')
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
		#self.collect_occ_samples(True)
		print("2 Complete")

#	3-1) Setpoint per zone temperature
		#self.collect_genie_setpoint_per_zone(True)
		print("3-1 Complete")
#	3-2) Warm-Cool Adjust per zone temperature
		#self.collect_therm_wcad_per_zone(True)
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
			afterEnergy = row[2]
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

	

# Processing Wrapper: process stored raw data for grap
# Necessary processing:
#	1) 	setpoint deviation 
#	2)	classify energy

	def process_all_data(self):
		ForceFlag = True
		GenieFlag = True
		ThermFlag = False
		#1-1
		self.proc_genie_setdev(True)
		#1-2
		self.proc_therm_setdev(True)

		#2-1
		self.proc_energy(True, GenieFlag)
		#2-2
		self.proc_energy(True, ThermFlag)

	def plot_setpnt_dev_assist(self, setpntDev, sortedFlag):
		avgs = dict()
		stds = dict()
		for key in setpntDev.iterkeys():
			avgs[key] = np.mean(setpntDev[key])
			stds[key] = np.std(setpntDev[key])

		sortedAvgs = dict(zip(setpntDev.keys(), avgs.values()))
		sortedStds = list()
		if sortedFlag:
			sortedAvgs = OrderedDict(sorted(sortedAvgs.items(), key=operator.itemgetter(1)))
			for key in sortedAvgs.iterkeys():
				sortedStds.append(stds[key])
		else:
			for key in sortedAvgs.iterkeys():
				sortedStds.append(stds[key])
		
		fig = plt.figure(figsize=(4,2))
		plt.bar(range(0,len(sortedAvgs)), sortedAvgs.values(), yerr=sortedStds)
		plt.ylabel(u'Temperature \ndifference ($^\circ$F)', labelpad=-2)
		plt.tight_layout()
		plt.show()
		return fig

	def save_fig(self, fig, name):
		pp = PdfPages(self.figdir+name+'.pdf')
		pp.savefig(fig, bbox_inches='tight')
		pp.close()

	def plot_setpnt_dev(self):
		genie_setpnt_dev_zone = self.genieprocdb.load('setpoint_dev_zone')
		genie_setpnt_dev_hour = self.genieprocdb.load('setpoint_dev_hour')
		genie_setpnt_dev_month = self.genieprocdb.load('setpoint_dev_month')
		therm_setpnt_dev_zone = self.thermprocdb.load('setpoint_dev_zone')
		therm_setpnt_dev_hour = self.thermprocdb.load('setpoint_dev_hour')
		therm_setpnt_dev_month = self.thermprocdb.load('setpoint_dev_month')
	
		fig1 = self.plot_setpnt_dev_assist(genie_setpnt_dev_zone, True)
		fig2 = self.plot_setpnt_dev_assist(genie_setpnt_dev_hour, False)
		fig3 = self.plot_setpnt_dev_assist(genie_setpnt_dev_month, False)
		fig4 = self.plot_setpnt_dev_assist(therm_setpnt_dev_zone, True)
		fig5 = self.plot_setpnt_dev_assist(therm_setpnt_dev_hour, False)
		fig6 = self.plot_setpnt_dev_assist(therm_setpnt_dev_month, False)
		
		self.save_fig(fig1, 'spt_dev_genie_zone')
		self.save_fig(fig2, 'spt_dev_genie_hour')
		self.save_fig(fig3, 'spt_dev_genie_month')
		self.save_fig(fig4, 'spt_dev_therm_zone')
		self.save_fig(fig5, 'spt_dev_therm_hour')
		self.save_fig(fig6, 'spt_dev_therm_month')
	
	def plot_temp_vs_setpnt(self, genieFlag):
		if genieFlag:
			tempDict= self.genierawdb.load('temp_vs_setpnt')
			filename = 'genie_temp_vs_setpnt'
		else:
			tempDict= self.thermrawdb.load('temp_vs_wcad')
			filename = 'therm_temp_vs_setpnt'
		xmin = 65
		xmax = 80
		ymin = -6
		ymax = 6
		xgran = 0.5
		ygran = 0.5
		xnum = int((xmax-xmin)/xgran)
		ynum = int((ymax-ymin)/ygran)
		tmap = np.ndarray([ynum,xnum], offset=0)
		#init tmap
		for i in range(0,xnum):
			for j in range(0, ynum):
				tmap[j,i] = 0

		# Calc tmap
		prevSetpnt = np.float64(tempDict[0].values()[0])
		for tempObj in tempDict:
			currTemp = tempObj.keys()[0]
			if currTemp > xmax:
				continue
			elif currTemp < xmin:
				continue
			x = int((xmax-currTemp)/xgran)-1
			setpnt = np.float64(tempObj.values()[0])
			setpntDiff = setpnt - prevSetpnt
			if setpntDiff > ymax:
					continue
			elif setpntDiff < ymin:
				continue
			y = int((ymax-setpntDiff)/ygran) - 1
			tmap[x,y] += 1
			prevSetpnt = setpnt

		# Actual Plotting
		fig = plt.figure(figsize=(4,2))
		plt.pcolor(tmap, cmap=cm.Blues)
		xlabels = ['65', '67.5', '70', '72.5', '75', '77.5', '80']
		ylabels = ['-6', '-4','-2','0','2','4','6']
		plt.xticks(np.arange(0,31,5), xlabels, fontsize=10)
		plt.yticks(np.arange(0,13,2), ylabels, fontsize=10)
		cbar = plt.colorbar()
		cbar.set_label("Count (Number)", labelpad=-0.1)
		plt.xlabel(u'Zone temperature ($^\circ$F)', labelpad=-1.5)
		plt.ylabel(u'Temperature \nsetpoint ($^\circ$F)', labelpad=-2)
		plt.tight_layout()
		plt.show()
			
		self.save_fig(fig, filename)
		return fig
	
	def plot_energy_diff_assist(self, energySave, energyWaste):
		x = range(0,len(energySave))
		fig = plt.figure(figsize=(4,2))
		plt.bar(x, energySave.values())
		plt.bar(x, -np.array(energyWaste.values()), top=0)
		plt.ylabel(u'Energy (Wh)', labelpad=-2)
		plt.tight_layout()
		plt.show()
		return fig

	def plot_energy_diff(self, genieFlag):
		if genieFlag:
			db = self.genieprocdb
		else:
			db = self.thermprocdb
		energySaveMonth= db.load('energy_save_month')
		energySaveZone= db.load('energy_save_zone')
		energyWasteMonth= db.load('energy_waste_month')
		energyWasteZone= db.load('energy_waste_zone')

		figMonth = self.plot_energy_diff_assist(energySaveMonth, energyWasteMonth)
		figZone = self.plot_energy_diff_assist(energySAveZone, energyWasteZone)

		if genieFlag:
			self.save_fig(figMonth, 'genie_energy_diff_month')
			self.save_fig(figZone, 'genie_energy_diff_zone')
		else:
			self.save_fig(figMonth, 'therm_energy_diff_month')
			self.save_fig(figZone, 'therm_energy_diff_zone')

	def plot_calendar_sample(self):
		occSamples = self.genierawdb.load('occ_samples')
		beforeOcc = occSamples['2109'][0]
		afterOcc = occSamples['2109'][1]

		fig, axes = plt.subplots(nrows=2)
		axes[0].set_ylim([0.9,3.1])
		axes[1].set_ylim([0.9,3.1])
		axes[0].plot_date(beforeOcc['timestamp'], beforeOcc['value'], linestyle='-',marker='None')
		axes[1].plot_date(afterOcc['timestamp'], afterOcc['value'], linestyle='-',marker='None')
		plt.show()
		return fig

	def plot_actuate_setpnt_ts_assist(self, genieFlag):
		if genieFlag:
			setpntDict = self.genierawdb.load('setpoint_per_zone')
			actuateDict = self.genierawdb.load('actuate_per_zone')
		else:
			setpntDict = self.thermrawdb.load('wcad_per_zone')
			actuateDict = self.thermrawdb.load('actuate_per_zone')

		setpntTS = defaultdict(float)
		actuateTS = defaultdict(float)

		for setpnts in setpntDict.itervalues():
			for setpnt in setpnts.iterrows():
				setpnt = setpnt[1]
				tp = setpnt['timestamp']
				setpntTS[(tp.year-2013)*12+tp.month] += 1
		for actuates in actuateDict.itervalues():
			for actuate in actuates.iterrows():
				actuate = actuate[1]
				tp = setpnt['timestamp']
				actuateTS[(tp.year-2013)*12+tp.month] += 1

		return setpntTS, actuateTS

		
	def plot_actuate_setpnt_ts(self):
		GenieFlag = True
		ThermFlag = False
		genieSetpnt, genieActuate = self.plot_actuate_setpnt_ts_assist(GenieFlag)
		thermSetpnt, thermActuate = self.plot_actuate_setpnt_ts_assist(ThermFlag)

		x =np.arange(0,len(genieSetpnt))

		fig = plt.figure(figsize=(4,2))
		p1 = plt.bar(x-0.1, np.array(genieSetpnt.values()), width=0.2, align='center')
		p2 = plt.bar(x-0.1, np.array(genieSetpnt.values()), bottom=np.array(genieSetpnt.values()), width=0.2, align='center')
		p3 = plt.bar(x-0.1, np.array(thermSetpnt.values()), width=0.2, align='center')
		p4 = plt.bar(x-0.1, np.array(thermSetpnt.values()), bottom=np.array(thermSetpnt.values()), width=0.2, align='center')

		plt.show()
		self.save_fig(fig, 'utilization_actu_setpnt')
		return fig



# Plotting Wrapper
# 	1)	setpoint_dev
#	2) 	Zone Temperature vs Setpoint Diff
#	3)	Energy Diff over Zone, Hour, Month
#	4)	Calendar sample
#	5)	Actuate and temperature over Time(month)
#	6)	

	def plot_all_data(self):
		GenieFlag = True
		ThermFlag = True
# 1)
		#self.plot_setpnt_dev()
# 2)
		#self.plot_temp_vs_setpnt(GenieFlag)
		#self.plot_temp_vs_setpnt(ThermFlag) #TODO: All of them are positive
# 3)
		#self.plot_energy_diff(GenieFlag)
		#self.plot_energy_diff(ThermFlag)
# 4)
		self.plot_calendar_sample()
# 5)
		self.plot_actuate_setpnt_ts()

