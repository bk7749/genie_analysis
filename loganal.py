#/usr/bin/python2.7
import re
import csv
from datetime import datetime, timedelta
import requests
import sys
from collections import defaultdict
from pytz import timezone
from pytz import utc
import pandas as pd
import traceback
import json
import numpy as np

from bdmanager import bdmanager
from localdb import localdb

class loganal:
	
	rawfile = None
	logList = None
	roommap = None
	dbconn = None
	zonemap = None
	raw2anonzonemap = dict()
	geniezonelist = list()
	zonelist = list()
	genielogdb = None
	
	confrooms = ['2109','2217','3109','3217','4109','4217']
	samplerooms = ['2150']

	# Result of total counting
	total_temp_controls = 0
	total_actu_controls = 0
	total_login = 0
	total_feedback = 0
	total_feedback_temp_pattern = defaultdict(int)
	total_feedback_season_pattern = defaultdict(int)
	total_actu_types = defaultdict(int)

	# Result of personal counting
	personal_temp_controls = defaultdict(int)
	personal_actu_controls = defaultdict(int)
	personal_login = defaultdict(int)
	personal_feedback = defaultdict(int)
	personal_begintime = defaultdict(int)
	personal_endtime = defaultdict(int)
	personal_active_range = defaultdict(int)
	personal_visit = defaultdict(int)
	personal_satisfy =defaultdict(int)
	personal_schedule = defaultdict(int)
	personal_frequency = defaultdict(list)

	# Result of system counting
	system_temp_controls = defaultdict(int)
	system_actu_controls = defaultdict(int)
	system_feedback = defaultdict(int)
	system_comp_power = list()
#	system_setpoint_dev_zone = dict()
	system_setpoint_dev_zone = defaultdict(list)
	system_setpoint_dev_hour = defaultdict(list)
	system_setpoint_dev_month = defaultdict(list)
	thermostat_setpoint_dev_zone = defaultdict(list)
	thermostat_setpoint_dev_hour = defaultdict(list)
	thermostat_setpoint_dev_month = defaultdict(list)
	system_calendar_energy = defaultdict(list)
	system_temporary_oc = dict()
	system_warmcooladj = dict()

	# samples of log
	sample_oc_confroom_before = None
	sample_oc_confroom_after = None
	sample_oc_normroom_before = None
	sample_oc_normroom_after = None

	# temporal data
	temporal_actu_month = defaultdict(list)
	temporal_temp_month = defaultdict(list)
	temporal_actu_hour_weekend = defaultdict(int)
	temporal_actu_hour_weekday = defaultdict(int)
	temporal_actu_all = dict()

	# Context data
	ctx_temp_setpoint = list()
	ctx_wcad_setpoint = list()

	# Raw data
	raw_temp_control_perzone = dict()

	beginTime = datetime(2013,12,1,2,0,0,0)

	def __init__(self, inputfile):
		self.rawfile = inputfile

		# Read room map
		self.roommap = dict()
		reader = csv.reader(open('metadata/roommap.csv', 'r'), delimiter=',')
		reader.next()
		for row in reader:
			self.roommap[row[0]] = row[1] #TODO: Check if this works

		# Read genie zone map
		self.zonemap = dict()
		reader = csv.reader(open('metadata/zonemap.csv', 'r'), delimiter=',')
		reader.next()
		for row in reader:
			self.zonemap[row[1]] = row[0]
			if not (row[0] in self.raw2anonzonemap):
					self.raw2anonzonemap[row[0]] = row[1]
		self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')

		# Read log_list
		reader = csv.reader(open(self.rawfile, 'r'), delimiter=';')
		#reader = csv.reader(open(self.rawfile, 'r'), delimiter=';')
		self.logList = list()
		keys = reader.next()
		for row in reader:
			log = {}
			for idx, column in enumerate(row):
				log[keys[idx]] = column
			self.logList.append(log)

		self.genielogdb = localdb('genielog.shelve')

	def calc_user_activities_raw(self):
		setpntDict = defaultdict(list)
		actuDict = defaultdict(list)
		for log in self.logList:
			tp = datetime.strptime(log['timestamp'],'%Y-%m-%d %H:%M:%S')
			if tp >= self.beginTime:
				if log['webpage'] == 'control':
					if log['actuator'] == 'Temperature':
						setpntDict[log['username']].append(log)
					elif log['actuator'] == 'Actuate':
						actuDict[log['username']].append(log)
		
		self.genielogdb.store('setpoint_per_user', setpntDict)
		self.genielogdb.store('actuate_per_user', actuDict)

	def calc_user_activities_count(self):
		setpntDict = defaultdict(int)
		actuDict = defaultdict(int)
		# Above should be changed to use normal dict and be initialized by default user list
		for log in self.logList:
			if log['webpage'] == 'control':
				if log['actuator'] == 'Temperature':
					setpntDict[log['username']] += 1
				elif log['actuator'] == 'Actuate':
					actuDict[log['username']] += 1
		
		self.genielogdb.store('setpoint_count_per_user', setpntDict)
		self.genielogdb.store('actuate_count_per_user', actuDict)

	def calc_user_feedback(self):
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

		for log in self.logList:
			if log['webpage']=='feedback':
				username = log['username']
				comfort = float(log['comfort'])
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
		self.genielogdb.store('feedback_avg_per_user', avgDict)
		self.genielogdb.store('feedback_std_per_user', stdDict)
		self.genielogdb.store('feedback_cnt_per_user', cntDict)
		self.genielogdb.store('feedback_pos_avg_per_user', posAvgDict)
		self.genielogdb.store('feedback_pos_std_per_user', posStdDict)
		self.genielogdb.store('feedback_pos_cnt_per_user', posCntDict)
		self.genielogdb.store('feedback_neg_avg_per_user', negAvgDict)
		self.genielogdb.store('feedback_neg_std_per_user', negStdDict)
		self.genielogdb.store('feedback_neg_cnt_per_user', negCntDict)



###################################################################
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList

	def dict2csv(self, filename, my_dict, tag):
		with open(filename, 'wb') as f:
			w = csv.writer(f,delimiter=',')
			w.writerow(tag)
			for key, value in my_dict.items():
				w.writerow([key,value])
	
	def export_result(self):
		basedir = 'results/'
		tags = ['username', 'count']
		self.dict2csv(basedir+'user temperature controls.csv', self.personal_temp_controls, tags)
		self.dict2csv(basedir+'user actuate controls.csv', self.personal_actu_controls, tags)
		self.dict2csv(basedir+'user login.csv', self.personal_login, tags)
		self.dict2csv(basedir+'user feedback.csv', self.personal_feedback, tags)
		self.dict2csv(basedir+'user active range.csv', self.personal_active_range, tags)
		self.dict2csv(basedir+'user satisfy.csv', self.personal_satisfy, tags)

		tags = ['zone', 'count']
		self.dict2csv(basedir+'system temp controls.csv', self.system_temp_controls, tags)
		self.dict2csv(basedir+'system actu controls.csv', self.system_actu_controls, tags)
		self.dict2csv(basedir+'system actu controls.csv', self.system_feedback, tags)

		with open(basedir+'system comp power.csv', 'wb') as fp:
			writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
			writer.writerow(self.system_comp_power)
	
	def counting_total(self):
		for log in self.logList:
			if log['webpage']=='home' or log['webpage']=='login':
				self.total_login += 1
			elif log['webpage'] == 'control':
				if log['actuator'] == 'Temperature':
					self.total_temp_controls += 1
				elif log['actuator'] == 'Actuate':
					self.total_actu_controls += 1
					if log['value1'] == 1:
						self.total_actu_types['1'] += 1
					elif log['value1'] == 3:
						self.total_actu_types['3'] += 1
					elif log['value1'] == 2:
						self.total_actu_types['2'] += 1
			elif log['webpage'] == 'feedback':
				self.total_feedback +=1

	def counting_personal(self):

		# Set active begin time for each pesonnel
		for log in self.logList:
			if not log['username'] in self.personal_begintime:
				self.personal_begintime[log['username']] = log['timestamp']
		# Set active end time for each personnel
		for log in reversed(self.logList):
			if not log['username'] in self.personal_endtime:
				self.personal_endtime[log['username']] = log['timestamp']

		# Set active range for each personnel
		for user, beginTimeStr in self.personal_begintime.iteritems():
			endTimeStr = self.personal_endtime[user]
			beginTime = datetime.strptime(beginTimeStr, self.timeStrFormat)
			endTime = datetime.strptime(endTimeStr, self.timeStrFormat)
			activeRange = endTime - beginTime
			self.personal_active_range[user] = activeRange.total_seconds()

		# Counting webpages per person
		for log in self.logList:
			if log['webpage'] == 'home' or log['webpage']=='login':
				self.personal_login[log['username']] += 1
				self.personal_frequency[log['username']].append(log['timestamp'])
			elif log['webpage'] == 'control':
				if log['actuator']=='Temperature':
					self.personal_temp_controls[log['username']] += 1
				elif log['actuator']=='Actuate':
					self.personal_actu_controls[log['username']] += 1
			elif log['webpage'] =='feedback':
				if log['username'] in self.personal_feedback:
					feedback = self.personal_feedback[log['username']]
				else:
					feedback = defaultdict(int)
					self.personal_feedback[log['username']] = feedback
				if 'value1' in log:
					feedback[log['comfort']] += 1
			elif log['webpage']=='schedule':
				self.personal_schedule[log['username']] += 1

		# Counting total webpages per person
		for log in self.logList:
			self.personal_visit[log['username']] += 1

	def counting_satisfy(self):
		prevLog = dict()
		for log in self.logList:
			if prevLog.has_key('username'):
				if (prevLog['webpage']=='home' or prevLog['webpage']=='login') and prevLog['username']!=log['username']:
					self.personal_satisfy[prevLog['username']] += 1
			prevLog = log
	
	# input: datetime, output: timedelta
	# Deprecated
	def get_utc2pac(timepoint):
		summerBegin2015 = datetime(2015,3,8,0,0,0)
		summerEnd2015 = datetime(2015,11,1,0,0,0)
		summerBegin2014 = datetime(2015,3,9,0,0,0)
		summerEnd2014 = datetime(2014,11,2,0,0,0)
		summerBegin2013 = datetime(2013,3,10,0,0,0)
		summerEnd2013 = datetime(2013,3,3,0,0,0)
		if (timepoint>summerBegin2015 and timepoint<summerEnd2015) or\
			(timepoint>summerBegin2014 and timepoint<summerEnd2014) or\
			(timepoint>summerBegin2013 and timepoint<summerEnd2013):
			UTC2PAC = 7
		else:
			UTC2PAC = 8
		return timedelta(hours=UTC2PACC)
	
	def rawts2pdseries(self, ts):
		keys = list()
		vals = list()
		for idx, element in enumerate(ts):
			onets = element.items()[0]
			keys.append(onets[0])
			vals.append(onets[1])
		return pd.Series(vals, index=keys)


	# ts is pd
	def sum_power(self, ts):
#		ts = self.rawts2pdseries(ts)
		localtimeformat = '%Y-%m-%dT%H:%M:%S+00:00'
		totalTime = 0
		totalEnergy = 0
		for i in range(1, len(ts)):
			t1 = ts.index[i-1]
			t1 = datetime.strptime(t1,localtimeformat)
			t2 = ts.index[i]
			t2 = datetime.strptime(t2,localtimeformat)
			dt = t2-t1
			#v1 = ts[i-1] 		# No use
			v2 = ts[i]
			totalEnergy += v2*dt.total_seconds()
			#totalTime += dt	# No Use

		return totalEnergy

	def compare_energy(self):
		pst = timezone('US/Pacific')

		for log in self.logList:
			if log['webpage'] == 'control':
				timestr = log['timestamp']
				timepoint = datetime.strptime(timestr, self.timeStrFormat)
				timepoint = timepoint.replace(tzinfo=pst)
				beforeTime = timepoint.replace(hour=0,minute=0,second=0)
				if timepoint.weekday()==4 or timepoint.weekday()==6:
					afterTime = beforeTime - timedelta(days=1)
				else:
					afterTime = beforeTime + timedelta(days=1)
				
				beforeTimeStr1 = utc.normalize(beforeTime).strftime(self.timeUrlFormat)
				beforeTimeStr2 = utc.normalize(beforeTime+timedelta(days=1)).strftime(self.timeUrlFormat)
				afterTimeStr1 = utc.normalize(afterTime).strftime(self.timeUrlFormat)
				afterTimeStr2 = utc.normalize(afterTime+timedelta(days=1)).strftime(self.timeUrlFormat)
			
				template = "HVAC Zone Power"
				payload = {'context': '{"room":"' + 'rm-'+ self.zonemap[log['zone']] + '", "template":"'+template+'"}'}
				resp = requests.get(self.srcUrlBase, params=payload, auth=self.srcUrlOptions,timeout=10)
				if resp.status_code==200:
					sensor = resp.json()
					sensor = sensor[u'sensors']
					if len(sensor) > 0:
						try:
							sensor = sensor[0]
							uuid = sensor[u'uuid']
							url = self.srcUrlBase + '/' + uuid + '/sensorpoints/total_zone_power/timeseries?start=' + beforeTimeStr1 + '&stop=' + beforeTimeStr2
							sensorData = requests.get(url, auth=self.srcUrlOptions, timeout=15)
							sensorData = sensorData.json()
							beforeTS = sensorData['timeseries']
							url = self.srcUrlBase + '/' + uuid + '/sensorpoints/total_zone_power/timeseries?start=' + afterTimeStr1 + '&stop=' + afterTimeStr2
							sensorData = requests.get(url, auth=self.srcUrlOptions, timeout=15)
							sensorData = sensorData.json()
							afterTS = sensorData['timeseries']
							beforeTS = self.rawts2pdseries(beforeTS)
							afterTS = self.rawts2pdseries(afterTS)
							beforePower = self.sum_power(beforeTS)
							afterPower = self.sum_power(afterTS)
							self.system_comp_power.append([beforePower, afterPower, log['zone'], log['timestamp']])
						except:
							print resp
							e = sys.exc_info()[0]
							print( "<p>Error: %s</p>" % e )
				else:
					print("Error at " + log['timeseries'])
		
	def counting_system_control(self):
		for log in self.logList:
			if log['webpage']=='control':
				if log['actuator']=='Temperature':
					self.system_temp_controls[log['zone']] += 1
				elif log['actuator']=='Actuate':
					self.system_actu_controls[log['zone']] += 1
			elif log['webpage'] =='feedback':
				self.system_feedback[log['zone']] += 1

#time is datetime in PST here.
	def get_bd_data(self, template, datatype, room, beginTime, endTime):
		pst = timezone('US/Pacific')
		beginTime = beginTime.replace(tzinfo=pst)
		beginTimeUTC = utc.normalize(beginTime)
		endTime = endTime.replace(tzinfo=pst)
		endTimeUTC = utc.normalize(endTime)
		beginTimeStr = beginTimeUTC.strftime(self.timeUrlFormat)
		endTimeStr = endTimeUTC.strftime(self.timeUrlFormat)

		#template = 'Genie HVAC Control'
		payload = {'context': '{"room":"' + 'rm-'+ room + '", "template":"'+template+'"}'}
		resp = requests.get(self.srcUrlBase, params=payload, auth=self.srcUrlOptions,timeout=10)
		pdts = pd.Series()
		if resp.status_code==200:
			sensor = resp.json()
			sensor = sensor[u'sensors']
			if len(sensor) > 0:
				try:
					sensor = sensor[0]
					uuid = sensor[u'uuid']
					url = self.srcUrlBase + '/' + uuid + '/sensorpoints/'+datatype+'/timeseries?start=' + beginTimeStr+ '&stop=' + endTimeStr
					sensorData = requests.get(url, auth=self.srcUrlOptions, timeout=15)
					sensorData = sensorData.json()
					ts = sensorData['timeseries']
					pdts = self.rawts2pdseries(ts)
					print("success at :"+room)
				except:
					e = sys.exc_info()[0]
					print( "<p>Error: %s</p>" % e )
					traceback.print_exc()
					pdts = pd.Series()
			else:
				print("No sensor found at "+room)
		else:
			print("bad request: ", resp)
		return pdts

	def get_temp_point(self, sensorType, zone, timestr, rawzonenameFlag):
		localtimeformat = '%Y-%m-%dT%H:%M:%S+00:00' # For pdts
#		room = self.roommap[room]
		if not rawzonenameFlag:
			zone = self.zonemap[zone]
		
		timepnt = datetime.strptime(timestr, self.timeStrFormat)
		beginTime = timepnt - timedelta(hours=1)
		endTime = timepnt + timedelta(hours=1)
		temp_ts = self.get_bd_data(sensorType, 'PresentValue', zone, beginTime, endTime)

		if len(temp_ts)==0:
			print("No data at zone: "+zone)
			return None
		
		closestTime = datetime(2018,1,1,0,0,0)
		closestTimeStr = str()
		for sampletimestr in temp_ts.index:
			sampletime = datetime.strptime(sampletimestr, localtimeformat)
			timediff = abs(sampletime - timepnt)
			mindiff = abs(closestTime-timepnt)
			if timediff<mindiff:
				closestTime = sampletime	
				closestTimeStr = sampletimestr
		closestTemp = temp_ts[closestTimeStr]
		return closestTemp

#	def stat_per_feedback(self):
#		i=0
#		j=0
#		for log in self.logList:
#			if log['webpage']=='feedback':
#				i +=1
#				print i
#				comfort = log['comfort']
#				if comfort in self.total_feedback_temp_pattern:
#					comfortObj = self.total_feedback_temp_pattern[comfort]
#				else:
#					comfortObj = defaultdict(int)
#					self.total_feedback_temp_pattern[comfort] = comfortObj
#				tempPnt = self.get_temp_point('Zone Temperature', log['zone'], log['timestamp'])
#				if tempPnt == None:
#					print log
#					j+=1
#				comfortObj[tempPnt] += 1
#		print j

	def stat_per_feedback(self):
		localtimeformat = '%Y-%m-%dT%H:%M:%S+00:00' # For pdts
		feedbackfile = 'feedback.csv'
		with open(feedbackfile, 'r') as fp:
			reader = csv.reader(fp)
			for row in reader:
				# Extract temperature pattern
				comfort = int(row[3])
				if comfort in self.total_feedback_temp_pattern:
					comfortObj = self.total_feedback_temp_pattern[comfort]
				else:
					comfortObj = defaultdict(int)
					self.total_feedback_temp_pattern[comfort] = comfortObj
				tp = datetime.strptime(row[0],localtimeformat)
				timestr = datetime.strftime(tp,self.timeStrFormat)
				tempPnt = self.get_temp_point('Zone Temperature', row[2], timestr, True)
				if tempPnt ==None:
					print row
					continue
				comfortObj[tempPnt] += 1

	def stat_temp_setpoint(self):
		for log in self.logList:
			if log['webpage'] == 'control' and log['actuator']=='Temperature':
				currTemp = self.get_temp_point('Zone Temperature', log['zone'], log['timestamp'], False)
				self.ctx_temp_setpoint.append({currTemp:log['value1']})

	#TODO: Test this function
	def stat_wcad_setpoint(self):
		i = 0
		for zone in self.zonelist:
			i +=1
			print i
			if zone in self.geniezonelist or zone=='3221' or zone=='4262':
				continue
			filename_wcad = 'data/saved_wcad_' + zone + '.shelve'
			reader= shelve.open(filename_wcad)
			wcadList = reader['wcad']
			reader.close()
			for tpstr, setpoint in wcadList.iterkv():
				tp = datetime.strptime(tpstr, self.timeBDFormat)
				tpstr = tp.strftime(self.timeStrFormat)
				currTemp = self.get_temp_point('Zone Temperature', zone, tpstr, True)
				self.ctx_wcad_setpoint.append({currTemp:setpoint})

#TODO: Run this again
	def stat_actu_deprecated(self):
		for log in self.logList:
			if log['webpage'] == 'control':
				tp = log['timestamp']
				tp = datetime.strptime(tp, self.timeStrFormat)
				if log['actuator']=='Actuate':
					self.temporal_actu_month[(tp.year-2013)*12 + tp.month].append(tp)
					if tp.weekday()<5:
						self.temporal_actu_hour_weekend[tp.hour] += 1
					else:
						self.temporal_actu_hour_weekday[tp.hour] += 1
				elif log['actuator']=='Temperature':
					self.temporal_temp_month[(tp.year-2013)*12 + tp.month].append(tp)

	def process_offtime(self, offtime):
		newofftime = list()
		for key, value in offtime.iterkv():
			if value != 'None':
				newofftime.append(datetime.strptime(key, self.timeBDFormat))
		return newofftime

	def stat_actu(self):
		template = 'Genie HVAC Control'
		datatype = 'off-time'
		beginTime = datetime(2013,12,1,0,0,0)
		endTime = datetime(2015,6,16,0,0,0)

		for zone in self.geniezonelist:
			offtime = self.get_bd_data(template, datatype, zone, beginTime, endTime)
			refinedOfftime = self.process_offtime(offtime)
			self.temporal_actu_all[zone] = refinedOfftime
			

#TODO: Run this again
	def stat_setpoint_dev(self):
		#for log in self.logList:
		#	if log['webpage']=='control' and log['actuator']=='Temperature':
		#		commonsetpoint = self.get_temp_point('Common Setpoint', log['zone'], log['timestamp'], False)
		#		usersetpoint = log['value1']
		#		if commonsetpoint==None or usersetpoint == None:
		#			print "No setpoint data at: ", log
		#			continue
		#		setdiff = float(usersetpoint) - commonsetpoint
		#		# per Zone
		#		self.system_setpoint_dev_zone[log['zone']].append(setdiff)
		#		# per Hour
		#		tp = datetime.strptime(log['timestamp'], self.timeStrFormat)
		#		self.system_setpoint_dev_hour[tp.hour].append(setdiff)
		#		# per Month
		#		self.system_setpoint_dev_month[(tp.year-2013)*12+tp.month].append(setdiff)

		beginTimeDefault = datetime(2013,10,1,0,0,0)
		endTimeDefault = datetime(2014,10,1,0,0,0)
		template = 'HVAC Zone Information'
		datatype = 'default_common_setpoint'
		for zone in self.geniezonelist:
			if zone=='4223':
				continue
			commonsetpoint = self.get_bd_data(template, datatype, zone, beginTimeDefault, endTimeDefault)
			if len(commonsetpoint)>0:
				commonsetpoint = float(commonsetpoint[0])
			else:
				continue

			if zone in self.raw_temp_control_perzone:
				zoneTempControl = self.raw_temp_control_perzone[zone]
			else:
				continue
			for tpstr, tempControl in zoneTempControl.iterkv():

				setdiff = float(tempControl)-commonsetpoint
				# per Zone
				tp = datetime.strptime(tpstr, self.timeBDFormat)
				self.system_setpoint_dev_zone[zone].append(setdiff)
				# per Hour
				self.system_setpoint_dev_hour[tp.hour].append(setdiff)
				# per Month
				self.system_setpoint_dev_month[(tp.year-2013)*12+tp.month].append(setdiff)

		for zone in self.zonelist:
			if (zone in self.geniezonelist) or zone=='3242':
				continue
			filename_wcad = 'data/saved_wcad_' + zone + '.shelve'
			reader = shelve.open(filename_wcad)
			try:
				wcadSeries = reader['wcad']
			except:
				reader.close()
				continue
			reader.close()
			for tpstr, wcad in wcadSeries.iterkv():
				tp = datetime.strptime(tpstr, self.timeBDFormat)
				tpstr = tp.strftime(self.timeStrFormat)
				wcad = float(wcad)
#				if abs(wcad)>10:
#					continue

				# per zone
				self.thermostat_setpoint_dev_zone[zone].append(wcad)
				# per Hour
				self.thermostat_setpoint_dev_hour[tp.hour].append(wcad)
				# per Month
				self.thermostat_setpoint_dev_month[(tp.year-2013)*12+tp.month].append(wcad)
				


	def stat_login_frequency(self):
		for log in self.logList:
			if log['webpage'] == 'login':
				self.personal_frequency[log['username']].append(log['timestamp'])
				
			
	def comp_calendar(self):
		# Get samples of OCs to show calendar's effect
		confroom = '2109'
		normroom = '2150'
		template = 'Occupied Command'
		datatype = 'PresentValue'
		beginTime1 = datetime(2014,5,12,0,0,0)
		endTime1 = datetime(2014,5,19,0,0,0)
		beginTime2 = datetime(2015,5,11,0,0,0)
		endTime2 = datetime(2015,5,18,0,0,0)
		self.sample_oc_confroom_before = self.get_bd_data(template, datatype, confroom, beginTime1, endTime1)
		self.sample_oc_confroom_after = self.get_bd_data(template, datatype, confroom, beginTime2, endTime2)
		self.sample_oc_normroom_before = self.get_bd_data(template, datatype, normroom, beginTime1, endTime1)
		self.sample_oc_normroom_after = self.get_bd_data(template, datatype, normroom, beginTime2, endTime2)

		# Copmare energy for calendar
		beginTime1 = datetime(2014,4,27,0,0,0)
		endTime1 = datetime(2014,5,25,0,0,0)
		beginTime2 = datetime(2015,4,26,0,0,0)
		endTime2 = datetime(2015,5,24,0,0)
		template = 'HVAC Zone Power'
		datatype = 'total_zone_power'
		for room in self.confrooms+self.samplerooms:
			beforets = self.get_bd_data(template,datatype,room, beginTime1,endTime1)
			afterts = self.get_bd_data(template,datatype,room, beginTime2,endTime2)
			beforeEnergy = self.sum_power(beforets)
			afterEnergy = self.sum_power(afterts)
			self.system_calendar_energy[room].append(beforeEnergy)
			self.system_calendar_energy[room].append(afterEnergy)

	def diff_list(self, data, descendFlag, ascendFlag):
		preVal = data[0]
		diffList = list()
		for tp, value in data.iterkv():
			if descendFlag and value < preVal-3:
				diffList.append(datetime.strptime(tp,self.timeBDFormat))
			elif not descendFlag and ascendFlag and value>preVal:
				diffList.append(datetime.strptime(tp,self.timeBDFormat))
			elif not descendFlag and value !=preVal:
				diffList.append(datetime.strptime(tp,self.timeBDFormat))
			preVal = value

		return diffList

	def diff_series(self, data):
		newseries = pd.Series()
		newkeys = list()
		newvals = list()
		prevVal = data[0]
		for tpstr, value in data.iterkv():
			tp = datetime.strptime(tpstr, self.timeBDFormat)
			if tp.year==2014 and tp.month==12:
				pass
			if abs(value-prevVal)>0.5:
				newkeys.append(tpstr)
				newvals.append(value)
				print tpstr, value, prevVal
			prevVal = value

		return pd.Series(newvals, index=newkeys)

	def find_near_index(self, targettp, timelist):
		for tp in timelist:
			if abs(tp-targettp)<timedelta(minutes=10):
				return True

		return False
	
	def calc_occ(self, acsList, ocList):
		acsDiff = self.diff_list(acsList, True, False)
		ocDiff = self.diff_list(ocList, False, False)
		tempocList = list()
		for acs in acsDiff:
			#if not acs in ocDiff:
			if not self.find_near_index(acs,ocDiff):
				tempocList.append(acs)
		return tempocList
	
	def download_thermostat_vals(self):
		wcadjList = defaultdict(list)
		tempocList = defaultdict(list)
		beginTime = datetime(2013,12,1,0,0,0)
		endTime = datetime(2015,6,16,0,0,0)
		i=0
		for zone in self.zonelist:
			filename_wcad = 'data/saved_wcad_' + zone + '.shelve'
			filename_tempoc = 'data/saved_tempoc_'+zone + '.shelve'
			i += 1
			print i
			if zone=='3221' or zone=='4262':
				continue
			if zone!='2219':
				continue
			wcad = self.diff_series(self.get_bd_data('Warm Cool Adjust', 'PresentValue', zone, beginTime, endTime))
			writer = shelve.open(filename_wcad)
			writer['wcad'] = wcad
			writer.close()
			wcad = None
#			self.system_warmcooladj[zone] = wcad

			tempoc = self.get_bd_data('Temp Occ Sts', 'PresentValue', zone, beginTime, endTime)
			if len(tempoc)<=0:
				print("No temp occ sts at: ", zone)
				tempoc = None
				acsList = self.get_bd_data('Actual Cooling Setpoint', 'PresentValue', zone, beginTime, endTime)
				ocList = self.get_bd_data('Occupied Command', 'PresentValue', zone, beginTime, endTime)
				tempoc = self.calc_occ(acsList, ocList)
			else:
				tempoc = self.diff_list(tempoc, False, True)

			writer = shelve.open(filename_tempoc)
			writer['tempoc'] = tempoc
			writer.close()

	def download_temp_control(self):
		template = 'Genie HVAC Control'
		datatype = 'Temperature'
		beginTime = datetime(2013,12,1,0,0,0)
		endTime = datetime(2015,6,16,0,0,0)
		for zone in self.zonelist:
			self.raw_temp_control_perzone[zone] = self.get_bd_data(template, datatype, zone, beginTime, endTime)
			
