import abc
import localdb
reload(localdb)
from localdb import localdb

import shelve
import sys
import requests
import traceback
import re
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict
from pytz import timezone
from pytz import utc
import authdata



class bdmanager:

	timeBDFormat = "%Y-%m-%dT%H:%M:%S+00:00"
	srcUrlBase = authdata.srcUrlBase
	srcUrlOptions = authdata.srcUrlOptions

#
#	def store(self):
#		pass

#	def load(self):
#		pass

	def pst2utc(self, tp):
		pst = timezone('US/Pacific')
		tp = tp.replace(tzinfo=pst)
		tpUTC = utc.normalize(tp)
		return tpUTC

	def utc2pst(self, tp):
		pst = timezone('US/Pacific')
		utctz = timezone("UTC")
		return tp.replace(tzinfo=utctz).astimezone(pst)

	# template (str), sensorpoint type (str), zone number (str), beginTime (datetime), endTime (datetime) -> raw ts data (list of dict)
	def download_raw(self, template, sensorpoint, zone, beginTime, endTime):
		beginTimeUTCstr = datetime.strftime(self.pst2utc(beginTime), self.timeBDFormat)
		endTimeUTCstr = datetime.strftime(self.pst2utc(endTime), self.timeBDFormat)
		
		payload = {'context': '{"room":"' + 'rm-'+ zone + '", "template":"'+template+'"}'}
		resp = requests.get(self.srcUrlBase, params=payload, auth=self.srcUrlOptions, timeout=10)
		#ts = pd.
		if resp.status_code ==200:
			resp = resp.json()
			sensors = resp['sensors']
			if len(sensors)>0:
				try:
					sensor = sensors[0]
					uuid = sensor['uuid']
					url = self.srcUrlBase + '/' + uuid + '/sensorpoints/'+sensorpoint+'/timeseries?start=' + beginTimeUTCstr+ '&stop=' + endTimeUTCstr
					sensorResp = requests.get(url, auth=self.srcUrlOptions, timeout=15)
					sensorResp = sensorResp.json()
					ts = sensorResp['timeseries']
					return ts
				except:
					e = sys.exc_info()[0]
					print( "<p>Error: %s</p>" % e )
					traceback.print_exc()
			else:
				print("No sensor found at "+zone)
		else:
			print("bad request: ", resp)
		
		return list() # return zero-length dummy list if there is no data

	def download_dataframe(self, template, sensorpoint, zone, beginTime, endTime):
		return self.raw2pddf(self.download_raw(template, sensorpoint, zone, beginTime, endTime))
	
	# rawts (list of dict) -> processed ts (pdts)
	# timestamp is datetime
	def raw2pddf(self, rawdata):
		keys = list()
		vals = list()
		for idx, element in enumerate(rawdata):
			onets = element.items()[0]
			timestamp = onets[0]
			timestamp = datetime.strptime(timestamp,self.timeBDFormat)
			timestamp = self.utc2pst(timestamp)
			keys.append(timestamp)
			vals.append(onets[1])
		d = {'timestamp': keys, 'value':vals}
		return pd.DataFrame(d)
