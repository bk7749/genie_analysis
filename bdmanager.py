import abc
import datamanager
reload(datamanager)
from datamanager import datamanager

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



class bdmanager(datamanager):

	timeBDFormat = "%Y-%m-%dT%H:%M:%S+00:00"
	srcUrlBase = 'http://bd-datas1.ucsd.edu/admin/api/sensors'
	srcUrlOptions = ('jbkoh@eng.ucsd.edu', '6f9dfee6-3705-4b98-93c2-597716a6dcf0')

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

	# template (str), sensorpoint type (str), zone number (str), beginTime (datetime), endTime (datetime) -> raw ts data (list of dict)
	def download_raw(self, template, sensorpoint, zone, beginTime, endTime):
		beginTimeUTCstr = datetime.strftime(self.pst2utc(beginTime), self.timeBDFormat)
		endTimeUTCstr = datetime.strftime(self.pst2utc(endTime), self.timeBDFormat)
		
		payload = {'context': '{"room":"' + 'rm-'+ zone + '", "template":"'+template+'"}'}
		resp = requests.get(self.srcUrlBase, params=payload, auth=self.srcUrlOptions, timeout=10)
		#ts = pd.Series()
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
				print("No sensor found at "+room)
		else:
			print("bad request: ", resp)
		
		return None

	def download_dataframe(self, template, sensorpoint, zone, beginTime, endTime):
		return self.raw2pdts(self.download_raw(template, sensorpoint, zone, beginTime, endTime))
	
	# rawts (list of dict) -> processed ts (pdts)
	def raw2pddf(self, rawdata):
		keys = list()
		vals = list()
		for idx, element in enumerate(rawdata):
			onets = element.items()[0]
			keys.append(onets[0])
			vals.append(onets[1])
		d = {'timestamp': keys, 'value':vals}
		return pd.DataFrame(d)

			
		

datamanager.register(datamanager)

print('Sublcass:', issubclass(bdmanager, datamanager))
print('instance:', isinstance(bdmanager(), datamanager))
