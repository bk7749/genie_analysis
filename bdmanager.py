import abc
import datamanager
reload(datamanager)
from datamanager import datamanager

import shelve
import request
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


	def store(self):
		pass

	def load(self):
		pass

	def pst2utc(self, tp):
		pst = timezone('US/Pacific')
		tp = tp.replace(tzinfo=pst)
		tpUTC = utc.normalize(beginTime)
		return tpUTC

	# template (str), sensorpoints type (str), zone number (str), beginTime (datetime), endTime (datetime) -> ts data (pd.Series)
	def download_raw(self, template, sensorpoints, zone, beginTime, endTime):
		beginTimeUTCstr = datetime.strptime(self.pst2utc(beginTime), self.timeBDFormat)
		endTimeUTCstr = datetime.strptime(self.pst2utc(endTime), self.timeBDFormat)
		
		payload = {'context': '{"room":"' + 'rm-'+ zone + '", "template":"'+template+'"}'}
		resp = requests.get(self.srcUrlBase, params=payload, auth=self.srcUrlOptions, timeout=10)
		#ts = pd.Series()
		if resp.status_code ==200:
			resp = resp.json()
			sensors = resp['sensors']
			if len(sensor)>0:
				pass
			else:
				e= sys.exc_info()[0]
				print( "<p>Error: %s</p>" % e )
				traceback.print_exc()
		else:
			print("bad request: ", resp)
		
		return ts
		
		

datamanager.register(datamanager)

print('Sublcass:', issubclass(bdmanager, datamanager))
print('instance:', isinstance(bdmanager(), datamanager))
