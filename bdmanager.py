import abc
import datamanager
reload(datamanager)
from datamanager import datamanager

import shelve
import request
import re
from datetime import datetime, timedelta



class bdmanager(datamanager):
	def store(self):
		pass

	def load(self):
		pass

	def pst2utc(self, tp):
		pst = timezone('US/Pacific')
		tp = tp.replace(tzinfo=pst)
		tpUTC = utc.normalize(beginTime)
		return tpUTC

	# time is datetime
	def download(self, template, sensorpoints, zone, beginTime, endTime):
		pst = timezone('US/Pacific')
		beginTimeUTC = self.pst2utc(beginTime)
		endTimeUTC = self.pst2utc(endTime)

datamanager.register(datamanager)

print('Sublcass:', issubclass(bdmanager, datamanager))
print('instance:', isinstance(bdmanager(), datamanager))
