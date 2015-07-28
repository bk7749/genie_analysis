import abc 

class datamanager:
	__metaclass__ = abc.ABCMeta

	@abc.abstractmethod
	def store(self, name, data):
		pass

	@abc.abstractmethod
	def load(self, name):
		return
	
	@abc.abstractmethod
	#Is this necessary for both of Log and BD?
	def download(self, zone, template, sensorpoints):
		return

