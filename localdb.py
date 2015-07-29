import abc 
import shelve

class localdb:
	__metaclass__ = abc.ABCMeta
	baseDataDir = 'data/'
	dbfilename = 'statistics.shelve'

	def __init__ (self, dbfilename):
		self.dbfilename = dbfilename


#	@abc.abstractmethod
	def store(self, name, data):
		writer = shelve.open(self.baseDataDir+self.dbfilename)
		writer[name] = data
		writer.close()

#	@abc.abstractmethod
	def load(self, name):
		reader = shelve.open(self.baseDataDir+self.dbfilename)
		output = reader[name]
		reader.close()
		return output

	def remove(self, name):
		reader = shelve.open(self.baseDataDir+self.dbfilename)
		del reader[name]
		reader.close()
	
	#@abc.abstractmethod
	#Is this necessary for both of Log and BD?
	#def download(self, zone, template, sensorpoints):
	#	return
