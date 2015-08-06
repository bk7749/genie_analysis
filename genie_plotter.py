import plotter
from localdb import localdb
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors as col
from matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import csv
from datetime import datetime, timedelta

class genie_plotter(plotter):

	genierawdb = None
	thermrawdb = None
	genieprocdb = None
	thermprocdb = None
	zonelist = None
	geniezonelist = None
	figdir = 'fig/'
	

	def __init__(self):
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
		self.genieprocdb = localdb('genieprocessed.shelve')
		self.thermprocdb = localdb('thermprocessed.shelve')
		self.zonelist = self.csv2list('metadata\partial_zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\partial_geniezonelist.csv')
	
#	def save_fig(self, fig, name):
#		pp = PdfPages(self.figdir+name+'.pdf')
#		pp.savefig(fig, bbox_inches='tight')
#		pp.close()
	
	def csv2list(self, filename):
		outputList = list()
		with open(filename, 'r') as fp:
			reader = csv.reader(fp, delimiter=',')
			for row in reader:
				outputList.append(row[0])
		return outputList
	
	def plot_setpnt_dev_assist(self, setpntDev, sortedFlag, xlabel):
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
		
		ylabel = u'Temperature \ndifference ($^\circ$F)'
		fig = plotter.multiple_stacked_bars(np.array(sortedAvgs.values()), figSizeIn=(4,2), xlabel, ylabel, 0)
		return fig

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
		
		plotter.save_fig(fig1, self.figdir+'spt_dev_genie_zone.pdf')
		plotter.save_fig(fig2, self.figdir+'spt_dev_genie_hour.pdf')
		plotter.save_fig(fig3, self.figdir+'spt_dev_genie_month.pdf')
		plotter.save_fig(fig4, self.figdir+'spt_dev_therm_zone.pdf')
		plotter.save_fig(fig5, self.figdir+'spt_dev_therm_hour.pdf')
		plotter.save_fig(fig6, self.figdir+'spt_dev_therm_month.pdf')
	
	def plot_temp_vs_setpnt(self, genieFlag):
		if genieFlag:
			tempDict= self.genierawdb.load('temp_vs_setpnt')
			filename = 'genie_temp_vs_setpnt'
		else:
			tempDict= self.thermrawdb.load('temp_vs_wcad')
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
		xlabels = ['65', '67.5', '70', '72.5', '75', '77.5', '80']
		ylabels = ['-6', '-4','-2','0','2','4','6']
		cbarLabel = "Count (Number)"
		xlabel = u'Zone temperature ($^\circ$F)'
		ylabel = u'Temperature \nsetpoint ($^\circ$F)'
		fig = plotter.plot_colormap(tmap, figSizeIn=(4,2), xlabel, ylabel, cbarLabel, cm.Blues)
		plt.xticks(np.arange(0,31,5), xlabels, fontsize=10)
		plt.yticks(np.arange(0,13,2), ylabels, fontsize=10)

		plotter.save_fig(fig, self.figdir+'therm_temp_vs_setpnt.pdf')
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

		ylabel = u'Energy (Wh)'
		xlabelMonth = 'Time (Month/Year)'
		xlabelZone = 'Zone'
		figMonth = plotter.plot_up_down_bars(energySaveMonth, energyWasteMonth, (4,2), xlabelMonth, ylabel)
		figZone = plotter.plot_up_down_bars(energySaveZone, energyWasteZone, (4,2), xlabelZone, ylabel)

		if genieFlag:
			plotter.save_fig(figMonth, self.figdir+'genie_energy_diff_month.pdf')
			plotter.save_fig(figZone, 'genie_energy_diff_zone.pdf')
		else:
			plotter.save_fig(figMonth, self.figdir+'therm_energy_diff_month.pdf')
			plotter.save_fig(figZone, self.figdir+'therm_energy_diff_zone.pdf')
	
	def plot_calendar_sample(self):
		occSamples = self.genierawdb.load('occ_samples')
		beforeOcc = occSamples['2109'][0]
		afterOcc = occSamples['2109'][1]

		tsList = list()
		tsList.append(beforeOcc['timestamp'])
		tsList.append(afterOcc['timestamp'])
		occList = list()
		occList.append(beforeOcc['value'])
		occList.append(afterOcc['value'])
		xlabel = 'Time'
		ylabel = 'Occupied Command'

		plotter.plot_multiple_timeseries(tsList, occList, xlabel
		fig, axes = plt.subplots(nrows=2)
		axes[0].set_ylim([0.9,3.1])
		axes[1].set_ylim([0.9,3.1])
		plt.show()
		
		plotter.save_fig(fig, self.figdir+'utilization_actu_setpnt.pdf')
		return fig
	
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
		plotter.save_fig(fig, self.figdir+'utilization_actu_setpnt.pdf')
		return fig

	def plot_all(self):
		self.plot_setpnt_dev()
		self.plot_temp_vs_setpnt()
		self.plot_energy_diff()
		self.plot_calendar_sample()
		self.plot_actuate_setpnt_ts()
