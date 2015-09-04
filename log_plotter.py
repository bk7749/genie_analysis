import plotter
reload(plotter)
from localdb import localdb
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.patches as patches
import matplotlib.colors as col
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np
import csv
from datetime import datetime, timedelta
from collections import OrderedDict, defaultdict
import operator
import pandas as pd


class log_plotter:

	genierawdb = None
	thermrawdb = None
	genieprocdb = None
	thermprocdb = None
	generaldb = None
	zonelist = None
	geniezonelist = None
	notgenielist = list()
	figdir = 'figs/'
	figsize = (4,2)
	

	def __init__(self):
		self.genierawdb = localdb('genieraws.shelve')
		self.thermrawdb = localdb('thermraws.shelve')
		self.genieprocdb = localdb('genieprocessed.shelve')
		self.thermprocdb = localdb('thermprocessed.shelve')
		self.generaldb = localdb('general.shelve')
		self.genielogdb = localdb('genielog.shelve')
		self.zonelist = self.csv2list('metadata\zonelist.csv')
		self.geniezonelist = self.csv2list('metadata\geniezonelist.csv')
		for zone in self.zonelist:
			if zone not in self.geniezonelist:
				self.notgenielist.append(zone)
	
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

	def plot_user_activities(self):
		setpoints = self.genielogdb.load('setpoint_count_per_user')
		actuates = self.genielogdb.load('actuate_count_per_user')

		sortedActuList = list()
		sortedSetpnts = OrderedDict(sorted(setpoints.items(), key=operator.itemgetter(1)))
		for user in sortedSetpnts.keys():
			if user in actuates.keys():
				sortedActuList.append(actuates[user])
			else:
				sortedActuList.append(0)

		plotData = [sortedSetpnts.values(), sortedActuList]
		linewidth=0
		legends = ['Setpoints', 'Actuates']
		xlabel = 'User'
		ylabel = '# Activity'

		fig, axis = plt.subplots(1,1)
		clist = ['skyblue','peachpuff']
		hatch = ['xx', 'oo']

#		legendLoc = 'upper left'
		legendLoc = (0.25,0.7)
		plotter.plot_multiple_stacked_bars(plotData, 0, xlabel=xlabel, ylabel=ylabel, axis=axis, dataLabels=legends, linewidth=linewidth, clist=clist, legendLoc=legendLoc, hatchSeries=hatch)
		figsize = self.figsize
		fig.set_size_inches(figsize)
		plotter.save_fig(fig, self.figdir+'genie_user_activities.pdf')

	def plot_an_user_activity(self, username):
		setpoint = self.genielogdb.load('setpoint_per_user')[username]
		actuate = self.genielogdb.load('actuate_per_user')[username]

		setpntMonth = dict()
		actuMonth = dict()
		for month in range(12,31):
			setpntMonth[month] = 0
			actuMonth[month] = 0

		for log in setpoint:
			tp = datetime.strptime(log['timestamp'],'%Y-%m-%d %H:%M:%S')
			setpntMonth[(tp.year-2013)*12+tp.month] += 1
		for log in actuate:
			tp = datetime.strptime(log['timestamp'],'%Y-%m-%d %H:%M:%S')
			actuMonth[(tp.year-2013)*12+tp.month] += 1

		plotData = [setpntMonth.values(), actuMonth.values()]
		maxY = max([max(setpntMonth.values()), max(actuMonth.values())])
		linewidth=None
		legends = ['Setpoints', 'Actuates']
		xlabel = 'Time (Month/Year)'
		ylabel = '# Activity'
		xtickLabel = plotter.make_month_tag()

		fig, axis = plt.subplots(1,1)
		clist = ['skyblue','peachpuff']
		hatch = ['xx','oo']
		ylim = (0,60)
		figsize = self.figsize

		plotter.plot_multiple_stacked_bars(plotData, 0, xlabel=xlabel, ylabel=ylabel, axis=axis, dataLabels=legends, linewidth=linewidth, clist=clist, xtickTag=xtickLabel, ylim=ylim, hatchSeries=hatch)
		greyRect = patches.Rectangle((6.5,0), 5,70, alpha=0.1)
		axis.add_patch(greyRect)
		axis.text(7, ylim[1]/2, 'Lost Data')
		fig.set_size_inches(figsize)
		fig.tight_layout()
		plotter.save_fig(fig, self.figdir+'users/genie_user_activities_' + username + '.pdf')


	def plot_all(self):
		self.plot_user_activities()
		self.plot_an_user_activity('user053')
		self.plot_an_user_activity('user050')
		self.plot_an_user_activity('user001')
		self.plot_an_user_activity('user056')
		self.plot_an_user_activity('user043')
		self.plot_an_user_activity('user004')
		self.plot_an_user_activity('user077')
		self.plot_an_user_activity('user064')
		self.plot_an_user_activity('user053')
		self.plot_an_user_activity('user017')
		self.plot_an_user_activity('user006')
		self.plot_an_user_activity('user039')
		self.plot_an_user_activity('user003')
		self.plot_an_user_activity('user065')
