import plotter
reload(plotter)
from localdb import localdb
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.gridspec as gridspec
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
	userZoneDict = None
	

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
	
	def init_user_zone_dict(self):
		pass
	
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

	def plot_comp_users(self):
		fig, (axis1, axis2, axis3) = plt.subplots(3,1)
		ylim=(0,15.5)
		title1='Short-term User\'s Activity'
		title2='Sporadic User\'s Activity'
		title3='Consistent User\'s Activity'
		title1 = None
		title2 = None
		title3 = None
		ytickRange = np.arange(0,16,5)
		ytickTag = ['0', '5', '10','15']
		self.plot_an_user_activity('user007', figGiven=fig, axis=axis1,ylim=ylim, xtickFlag=False, title=title1, ytickRange=ytickRange, ytickTag=ytickTag)
		self.plot_an_user_activity('user028', figGiven=fig, axis=axis2, ylim=ylim, xtickFlag=False, title=title2, legends=None, ytickRange=ytickRange, ytickTag=ytickTag)
		self.plot_an_user_activity('user013', figGiven=fig, axis=axis3, ylim=ylim, title=title3, legends=None, ytickRange=ytickRange, ytickTag=ytickTag)
		#fig.subplots_adjust(hspace=0.5)
		#axis1.text(1, 10, 'Short-term User')
		#axis2.text(1, 10, 'Sporadic User')
		#axis3.text(1, 10, 'Consistent User')
		axis1.text(1, 11, 'Short-term User', bbox=dict(fill=False, color='black'), size=7)
		axis2.text(1, 11, 'Sporadic User', bbox=dict(fill=False, color='black'), size=7)
		axis3.text(1, 11, 'Consistent User', bbox=dict(fill=False, color='black'), size=7)
		fig.set_size_inches((4,3))
		plotter.save_fig(fig, self.figdir+'users/compare_user_activities' + '.pdf')

	def plot_an_user_activity(self, username, figGiven=None, axis=None, ylim=(0,60), xtickFlag=True, title=None, legends=['Setpoints', 'Actuation'], ytickTag=None, ytickRange=None):
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
#		legends = ['Setpoints', 'Actuates']
		ylabel = '# Activity'
		if xtickFlag:
			xtickLabel = plotter.make_month_tag(datetime(2013,12,1))
			xlabel = 'Time (Month/Year)'
		else:
			xtickLabel = ['','','','','','','','','','','','','','','','','','','']
			xlabel = None


		if axis==None and figGiven==None:
			fig, axis = plt.subplots(1,1)
		else:
			fig = figGiven
		clist = ['skyblue','darkolivegreen']
#		hatch = ['//','']
		hatch = None
		#ylim = (0,60)
		figsize = self.figsize

		plotter.plot_multiple_stacked_bars(plotData, 0, xlabel=xlabel, ylabel=ylabel, axis=axis, dataLabels=legends, linewidth=linewidth, clist=clist, xtickTag=xtickLabel, ylim=ylim, hatchSeries=hatch, title=title, ytickTag=ytickTag, ytickRange=ytickRange)
		greyRect = patches.Rectangle((6.5,0), 5,70, alpha=0.1)
		axis.add_patch(greyRect)
		axis.text(6.7, ylim[1]/2, 'Data Unavailable', fontsize=6)

		if figGiven==None:
			fig.set_size_inches(figsize)
			fig.tight_layout()
			plotter.save_fig(fig, self.figdir+'users/genie_user_activities_' + username + '.pdf')

	def plot_user_feedback(self):
		avgDict = self.genielogdb.load('feedback_avg_per_user')
		stdDict = self.genielogdb.load('feedback_std_per_user')
		cntDict = self.genielogdb.load('feedback_cnt_per_user')

		avgDict = OrderedDict(sorted(avgDict.items(),key=operator.itemgetter(1)))
		stdList = list()
		cntList = list()
		for key, value in avgDict.iteritems():
			stdList.append(stdDict[key])
			cntList.append(cntDict[key])

		plotData = [avgDict.values()]
		yerrData = [stdList]
		
		fig, [axis1, axis2] = plt.subplots(2,1)
		clist = ['skyblue']
		xtickLabel = None
		ylim = None
		hatch = None
		title = None
		ytickTag = None
		ytickRange = None
		ylabel1 = 'average of feedback'
		ylabel2 = '# of feedback'
		xlabel = 'user'

		plotter.plot_multiple_stacked_bars(plotData, 0, xlabel=xlabel, ylabel=ylabel1, axis=axis1, clist=clist, xtickTag=xtickLabel, ylim=(-3.5,3.5), hatchSeries=hatch, title=title, ytickTag=ytickTag, ytickRange=ytickRange, stdSeries=yerrData)
		plotter.plot_multiple_stacked_bars([cntList], 0, xlabel=xlabel, ylabel=ylabel2, axis=axis2, clist=clist, xtickTag=xtickLabel, ylim=ylim, hatchSeries=hatch, title=title, ytickTag=ytickTag, ytickRange=ytickRange)
			
	#	fig.set_size_inches(figsize)
		fig.tight_layout()
		plotter.save_fig(fig, self.figdir+'users/genie_user_feedback.pdf')

	
	def plot_user_feedback_updown(self):
		posAvgDict = self.genielogdb.load('feedback_pos_avg_per_user')
		posStdDict = self.genielogdb.load('feedback_pos_std_per_user')
		cntDict = self.genielogdb.load('feedback_cnt_per_user')
		negAvgDict = self.genielogdb.load('feedback_neg_avg_per_user')
		negStdDict = self.genielogdb.load('feedback_neg_std_per_user')

		posAvgDict = OrderedDict(sorted(posAvgDict.items(),key=operator.itemgetter(1), reverse=True))
		negAvgDict = OrderedDict(sorted(negAvgDict.items(),key=operator.itemgetter(1), reverse=True))
		posStdList = list()
		negStdList = list()
		negAvgList = list()
		cntList = list()
		for key, value in posAvgDict.iteritems():
			posStdList.append(posStdDict[key])
			if key in negStdDict.keys():
				negStdList.append(negStdDict[key])
			else:
				negStdList.append(0)
			if key in negAvgDict.keys():
				negAvgList.append(negAvgDict[key])
			else:
				negAvgList.append(0)
			cntList.append(cntDict[key])

		for key, value in negAvgDict.iteritems():
			if not key in posAvgDict.keys():
				posStdList.append(0)
				posAvgDict[key] = 0
				negStdList.append(negStdDict[key])
				negAvgList.append(negAvgDict[key])
				cntList.append(cntDict[key])

		#fig, [axis1, axis2] = plt.subplots(2,1, sharex=True)
		#fig.set_size_inches((4,3))
		fig = plt.figure(figsize=(4,2))
		gs = gridspec.GridSpec(2,1,height_ratios=[3,1])
		axis1 = plt.subplot(gs[0])
		axis2 = plt.subplot(gs[1])
		clist = ['skyblue', 'darkolivegreen']
		eclist = ['darkblue', 'darkgreen']
		xtickLabel = None
		ylim = (-3.5,3.5)
		hatch = None
		title = None
		ytickTag = ['0','5','10','15']
		ytickRange = [0,5,10,15]
		ylabel1 = 'Average \nFeedback'
		ylabel2 = '# Feedback'
		xlabel = 'Genie User'
		legend = ['+ Feedback', '$-$ Feedback']
		linewidth = 0.1
		blockwidth = 0.8

		plotter.plot_up_down_bars(posAvgDict.values(), negAvgList, axis=axis1, upColor=clist[0],downColor=clist[1], upStd=posStdList, downStd=negStdList, xlabel=None, ylabel=ylabel1, dataLabels=legend, upEColor=eclist[0], downEColor=eclist[1], linewidth=linewidth, blockwidth=blockwidth, ylim=ylim)
		plotter.plot_multiple_stacked_bars([cntList], 0, xlabel=xlabel, ylabel=ylabel2, axis=axis2, clist=clist, xtickTag=xtickLabel, hatchSeries=hatch, title=title, ytickTag=ytickTag, ytickRange=ytickRange)
			
		fig.tight_layout()
		fig.subplots_adjust(hspace=0.05)
		plotter.save_fig(fig, self.figdir+'users/genie_user_feedback_updown.pdf')




	def plot_all(self):
		self.plot_user_feedback()
		self.plot_user_activities()
		self.plot_user_feedback_updown()
		#self.plot_an_user_activity('user053')
		#self.plot_an_user_activity('user050')
		#self.plot_an_user_activity('user001')
		#self.plot_an_user_activity('user056')
		#self.plot_an_user_activity('user043')
		#self.plot_an_user_activity('user004')
		#self.plot_an_user_activity('user077')
		#self.plot_an_user_activity('user064')
		#self.plot_an_user_activity('user053')
		#self.plot_an_user_activity('user017')
		#self.plot_an_user_activity('user006')
		#self.plot_an_user_activity('user039')
		#self.plot_an_user_activity('user003')
		#self.plot_an_user_activity('user065')
		self.plot_comp_users()
