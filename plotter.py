import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import operator
import matplotlib
import matplotlib.colors as col
import matplotlib.cm as cm
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

#class plotter:
# dataSeries (2-dimensional np.ndarray), figSize (tuple, length=2) -> fig
# each row of dataSeries is one data type.
# Other details should be implemented in inheritor

# NOTE
# Variable details:
# tickRanges: np.arange, contains tick numbers
# tickTags: list of tick name, contains tick names


def save_fig(fig, name):
	pp = PdfPages(name)
	pp.savefig(fig, bbox_inches='tight')
	pp.close()


################################################### dataSeries (2-dimensional np.ndarray), figSize (tuple, length=2) -> fig
# dataSeries (list of np.ndarray), figSize (tuple, length=2) -> fig
# stackNum starts from 0, which means no stack but just a bar.
# each row of dataSeries is one data type.
# number of stackNum indicates the dats to be stacked.
# e.g., if length of dataSeries is 6, and stack Num is 2,
# dataSeries[0] and dataSereis[1] should be stacked on same bar
# dataSeries[1] and dataSereis[2] should be stacked on same bar
# dataSeries[3] and dataSereis[4] should be stacked on same bar
# Other details should be implemented in inheritor
def plot_multiple_stacked_bars(dataSeries, stackNum, xlabel=None, ylabel=None, xtickRange=None, xtickTag=None, ytickRange=None, ytickTag=None, title=None, stdSeries=None, axis=None, fig=None, clist=None, dataLabels=None, ylim=None, linewidth=None):
	barNum = len(dataSeries)/(stackNum+1)
	totalBlockWidth = 0.8
	#oneBlockWidth = float(0.5/float(barNum))
	oneBlockWidth = float(0.8/float(barNum))
	x = np.arange(0,len(dataSeries[0]))
	if axis==None:
		#axis = plt.gca()
		fig, axis = plt.subplots(1,1)
	bars = list()
	colorIdx = 0
	dataLabelIdx = 0
	for barIdx in range(0,barNum):
		xpos = x-totalBlockWidth/2.0 + oneBlockWidth*barIdx + oneBlockWidth/2.0
		if stdSeries:
			std = stdSeries[barIdx*(stackNum+1)]
		else:
			std = None
		if clist:
			color = clist[colorIdx]
			colorIdx += 1
		else:
			color = None
		if dataLabels:
			dataLabel = dataLabels[dataLabelIdx]
			dataLabelIdx += 1
		else:
			dataLabel = None
		bars.append(axis.bar(xpos, dataSeries[barIdx*(stackNum+1)], yerr=std, width = oneBlockWidth, align='center', color=color, label=dataLabel, linewidth=linewidth))
		#plt.bar(xpos, dataSeries[barIdx*stackNum], yerr=std, width = oneBlockWidth, align='center')
		offset = dataSeries[barIdx]
		for stackIdx in range(1,stackNum+1):
			if stdSeries:
				std = stdSeries[barIdx*(stackNum+1)]
			else:
				std = None
			if clist:
				color = clist[colorIdx]
				colorIdx += 1 
			else:
				color = None
			if dataLabels:
				dataLabel = dataLabels[dataLabelIdx]
				dataLabelIdx += 1
			else:
				dataLabel = None
			bars.append(axis.bar(xpos, dataSeries[barIdx*(stackNum+1)+stackIdx], yerr=std, width=oneBlockWidth, bottom=offset, align='center', color=color, label=dataLabel, linewidth=linewidth))
			#plt.bar(xpos, dataSeries[barIdx*stackNum+stackIdx], yerr=std, width=oneBlockWidth, bottom=offset, align='center')
			offset += dataSeries[barIdx*(stackNum+1)+stackIdx]
	
	#plt.xlim(x[0]-1,x[len(x)-1]+1)
	axis.set_xlim(x[0]-1,x[len(x)-1]+1)
	if ylim:
		axis.set_ylim(ylim)
	if ylabel:
	#	plt.ylabel(ylabel, labelpad=-2)
		axis.set_ylabel(ylabel, labelpad=-2)
	if xlabel:
	#	plt.xlabel(xlabel, labelpad=-2)
		axis.set_xlabel(xlabel, labelpad=-2)
	
	if dataLabels: 
		axis.legend(handles=bars, fontsize=7, loc='best')
	if xtickTag:
		if not xtickRange:
			xtickRange = np.arange(0,len(xtickTag))
		#plt.xticks(xtickRange, xtickTag, fontsize=10, rotation=70)
		axis.set_xticks(xtickRange)
		axis.set_xticklabels(xtickTag, fontsize=10, rotation=70)
	if ytickTag:
		if not ytickRange:
			ytickRange = np.arange(0,len(ytickRag))
		#plt.yticks(ytickRange, ytickTag, fontsize=10)
		axis.set_yticks(ytickRange, ytickTag, fontsize=10)
	if title:
		#plt.title(title)
		axis.set_title(title, y=1.08)
	return fig

def plot_up_down_bars(upData, downData, figSizeIn, xlabel, ylabel, title=None):
	fig = plt.figure(figsize = figSizeIn)
	barNum = len(upData)
	if barNum != len(downData):
		print "data length mismatch"
		return None
	blockWidth = 0.5
	x = np.arange(0,barNum)
	plt.bar(x,upData)
	plt.bar(x,-downData)
	plt.ylabel(ylabel, labelpad=-2)
	plt.xlabel(xlabel, labelpad=-2)
	plt.tight_layout()
	if title:
		plt.title(title)
	plt.show()
	return fig

def plot_colormap(data, figSizeIn, xlabel, ylabel, cbarlabel, cmapIn, ytickRange, ytickTag, xtickRange=None, xtickTag=None, title=None):
	fig = plt.figure(figsize = figSizeIn)
	plt.pcolor(data, cmap=cmapIn)
	cbar = plt.colorbar()
	cbar.set_label(cbarlabel, labelpad=-0.1)
	plt.xlabel(xlabel)
	plt.ylabel(ylabel)
	if xtickTag:
		plt.xticks(xtickRange, xtickTag, fontsize=10)

	plt.yticks(ytickRange, ytickTag, fontsize=10)
	plt.tight_layout()
	if title:
		plt.title(title)
	plt.show()
	return fig

def plot_colormap_upgrade(data, figSizeIn, xlabel, ylabel, cbarlabel, cmapIn, ytickRange, ytickTag, xtickRange=None, xtickTag=None, title=None, xmin=None, xmax=None, xgran=None, ymin=None, ymax=None, ygran=None):
	if xmin:
		y, x = np.mgrid[slice(ymin, ymax + ygran, ygran),
				slice(xmin, xmax + xgran, xgran)]
		fig = plt.figure(figsize = figSizeIn)
#		plt.pcolor(data, cmap=cmapIn)
		plt.pcolormesh(x, y, data, cmap=cmapIn)
		plt.grid(which='major',axis='both')
		plt.axis([x.min(), x.max(), y.min(), y.max()])
	else:
		plt.pcolor(data, cmap=cmapIn)

	cbar = plt.colorbar()
	cbar.set_label(cbarlabel, labelpad=-0.1)
	plt.xlabel(xlabel)
	plt.ylabel(ylabel)
#	if xtickTag:
#		plt.xticks(xtickRange, xtickTag, fontsize=10)
#
#	plt.yticks(ytickRange, ytickTag, fontsize=10)
	plt.tight_layout()
	if title:
		plt.title(title)
	plt.show()
	return fig

# x (list of np.array(datetime)), y (list of np.array(number)) -> fig 
def plot_multiple_timeseries(xs, ys, xlabel, ylabel, xticks=None, xtickTags=None, yticks=None, ytickTags=None, titles=None):
	dataNum = len(ys)
	fig, axes = plt.subplots(dataNum)
	for i, axis in enumerate(axes):
		#plt.xticks(rotation='70')
		axis.plot_date(xs[i], ys[i], linestyle='-', marker='None')
		axis.set_xlabel(xlabel)
		axis.set_ylabel(ylabel)
		axis.set_ylim([0.9,3.1])
		if xticks:
			axis.set_xticks(xticks[i])
		if xtickTags:
			axis.set_xticklabels(xtickTags[i])
		if yticks:
			axis.set_yticks(yticks[i])
		if ytickTags:
			axis.set_yticklabels(ytickTags[i])
		if titles:
			axis.set_title(titles[i])
	plt.subplots_adjust(hspace=1)
	plt.show()
	return fig, axes

def plot_multiple_2dline(x, ys, xlabel=None, ylabel=None, xtick=None, xtickLabel = None, ytick=None, ytickLabel=None, title=None, axis=None, fig=None, ylim=None, dataLabels=None):
	dataNum = len(ys)
	if axis==None and fig==None:
		fig, axis = plt.subplots(1,1)
	dataLabelIdx = 0
	plotList = list()
	for i in range(0,dataNum):
		if dataLabels:
			dataLabel = dataLabels[dataLabelIdx]
			dataLabelIdx += 1
		axis.plot(x,ys[i], label=dataLabel)
	if dataLabels:
		axis.legend(fontsize=7, loc='best')
	if ylim:
		axis.set_ylim(ylim)
	if xlabel:
		axis.set_xlabel(xlabel)
	if ylabel:
		axis.set_ylabel(ylabel)
	if xtick:
		axis.set_xticks(xtick)
	if xtickLabel:
		axis.set_xticklabels(xtickLabel)
	if ytick:
		axis.set_yticks(ytick)
	if ytickLabel:
		axis.set_yticklabels(ytickLabel)
	if title:
		axis.set_title(title)
#	if dataLabels: 
#		plt.legend(handles=plotList, fontsize=7)

	return fig

def plot_yy_bar(dataSeries, xlabel=None, ylabel=None, xtickRange=None, xtickTag=None, ytickRange=None, ytickTag=None, title=None, stdSeries=None, axis=None, fig=None, clist=None, dataLabels=None, yerrs=None, ylim=None, linewidth=None):
	pass
	
def make_month_tag():
	monthTags = list()
	basetime = datetime(2013,12,1)
	for i in range(0,19):
		monthTags.append(basetime.strftime('%b/%y'))
		basetime += timedelta(days=31)

	return monthTags
