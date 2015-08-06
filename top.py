#import analyzer
#reload(analyzer)
#from analyzer import analyzer

#a = analyzer()
#a.collect_all_data()
#a.process_all_data()
#a.plot_all_data()

import sys
import pdb

from collector import collector
coll = collector()

if sys.argv[1]=='1':
	if sys.argv[2]=='1':
		pdb.run('coll.collect_all_data(True)')
	else:
		pdb.run('coll.collect_all_data(False)')
else:
	if sys.argv[2] =='1':
		coll.collect_all_data(True)
	else:
		coll.collect_all_data(False)


from processor import processor
proc = processor()
if sys.argv[1] == '1':
	pdb.run('proc.process_all_data()')
else:
	proc.process_all_data()


from genie_plotter import genie_plotter
grap = genie_plotter()
if sys.argv[1] == '1':
	pdb.run('grap.plot_all()')
else:
	grap.plot_all()

