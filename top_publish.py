import log_plotter
reload(log_plotter)
from log_plotter import log_plotter

logplot = log_plotter()
logplot.plot_all()

import genie_plotter
reload(genie_plotter)
from genie_plotter import genie_plotter
grap = genie_plotter()
grap.publish()
