import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import operator
import matplotlib
import matplotlib.colors as col
import matplotlib.cm as cm
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt

class plotter:
	# dataSeries (list of list), figSize (2dim tuple) -> fig
	# Other details are implemented in inheritor
	def plot_multiple_bars(self, dataSeries, figSize):
		pass
