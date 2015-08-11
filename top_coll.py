import collector
reload(collector)
from collector import collector

coll = collector()
coll.collect_all_data(True)
