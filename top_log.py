import loganal
reload(loganal)
from loganal import loganal

la = loganal('metadata/parsed_anon_KEEP.csv')
la.calc_user_activities_raw()
la.calc_user_activities_count()
