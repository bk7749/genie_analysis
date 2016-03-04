import shelve
import csv
import pandas as pd
from datetime import datetime

developers = ["sheimi@ucsd.edu", "hteraoka@eng.ucsd.edu", "bbalaji@ucsd.edu", " sheimi@ucsd.edu", " hteraoka@eng.ucsd.edu", " bbalaji@ucsd.edu", "sheimi+test@eng.ucsd.edu", "sheimi+genie-demo@eng.ucsd.edu", "sheimi+demo@eng.ucsd.edu"]

reader = shelve.open('data/genieraws.shelve')
feedbackDict = reader['user_feedback']
userlist = reader['user_id_list']
reader.close()
fp = open('metadata/userlist.csv', 'wb')
for zone, users in userlist.iteritems():
	for user in users:
		fp.write(user+'\n')

feedbackMap = dict()
feedbackMap['Cold'] = -3
feedbackMap['Cool'] = -2
feedbackMap['Slightly Cool'] = -1
feedbackMap['Good'] = 0
feedbackMap['Slightly Warm'] = 1
feedbackMap['Warm'] = 2
feedbackMap['Hot'] = 3

usermap = pd.read_excel(open('metadata/user_map.xlsx','rb'))
feedbackList = pd.read_excel(open('metadata\user_feedback_genie.xlsx','rb'))
for row in feedbackList.iterrows():
	name = row[1]['name']
	email = usermap['email'][usermap['name']==name]
	if len(email)==0:
		continue
	email = email[email.index[0]]
	time = row[1]['timestamp']
	time = time.replace('.','',5)
	tp = datetime.strptime(time, '%b %d, %Y, %I:%M %p')
	feedback = feedbackMap[row[1]['feedback']]
	feedback = pd.DataFrame(data={'timestamp':tp,'value':feedback}, index=[0])
	if email in feedbackDict.keys():
		feedbackDict[email].append(feedback)
		feedbackDict[email].index = range(0,len(feedbackDict[email]))
	else:
		feedbackDict[email] = feedback

for user in feedbackDict.keys():
	if user in developers:
		del feedbackDict[user]

