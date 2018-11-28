
import urllib
import bs4
import pandas as pd
import json
import numpy as np
import knoema
import datetime


site = "http://www.satp.org/satporgtp/countries/pakistan/database/casualties.htm"

import urllib.request
page = urllib.request.urlopen(site)

import bs4
from bs4 import BeautifulSoup
soup = BeautifulSoup(page)

tble=soup.find_all('table')[3]
A=[]
B=[]
C=[]
D=[]
E=[]
for row in tble.find_all('tr'):
  cells = row.find_all('td')
  A.append(cells[0].get_text().lstrip().rstrip())
  B.append(cells[1].get_text().lstrip().rstrip())
  C.append(cells[2].get_text().lstrip().rstrip())
  D.append(cells[3].get_text().lstrip().rstrip())
  E.append(cells[4].get_text().lstrip().rstrip())

df=pd.DataFrame()
df['Date']=A
df['Civilians']=B
df['Security Force Personnel']=C
df['Terrorist / Insurgents']=D
df['Total']=E
df.drop(df.index[-1],inplace=True)
df.drop(df.index[0],inplace=True)
#print(df)

tble1=soup.find_all('table')[2]
U=[]
W=[]
X=[]
Y=[]
Z=[]
for row1 in tble1.find_all('tr'):
  cells1 = row1.find_all('td')
  U.append(cells1[0].get_text().lstrip().rstrip())
  W.append(cells1[1].get_text().lstrip().rstrip())
  X.append(cells1[2].get_text().lstrip().rstrip())
  Y.append(cells1[3].get_text().lstrip().rstrip())
  Z.append(cells1[4].get_text().lstrip().rstrip()) 
df1=pd.DataFrame()
df1['Date']=U
df1['Civilians']=W
df1['Security Force Personnel']=X
df1['Terrorist / Insurgents']=Y
df1['Total']=Z
df1.drop(df1.index[0],inplace=True)
df1.drop(df1.index[-1],inplace=True)

new_df=pd.concat([df,df1],ignore_index=True)
date1=new_df['Date']
#print(date1)
new_df.drop(['Date'],axis=1,inplace=True)
#print(new_df)


json_data1 = '{"key":2098940,"id":"victim","name":"Victim","isGeo":false,"datasetId":"FTVP","datasetType":0,"fields":[{"key":2737320,"name":"id","displayName":"Id","type":6,"locale":null,"baseKey":null,"isSystemField":false}],"items":[{"key":1000000,"name":"Civilians","level":0,"hasData":true,"fields":{"id":"VIC.1"}},{"key":1000010,"name":"Security Force Personnel","level":0,"hasData":true,"fields":{"id":"VIC.2"}},{"key":1000020,"name":"Terrorist / Insurgents","level":0,"hasData":true,"fields":{"id":"VIC.3"}},{"key":1000030,"name":"Total","level":0,"hasData":true,"fields":{"id":"VIC.4"}}],"groups":[]}'
python_obj1 = json.loads(json_data1)

json_data = '{"key":2098950,"id":"indicator","name":"Indicator","isGeo":false,"datasetId":"FTVP","datasetType":0,"fields":[{"key":2737330,"name":"id","displayName":"Id","type":6,"locale":null,"baseKey":null,"isSystemField":false}],"items":[{"key":1000000,"name":"IND.1","level":0,"hasData":true,"fields":{"id":"IND.1"}}],"groups":[]}'
python_obj = json.loads(json_data)

d1=python_obj1["items"]
x=0
A1=[]
B1=[]
for x in d1:
 r1=x["name"]
 A1.append(r1)
 r2=x["fields"]
 r3=r2["id"]
 B1.append(r3)
 dfa=pd.DataFrame()
 dfa['Victim']=A1
 dfa['id']=B1
#print(dfa)

d2=python_obj["items"]
y=0
Y1=[]
Z1=[]
for y in d2:
 s1=y["name"]
 Y1.append(s1)
 s2=y["fields"]
 s3=s2["id"]
 Z1.append(s3)
 dfb=pd.DataFrame()
 dfb['Indicator']=Y1
 dfb['id']=Z1
#print(dfb)

#mapping
l1=list(new_df.columns.values)
#print(l1)
label=['Victim']
dfMap=pd.DataFrame(l1,columns=label)
#print(dfMap)
dfMap1=pd.merge(dfa,dfMap,on='Victim')

for d in new_df.columns:
 for m in dfMap1["Victim"]:
  if d==m:
   new_df.columns = dfMap1["id"]
#print(new_df)
new_df['Date']=date1
new_df['Unit']='Fatalities'
#print(new_df)


df_month={'Month':['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December'],'Code':['2018M1','2018M2','2018M3','2018M4','2018M5','2018M6','2018M7','2018M8','2018M9','2018M10','2018M11','2018M12']}
df_month=pd.DataFrame(df_month,columns=['Month','Code'])
#print(df_month)


new_df['DateS'] = ''; df_month.rename(columns={'Code': 'DateS'}, inplace=True)
mappingDF = new_df[['Date', 'DateS']].set_index('Date')
mappingDF.update(df_month.set_index('Month'))
new_df['DateS'] = mappingDF.values
new_df.loc[new_df['DateS'] == '', 'DateS'] = new_df['Date']
print(new_df)


dflist=list((new_df['DateS']))
#print(dflist)
mylist=[]
for Dt in dflist:
 if len(Dt)>5:
  mylist.append('M')
 else:
  mylist.append('A')
label1=['Frequency']
dfFreq=pd.DataFrame(mylist,columns=label1)
#print(dfFreq)
new_df.drop(['Date'],axis=1,inplace=True)
new_df=new_df.rename(columns={'DateS':'Date'})
new_df['Frequency']=dfFreq
new_df['Indicator']='IND.1'

Results = pd.melt(new_df, id_vars = ['Indicator','Date','Frequency','Unit',],var_name=["Victim"], value_name="Value")
#print(Results)

# writer = pd.ExcelWriter(r"C:\Users\Knoema\Desktop\data\final.xlsx", engine='xlsxwriter')
# Results.to_excel(writer, index=False,sheet_name='Sheet1')
# writer.save()

# apicfg = knoema.ApiConfig()
# apicfg.host = 'beta.knoema.org'
# apicfg.app_id = '6mOND0A'
# apicfg.app_secret ='m5mfK73GYzOHgQ'
# print("Uploading")
# knoema.upload(r"C:\Users\Knoema\Desktop\data\final.xlsx",'FTVP',public="False")
# print("Uploaded")
# knoema.verify('FTVP', datetime.date(2018,2,10),'South Asia Terrorism Portal', 'http://www.satp.org/satporgtp/countries/pakistan/database/')
# print("Verified")