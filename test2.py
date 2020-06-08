import pandas as pd
import re
import requests

df_states_timeline = pd.read_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/states_timeline_data.csv')
state_tup = tuple(df_states_timeline['State UT'])
cols = ['date']
cols.extend([x.replace(" ", "") for x in list(df_states_timeline['State UT'])])
#print(cols)
date_v = list(df_states_timeline.columns)[:-1]
# print(date_v)

df_timeline = pd.DataFrame(None, None, cols)
df_timeline['date'] = date_v
for i in state_tup:
    s_val = [list(x) for i, x in df_states_timeline.loc[df_states_timeline['State UT'] == i].iterrows()]
    #print(s_val)
    df_timeline[i.replace(' ', '')] = s_val[0][:-1]

#print(df_timeline.tail(1))
response_4 = requests.get('https://covid-19india-api.herokuapp.com/all')
states = response_4.json()
print(states[0])
df_india_summary = pd.DataFrame(states[0],index=['last_updated'])
print(tuple(df_india_summary.columns))




