import requests
import pandas as pd
import mysql.connector


def insert_sql(df, table, cursor):
    cols = ','.join([str(i) for i in df.columns.tolist()])
    s_in = []
    for i, row in df.iterrows():
        s_in.append(tuple(row))
    sql = "INSERT INTO " + table + "(" + cols + ") VALUES (" + "%s," * (len(df.columns)-1) + "%s)"
    cursor.executemany(sql, s_in)


# response_1=requests.get('https://covid-india-cases.herokuapp.com/states')
response_2 = requests.get('https://covid-india-cases.herokuapp.com/statetimeline/')
response_3 = requests.get('https://covid-19india-api.herokuapp.com/global')
response_4 = requests.get('https://covid-19india-api.herokuapp.com/all')

# print(response_1.status_code)
print(response_2.status_code)
print(response_3.status_code)
print(response_4.status_code)
# states=response_1.json()
states_timeline = response_2.json()
global_data = response_3.json()
states = response_4.json()

df_states = pd.DataFrame(states[1]['state_data'])
df_india_summary = pd.DataFrame(states[0],index=['last_updated'])
df_states_timeline = pd.DataFrame(states_timeline)
df_global = pd.DataFrame(global_data['data'], index=['details'])
df_update = pd.DataFrame(global_data['updates'])

# formatting data
df_global['active_rate']=df_global['active_rate'].apply(lambda x:x.replace('%',''))
df_global['recovered_rate']=df_global['recovered_rate'].apply(lambda x:x.replace('%',''))
df_global['death_rate']=df_global['death_rate'].apply(lambda x:x.replace('%',''))

df_update['global_updates']=df_update[0].apply(lambda x: x.replace('\"',''))
df_update['country']=df_update['global_updates'].apply(lambda x: x.split(' in ')[1].split('.')[0].rstrip())
df_update['new_case']=df_update['global_updates'].apply(lambda x: int(x.split(' new ')[0].replace(',','').strip() or 0))
df_update['new_death']=df_update['global_updates'].apply(lambda x: int(x[x.split('.')[0].find(' and ')+len(' and '):x.split('.')[0].rfind(' new ')].replace(',','').strip() or 0))
del df_update[0]


state_tup = tuple(df_states_timeline['State UT'])
cols = ['date']
cols.extend([x.replace(" ", "") for x in list(df_states_timeline['State UT'])])
date_v = list(df_states_timeline.columns)[:-1]
df_timeline = pd.DataFrame(None, None, cols)
df_timeline['date'] = date_v
for i in state_tup:
    s_val = [list(x) for i, x in df_states_timeline.loc[df_states_timeline['State UT'] == i].iterrows()]
    df_timeline[i.replace(' ', '')] = s_val[0][:-1]

# create csv
df_states.to_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/states_data.csv', index = None, header=True)
df_india_summary.to_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/india_total_summary.csv', index = None, header=True)
df_states_timeline.to_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/states_timeline_data.csv', index = None, header=True)
df_global.to_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/global_data.csv', index = None, header=True)
df_update.to_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/global_updates.csv', index = None, header=True)
df_timeline.to_csv(r'/home/amritaparna/PycharmProjects/covid19/venv/states_timeline_data_mod.csv', index = None, header=True)

# table load
mydb = mysql.connector.connect(
    host="localhost",
    user="sau",
    passwd="1998",
    database="covid19_study",
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
mycursor.execute('delete from state_details')
insert_sql(df_states, 'state_details', mycursor)
insert_sql(df_india_summary, 'india_summary', mycursor)
insert_sql(df_global,'global_data',mycursor)
insert_sql(df_update,'global_updates',mycursor)
insert_sql(df_timeline,'state_timeline',mycursor)
mydb.commit()


