import time
import pandas as pd
import argparse
from sqlalchemy import create_engine,text



# This Code is use to chunk bulk data from command line for run split.py from command line 
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('filename', type=str,help='an integer for the accumulator')


args = parser.parse_args()
filename= args.filename
print(filename)



# Crete Engine To Connect With Database
engine = create_engine('mysql+mysqlconnector://username:password@host:port/database name')


# A Dictionary For Change status Name of Machine
status_dic = {'running':'ACTIVE',
                'waiting':'INTERRUPTED',
                 'stopped':'STOPPED', 
                 'nodet':'mtcdisconnect'}

# Add 2 New Column For 1 is For Combine date and time And 1 is For Chenged Status of Machine

Data = pd.read_csv(filename)
Data['Logdatetime'] = Data['log_Year'].astype(str) + '-' + Data['log_Month'].astype(str)+ '-' + Data['log_Day'].astype(str)+ ' ' + Data['log_Hour'].astype(str)+ ':' + Data['log_Minute'].astype(str)+ ':' + Data['log_Second'].astype(str)
Data['Status1'] = Data.log_Status.map(status_dic)



# This Loop is for create a query to add in databese and also create a text files of query as per requirment
allQuery = ''
count =0
for index,row in Data.iterrows():
    allQuery += f'''INSERT INTO `mtConnect` (`mtConnectId`, `machineId`, `State`, `Partcount`, `ControllerMode`, `RotatoryLoad`, `PathFeedrate`, `RotatoryActVelo`, `RotatoryOverrideVelo`, `Xload`, `Yload`, `Zload`, `addedTime`, `originalTime`, `EmergencyStop`, `PathPosition`, `toolid`, `line`) VALUES (NULL, '2', '{row.Status1}', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{row.Logdatetime}', '{row.Logdatetime}', NULL, NULL, NULL, NULL);'''
    
    
    query =text(f"INSERT INTO `mtConnect` (`mtConnectId`, `machineId`, `State`, `Partcount`, `ControllerMode`, `RotatoryLoad`, `PathFeedrate`, `RotatoryActVelo`, `RotatoryOverrideVelo`, `Xload`, `Yload`, `Zload`, `addedTime`, `originalTime`, `EmergencyStop`, `PathPosition`, `toolid`, `line`) VALUES (NULL, '2', '{row.Status1}', 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '{row.Logdatetime}', '{row.Logdatetime}', NULL, NULL, NULL, NULL);")
    with engine.begin() as conn:
        result = conn.execute(query)

    time.sleep(0.3)

    if (count%1000)==0: 
        
        f = open(f"sql{count}.txt", "w")
        f.write(allQuery)
        f.close()
        print('10k com',count)
        allQuery= ''
    count = count+1
    print(count)


engine.dispose()
