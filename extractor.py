import pandas as pd
from datetime import datetime

date_converter = lambda x: datetime.strptime(x[0:19], '%Y-%m-%d %H:%M:%S')
def date_subtractor(x, y):
    return (x - y).days

def get_table_mimic(padj, table_name):
    path_mimicroot = ""
    return path_mimicroot + padj + "/" + "{0}/{1}-{0}.txt".format(pid, table_name)   

dfs = []
for j in range (0, 2):
    # padj: leftmost 2 digits 
    padj = str(j).rjust(2, '0')
    for i in range(1, 10):
        try:
            # padj: rightmost 3 digits 
            padi = str(i).rjust(3, '0')
            pid = padj + padi
            file_dpatients = padj + "/" + "{0}/D_PATIENTS-{0}.txt".format(pid)    
            #file_icustayev = "{0}/ICUSTAYEVENTS-{0}.txt".format(t)
            file_icustaydet = padj + "/" + "{0}/ICUSTAY_DETAIL-{0}.txt".format(pid)
            
            data_tmp = pd.read_csv(file_dpatients, header = 0, parse_dates=True, usecols=['SUBJECT_ID', 'HOSPITAL_EXPIRE_FLG'])
            #data_tmp_icustays = pd.read_csv(file_icustayev, header = 0, parse_dates=True, usecols=['SUBJECT_ID', 'ICUSTAY_ID','INTIME'])  
            data_tmp_icustaydet = pd.read_csv(file_icustaydet, header = 0, parse_dates=True, usecols=['SUBJECT_ID', 'ICUSTAY_ID', 'ICUSTAY_ADMIT_AGE', 'ICUSTAY_INTIME','ICUSTAY_OUTTIME'])  
            
            df = pd.merge(data_tmp, data_tmp_icustaydet, on = 'SUBJECT_ID')
            df['ICUSTAY_INTIME'] = df['ICUSTAY_INTIME'].apply(date_converter)
            df['ICUSTAY_OUTTIME'] = df['ICUSTAY_OUTTIME'].apply(date_converter)
            try:
                df['STAY'] = df.apply(lambda x: date_subtractor(x['ICUSTAY_OUTTIME'], x['ICUSTAY_INTIME']), axis = 1)
            except Exception, e2:
                print e2
            data_merged = df.query('ICUSTAY_ADMIT_AGE>15 and STAY>0')
            del df
            dfs.append(data_merged)
        except Exception, e:
            print e
del data_tmp
#del data_tmp_icustays
del data_merged
data = pd.concat(dfs)
del data['ICUSTAY_OUTTIME']
del data['ICUSTAY_INTIME']
print data
print "Total data: {0}".format(data.shape[0])

#df.to_csv('foo.csv')



'''
### HELPING CODES

## Own date parser
dateparser = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
# Which makes your read command:
pd.read_csv(infile, parse_dates=['columnName'], date_parser=dateparse)
# Or combine two columns into a single DateTime column
pd.read_csv(infile, parse_dates={'datetime': ['date', 'time']}, date_parser=dateparse)



#### HOW TO READ ALL FILES

#files = glob.glob('*.csv')
#weather_dfs = [pd.read_csv(fp, names=columns) for fp in files]
#weather = pd.concat(weather_dfs)


#### HOW TO INDEX

print df.iloc[0, 2].month()
subset1 = df.iloc[:, :]
print df



#### HOW TO QUERY DATA

#data_merged = df.query('ICUSTAY_ADMIT_AGE>0').query('0<b<2')
#df_filtered = df.query('a>0 and 0<b<2')


### HOW TO WRITE DATETIME DATE PARSER and DAY SUBTRACT
datetime_object = datetime.strptime('2896-10-24 11:42:00', '%Y-%m-%d %H:%M:%S')
datetime_object2 = datetime.strptime('2846-10-24 11:42:00', '%Y-%m-%d %H:%M:%S')
print round((datetime_object -  datetime_object2).days/365.2, 2)
'''


'''
#### HOW TO DO PIVOT
dfs = []
path = 'D:\\'
# data = pd.DataFrame()
for file in raw_files:
    data_tmp = pd.read_csv(path + file, engine='c',
                           compression='gzip',
                           low_memory=False,
                           usecols=['date', 'Value', 'ID'])
    data_tmp = data_tmp.pivot(index='date', columns='ID',
                              values='Value')
    dfs.append(data_tmp)
del data_tmp
data = pd.concat(dfs)
'''

'''
#### HOW TO WRITE LAMBDA
#doubler = lambda x: x*2
'''
