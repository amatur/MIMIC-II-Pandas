import pandas as pd
from datetime import datetime

date_converter = lambda x: datetime.strptime(x[0:19], '%Y-%m-%d %H:%M:%S')
def date_subtractor(x, y):
    return (x - y).days

def get_table_mimic(padj, table_name):
    path_mimicroot = ""
    return path_mimicroot + padj + "/" + "{0}/{1}-{0}.txt".format(pid, table_name)   

def filler(x):
    return x

'''
Physiological variables init
'''

class PhysioVar:
    """A physiological variable class"""
    def __init__(self, id, min, max, unit, name):
        self.id = id
        self.min = min
        self.max = max
        self.name = name

vararr = []
vararr.append(PhysioVar(618, 0, 250, 'bpm', 'RESP'))   
vararr.append(PhysioVar(211, 0, 250, 'bpm', 'HEART'))  
vararr.append(PhysioVar(676, 25, 42, 'c', 'TEMP'))  
##null
#vararr.append(PhysioVar(3834, 400, 5000, '1000 cells per micro L', 'WBC'))   
vararr.append(PhysioVar(1162, 4, 500, 'mg per dL', 'BUN'))   
vararr.append(PhysioVar(791, 0.1, 9, 'mg per dL', 'CREAT'))   
#vararr.append(PhysioVar(2633, 90, 180, 'mmHg', 'SYSBP'))   
#vararr.append(PhysioVar(2632, 60, 110, 'mmHg', 'DIABP')) 
vararr.append(PhysioVar(52, 70, 110, 'mmHg', 'MEANBP')) 
vararr.append(PhysioVar(646, 60, 100, '%', 'O2SAT')) 
vararr.append(PhysioVar(818, 0, 10, 'mg per dL', 'LACTIC'))   
#vararr.append(PhysioVar(6256, 3000, 1000000, 'cells per L', 'PLAT'))   
#vararr.append(PhysioVar(6256, -1, 99999999999, 'cells per L', 'PLAT'))   
#vararr.append(PhysioVar(833, 2000, 8000, 'mg per dL', 'RBC'))   
vararr.append(PhysioVar(813, 19, 60, '%', 'HEMATOCRIT'))   
vararr.append(PhysioVar(837, 120, 160, 'mEq per dL', 'SODIUM'))   
vararr.append(PhysioVar(829, 2.2, 8, 'mEq per dL', 'POTASSIUM'))   
vararr.append(PhysioVar(786, 4.8, 12, 'mg per dL', 'CALCIUM'))   
vararr.append(PhysioVar(821, 0, 10, 'mg per dL', 'MAGNESIUM'))   
vararr.append(PhysioVar(772, 0.5, 18, 'mg per dL', 'ALBUMIN'))   
vararr.append(PhysioVar(780, 6.8, 7.8, ' ', 'ARTERIALPH'))   
#vararr.append(PhysioVar(3750, 0, 1000, ' ', 'URINEOUTFOLEY'))   
vararr.append(PhysioVar(763, 20, 200, 'kg', 'WEIGHT'))   
#vararr.append(PhysioVar(3750, 6.8, 7.8, ' ', 'MECHANICALVENTILATION'))   


for i in range(len(vararr)): 
    print vararr[i].id
'''
End of Physiological variables
'''


dfs = []
for j in range (0, 10):
    # padj: leftmost 2 digits 
    padj = str(j).rjust(2, '0')
    for i in range(1, 20):
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
                
                #df['RESP'] = df.apply(lambda x: date_subtractor(x['ICUSTAY_OUTTIME'], x['ICUSTAY_INTIME']), axis = 1)
            except Exception, e2:
                print e2
            data_merged = df.query('ICUSTAY_ADMIT_AGE>15 and STAY>0')
            #data_merged = data_merged.merge(TABLE_CHARTEV,on=['ICUSTAY_ID'], how='inner').groupby(['ICUSTAY_ID'], as_index=False)['VALUE1NUM'].mean()
            
            
            '''
            accumulate only resp variable
            TABLE_CHARTEV = TABLE_CHARTEV.query('ITEMID == {0} and VALUE1NUM >= {1} and  VALUE1NUM <= {2}'.format(physivar.id, physivar.min, physivar.max ) )
            #print TABLE_CHARTEV.groupby('ICUSTAY_ID')['VALUE1NUM'].mean()
            aggregated = TABLE_CHARTEV.groupby('ICUSTAY_ID').mean()['VALUE1NUM']
            aggregated.name = 'RESP'
            #print "***********************$$$$"
            #print aggregated
            #print "***********************"
            data_merged = data_merged.join(aggregated,on='ICUSTAY_ID')
            '''
            TABLE_CHARTEV = pd.read_csv(get_table_mimic(padj, 'CHARTEVENTS'), header = 0, usecols=['SUBJECT_ID', 'ITEMID', 'ICUSTAY_ID', 'VALUE1NUM' ])
            
            
            for physivar in vararr: 
                aggregated = TABLE_CHARTEV.query('ITEMID == {0} and VALUE1NUM >= {1} and  VALUE1NUM <= {2}'.format(physivar.id, physivar.min, physivar.max ) )
                aggregated = aggregated.groupby('ICUSTAY_ID').mean()['VALUE1NUM']
                aggregated.name = physivar.name
                #print "***********************$$$$"
                #print aggregated
                #print "***********************"
                data_merged = data_merged.join(aggregated, on='ICUSTAY_ID')
            
            
            del df
            dfs.append(data_merged)
        except Exception, e:
            print e
del data_tmp
#del data_tmp_icustays
del data_merged
data = pd.concat(dfs)
#del data['ICUSTAY_OUTTIME']
#del data['ICUSTAY_INTIME']
print data
print "Total data: {0}".format(data.shape[0])
data = data.query('HOSPITAL_EXPIRE_FLG == \'N\'')
print "Total data, after expire: {0}".format(data.shape[0])

data.to_csv('all.csv')

no_read = data.groupby(['SUBJECT_ID'])
no_read = no_read.filter(lambda x: len(x) < 2)
no_read['CLASS'] = no_read.apply(lambda x: filler(0), axis = 1)
#no_read = data[data.groupby('ICUSTAY_ID').pid.transform(len) > 1]
print no_read
print "Total not readmit,  {0}".format(no_read.shape[0])
no_read.to_csv('negative.csv')



#readmitted =  data.groupby(['SUBJECT_ID'])
#data['ICUSTAY_INTIME'] = data.apply(lambda x: str(x['ICUSTAY_INTIME']), axis = 1)
#readmitted = data.groupby('SUBJECT_ID').min()['ICUSTAY_ID']  

readmitted = data.groupby(['SUBJECT_ID']).filter(lambda x: len(x) > 1)
print readmitted
readmitted = readmitted.sort_values('ICUSTAY_ID', ascending=True).drop_duplicates(['SUBJECT_ID'])
readmitted['CLASS'] = readmitted.apply(lambda x: filler(1), axis = 1)
#readmitted = data.join(readmitted, on='ICUSTAY_ID')
print readmitted



readmitted.to_csv('positive.csv')
mimicall = []
mimicall.append(no_read)
mimicall.append(readmitted)
finaldata = pd.concat(mimicall)

finaldata.drop(finaldata.columns[[0, 1, 2, 3, 4, 5, 6]], axis=1, inplace=True)  # df.columns is zero-based pd.Index 
#finaldata.drop(['ICUSTAY_OUTTIME', 'ICUSTAY_INTIME', 'HOSPITAL_EXPIRE_FLG', 'ICUSTAY_ID', 'SUBJECT_ID', 'STAY' ], axis=1, inplace=True)


finaldata.to_csv('finaldata.csv', index = False)
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
