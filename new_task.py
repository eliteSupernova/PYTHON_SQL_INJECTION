# import MySQLdb
import pymysql
import pandas as pd
import numpy as np
import mysql.connector
import datetime

conn=mysql.connector.connect(host='localhost',
                        database='task',
                        user='root',
                        password='password')


def con_date(i):
    new_file=0.0
    if i == 'NA':
       new_file=0.0
    else:
        date_file = datetime.datetime.strptime(i, '%H:%M:%S %p')
        hours = date_file.hour
        miniutes = date_file.minute
        new_time = ((hours * 60) + (miniutes))/ 60
        new_file=round(new_time,2)
    return new_file


def conv_int(l):
    new_feild=[]
    for i in l:
        try:
            new_feild.append(int(i))
        except ValueError:
            new_feild.append(0.0)
    return new_feild


file=pd.read_csv('C:\\Users\\1003647\\Desktop\\FLASK_API\\Prac_data.csv')
date='1990-01-01'
file.drop(['Emp Id','Critical Error'],axis=1,inplace=True)
file.columns.values[9:12]=['FLEX FIELD4','FLEX FIELD5','FLEX FIELD6']
file.fillna(value='NA',inplace=True)
new_date=['01-01-1990' if i=='NA' else i for i in file['DATE']]
fresh=[datetime.datetime.strptime(i,'%d-%m-%Y').date() for i in new_date]
file['DATE']=fresh
# new_wrok=con_date(list(file['WORKING HRS']))
# print(new_wrok)
file['WORKING HRS'] =[con_date(i) for i in file['WORKING HRS']]
# print(list(file['WORKING HRS']))
file['FLEX FIELD1'] = conv_int(file['FLEX FIELD1'])
file['FLEX FIELD2'] = conv_int(file['FLEX FIELD2'])
file['FLEX FIELD3'] = conv_int(file['FLEX FIELD3'])
# print(file.dtypes)
cur=conn.cursor()
SQL='select * from data'
main_sql = "INSERT INTO data(PROCESS_ID,DATES,WORK_TYPE,PROCESS_p,Sub_Process,WORKING_HRS,FLEX_FIELD1,FLEX_FIELD2,FLEX_FIELD3,FLEX_FIELD4,FLEX_FIELD5,FLEX_FIELD6) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
cur.execute(SQL)
row = cur.fetchall()
# print(row)

print(file)
# def con_date(l):
#     new_file=[]
#     for i in l:
if row==[]:
    print('if entered')
    value = [(file['PROCESS ID'][i].astype(str), file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i],file['Sub Process'][i],file['WORKING HRS'][i].astype(float), file['FLEX FIELD1'][i].astype(float), file['FLEX FIELD2'][i].astype(float), file['FLEX FIELD3'][i].astype(float),file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i]) for i in range(len(file['DATE']))]
    values=[]
    [values.append(i) for i in value if i not in values]
    print(len(values))
    for i in values:
        cur.execute(main_sql,i)
        conn.commit()
    # print(i)
else:
    value = [(file['PROCESS ID'][i].astype(str), file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i],file['Sub Process'][i],file['WORKING HRS'][i].astype(float), file['FLEX FIELD1'][i].astype(float), file['FLEX FIELD2'][i].astype(float), file['FLEX FIELD3'][i].astype(float),file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i]) for i in range(len(file['DATE']))]
    cur.execute(SQL)
    row = cur.fetchall()
    print(row)
    print(value)
    already_exist = []
    not_exist=[]
    [already_exist.append(i) if i in row else not_exist.append(i) for i in value]
    for i in not_exist:
        cur.execute(main_sql,i)
        conn.commit()
    print('not exist:',not_exist,len(not_exist))
    print('already:',already_exist,len(already_exist))
    # print('entered',i)
    # cur.execute(main_sql,i)
    # conn.commit()
    # print('entered',i)
            # print('entered', i)
            # cur.execute(main_sql, i)
            # conn.commit()
    # cur.execute('delete from file where PROCESS_ID="NA";')
    # conn.commit()
#         if i == 'NA':
#             new_file.append(0)
#         else:
#             string = str(i)
#             date_file = datetime.datetime.strptime(i, '%H:%M:%S %p')
#             hours = date_file.hour
#             miniutes = date_file.minute
#             new_time = ((hours * 60) + (miniutes)) / 60
#             new_file.append(round(new_time,2))
#     return new_file
#
# def conv_int(l):
#     new_feild=[]
#     for i in l:
#         try:
#             new_feild.append(int(i))
#         except ValueError:
#             new_feild.append(0.0)
#     return new_feild
#
#

# file=file.read()
# file=pd.read_csv(file)
# date='1990-01-01'
# file.drop(['Emp Id','Critical Error'],axis=1,inplace=True)
# file.columns.values[9:12]=['FLEX FIELD4','FLEX FIELD5','FLEX FIELD6']
# file.fillna(value='NA',inplace=True)
# new_date=['01-01-1990' if i=='NA' else i for i in file['DATE']]
# fresh=[datetime.datetime.strptime(i,'%d-%m-%Y').date() for i in new_date]
# file['DATE']=fresh
# file['WORKING HRS']=con_date(file['WORKING HRS'])
# file['FLEX FIELD1']=conv_int(file['FLEX FIELD1'])
# file['FLEX FIELD2']=conv_int(file['FLEX FIELD2'])
# file['FLEX FIELD3']=conv_int(file['FLEX FIELD3'])
# # print(file.columns)
# conn=mysql.connect()
# cur=conn.cursor()
# SQL='select * from file'
# # cur.execute(SQL)
# # row=cur.fetchall()
# main_sql = "INSERT INTO file(PROCESS_ID,DATES,WORK_TYPE,PROCESS_p,Sub_Process,WORKING_HRS,FLEX_FIELD1,FLEX_FIELD2,FLEX_FIELD3,FLEX_FIELD4,FLEX_FIELD5,FLEX_FIELD6) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
# # print(row[0][1])=value
# # for i in range(len(file['DATE'])):
# #     value = (
# #     file['PROCESS ID'][i], file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i], file['Sub Process'][i],
# #     file['WORKING HRS'][i], file['FLEX FIELD1'][i], file['FLEX FIELD2'][i], file['FLEX FIELD3'][i],
# #     file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i])
# #     cur.execute(main_sql,value)
# #     conn.commit()
# # aleady_exist=[]
#
# # for i in range(len(file['DATE'])):
# #     value = (
# #         file['PROCESS ID'][i], file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i],
# #         file['Sub Process'][i],
# #         file['WORKING HRS'][i], file['FLEX FIELD1'][i], file['FLEX FIELD2'][i], file['FLEX FIELD3'][i],
# #         file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i])
# #     cur.execute(SQL)
# #     row = cur.fetchall()
#     # print(row)
#     # print(row==())
#     # rows=list(row[i])
#     # print(list(rows))
#     # print(list(value))
#     # print(value[i])
#     #
# cur.execute(SQL)
# row = cur.fetchall()
# if row==():
#     print('if entered')
#     for i in range(len(file['DATE'])):
#         value = (file['PROCESS ID'][i], file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i],file['Sub Process'][i],file['WORKING HRS'][i], file['FLEX FIELD1'][i], file['FLEX FIELD2'][i], file['FLEX FIELD3'][i],file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i])
#         cur.execute(main_sql,value)
#         conn.commit()
#     print(i)
# else:
#     # for i in range(len(file['DATE'])):
#     # not_exist=[]
#     values = [(file['PROCESS ID'][i], file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i],file['Sub Process'][i],file['WORKING HRS'][i], file['FLEX FIELD1'][i], file['FLEX FIELD2'][i], file['FLEX FIELD3'][i],file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i]) for i in range(len(file['DATE']))]
#     print('value:',values,len(values))
#     cur.execute(SQL)
#     row = cur.fetchall()
#     rows=[i for i in row]
#     print('rows:',rows)
#     # for i in row:
#     #     if i not in rows:
#     #         rows.append(i)
#     already_exist = []
#     not_exist=[]
#     for i in values:
#         print(i)
#         if i not in rows:
#             print('not exist')
#             not_exist.append(i)
#             # print('entered',i)
#             # cur.execute(main_sql,i)
#             # conn.commit()
#             # print('entered',i)
#         else:
#             already_exist.append(i)
#             # print('entered', i)
#             # cur.execute(main_sql, i)
#             # conn.commit()
#     # cur.execute('delete fro m file where PROCESS_ID="NA";')
#     # conn.commit()
#     print('not exist:',not_exist,len(not_exist))
#     print('already:',already_exist,len(already_exist))
#     # print('else entered')
#     # for i in range(len(file['DATE'])):
#     #     value = (file['PROCESS ID'][i], file['DATE'][i], file['WORK TYPE'][i], file['PROCESS'][i],file['Sub Process'][i],file['WORKING HRS'][i], file['FLEX FIELD1'][i], file['FLEX FIELD2'][i], file['FLEX FIELD3'][i],file['FLEX FIELD4'][i], file['FLEX FIELD5'][i], file['FLEX FIELD6'][i])
#     #     cur.execute(SQL)
#     #     row = cur.fetchall()
#     #     for j in range(len(row)):
#     #         print(list(row[j]),list(value))
#     #         if list(row[j])==list(value):
#     #             # print(row[i],value[j])
#     #             print(True)
#     #             status=True
#     #             aleady_exist.append(list(row[i]))
#     #             print('alread exist')
#     #         else:
#     #             status=False
#     #             cur.execute(main_sql, value)
#     #             conn.commit()
#     # #
#     # # print("record updated",i)
# # print(already_exist)
#
#
# # print(file.head(15))
