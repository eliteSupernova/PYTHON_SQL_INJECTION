import pandas as pd
import numpy as np
# import mysql.connector
import datetime
from flask import Flask,request
from flaskext.mysql import MySQL

app=Flask(__name__)

mysql=MySQL()
app.config['MYSQL_DATABASE_USER']='root'
app.config['MYSQL_DATABASE_PASSWORD']='password'
app.config['MYSQL_DATABASE_DB']='task'
app.config['MYSQL_DATABASE_HOST']='localhost'
mysql.init_app(app)
def con_date(l):
    new_data=[]
    for i in l:
        if i == 'NA':
            new_data.append(0)
        else:
            string = str(i)
            date_data = datetime.datetime.strptime(i, '%H:%M:%S %p')
            hours = date_data.hour
            miniutes = date_data.minute
            new_time = ((hours * 60) + (miniutes)) / 60
            new_data.append(round(new_time,2))
    return new_data

def conv_int(l):
    new_feild=[]
    for i in l:
        try:
            new_feild.append(int(i))
        except ValueError:
            new_feild.append(0.0)
    return new_feild


@app.route('/execute', methods=['GET','POST'])
def execute():
    file=request.files['File']
    # data=file.read()
    data=pd.read_csv(file)
    date='1990-01-01'
    data.drop(['Emp Id','Critical Error'],axis=1,inplace=True)
    data.columns.values[9:12]=['FLEX FIELD4','FLEX FIELD5','FLEX FIELD6']
    data.fillna(value='NA',inplace=True)
    new_date=['01-01-1990' if i=='NA' else i for i in data['DATE']]
    fresh=[datetime.datetime.strptime(i,'%d-%m-%Y').date() for i in new_date]
    data['DATE']=fresh
    data['WORKING HRS']=con_date(data['WORKING HRS'])
    data['FLEX FIELD1']=conv_int(data['FLEX FIELD1'])
    data['FLEX FIELD2']=conv_int(data['FLEX FIELD2'])
    data['FLEX FIELD3']=conv_int(data['FLEX FIELD3'])
    # print(data.columns)
    conn=mysql.connect()
    cur=conn.cursor()
    SQL='select * from data'
    # cur.execute(SQL)
    # row=cur.fetchall()
    main_sql = "INSERT INTO data(PROCESS_ID,DATES,WORK_TYPE,PROCESS_p,Sub_Process,WORKING_HRS,FLEX_FIELD1,FLEX_FIELD2,FLEX_FIELD3,FLEX_FIELD4,FLEX_FIELD5,FLEX_FIELD6) Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    # print(row[0][1])=value
    # for i in range(len(data['DATE'])):
    #     value = (
    #     data['PROCESS ID'][i], data['DATE'][i], data['WORK TYPE'][i], data['PROCESS'][i], data['Sub Process'][i],
    #     data['WORKING HRS'][i], data['FLEX FIELD1'][i], data['FLEX FIELD2'][i], data['FLEX FIELD3'][i],
    #     data['FLEX FIELD4'][i], data['FLEX FIELD5'][i], data['FLEX FIELD6'][i])
    #     cur.execute(main_sql,value)
    #     conn.commit()
    # aleady_exist=[]

    # for i in range(len(data['DATE'])):
    #     value = (
    #         data['PROCESS ID'][i], data['DATE'][i], data['WORK TYPE'][i], data['PROCESS'][i],
    #         data['Sub Process'][i],
    #         data['WORKING HRS'][i], data['FLEX FIELD1'][i], data['FLEX FIELD2'][i], data['FLEX FIELD3'][i],
    #         data['FLEX FIELD4'][i], data['FLEX FIELD5'][i], data['FLEX FIELD6'][i])
    #     cur.execute(SQL)
    #     row = cur.fetchall()
        # print(row)
        # print(row==())
        # rows=list(row[i])
        # print(list(rows))
        # print(list(value))
        # print(value[i])
        #
    cur.execute(SQL)
    row = cur.fetchall()
    if row==():
        print('if entered')
        for i in range(len(data['DATE'])):
            value = (data['PROCESS ID'][i], data['DATE'][i], data['WORK TYPE'][i], data['PROCESS'][i],data['Sub Process'][i],data['WORKING HRS'][i], data['FLEX FIELD1'][i], data['FLEX FIELD2'][i], data['FLEX FIELD3'][i],data['FLEX FIELD4'][i], data['FLEX FIELD5'][i], data['FLEX FIELD6'][i])
            cur.execute(main_sql,value)
            conn.commit()
        print(i)
    else:
        # for i in range(len(data['DATE'])):
        # not_exist=[]
        values = [(data['PROCESS ID'][i], data['DATE'][i], data['WORK TYPE'][i], data['PROCESS'][i],data['Sub Process'][i],data['WORKING HRS'][i], data['FLEX FIELD1'][i], data['FLEX FIELD2'][i], data['FLEX FIELD3'][i],data['FLEX FIELD4'][i], data['FLEX FIELD5'][i], data['FLEX FIELD6'][i]) for i in range(len(data['DATE']))]
        print('value:',values,len(values))
        cur.execute(SQL)
        row = cur.fetchall()
        rows=[i for i in row]
        print('rows:',rows)
        # for i in row:
        #     if i not in rows:
        #         rows.append(i)
        already_exist = []
        not_exist=[]
        for i in values:
            print(i)
            if i not in rows:
                print('not exist')
                not_exist.append(i)
                # print('entered',i)
                # cur.execute(main_sql,i)
                # conn.commit()
                # print('entered',i)
            else:
                already_exist.append(i)
                # print('entered', i)
                # cur.execute(main_sql, i)
                # conn.commit()
        # cur.execute('delete fro m data where PROCESS_ID="NA";')
        # conn.commit()
        print('not exist:',not_exist,len(not_exist))
        print('already:',already_exist,len(already_exist))
        # print('else entered')
        # for i in range(len(data['DATE'])):
        #     value = (data['PROCESS ID'][i], data['DATE'][i], data['WORK TYPE'][i], data['PROCESS'][i],data['Sub Process'][i],data['WORKING HRS'][i], data['FLEX FIELD1'][i], data['FLEX FIELD2'][i], data['FLEX FIELD3'][i],data['FLEX FIELD4'][i], data['FLEX FIELD5'][i], data['FLEX FIELD6'][i])
        #     cur.execute(SQL)
        #     row = cur.fetchall()
        #     for j in range(len(row)):
        #         print(list(row[j]),list(value))
        #         if list(row[j])==list(value):
        #             # print(row[i],value[j])
        #             print(True)
        #             status=True
        #             aleady_exist.append(list(row[i]))
        #             print('alread exist')
        #         else:
        #             status=False
        #             cur.execute(main_sql, value)
        #             conn.commit()
        # #
        # # print("record updated",i)
    # print(already_exist)


    # print(data.head(15))
    return '1'


if __name__ == '__main__':
    app.run(debug=True)