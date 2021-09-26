import pandas as pd
import snowflake.connector as sf
import json
import csv

#parsing login credential as JSON
with open('sf_login.json') as json_file:
    login = json.load(json_file)

def run_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

if __name__ == '__main__':
    try:
        conn = sf.connect(user=login.get('user'),
                          password=login.get('password'),
                          account=login.get('account'),
                          )
    except Exception as e:
        print(e)
        print("Couldn't able to validate login credentials")
else:
    print('Connected Successfully')
try:
    warehouse_sql = 'use warehouse {}'.format(login.get('warehouse'))
    run_query(conn, warehouse_sql)
    try:
        sql = 'alter warehouse {} resume'.format(login.get('warehouse'))
        run_query(conn, sql)
    except:
        pass
    sql = 'use database {}'.format(login.get('database'))
    run_query(conn, sql)
    sql = 'use role {}'.format(login.get('role'))
    run_query(conn, sql)
    sql = 'use schema {}'.format(login.get('schema'))
    run_query(conn, sql)
    print('Query is Running,Wait!!')

   ##Uploading csv to staging area
    try:

       sql =conn.cursor().execute('PUT file:///C:\\Users\\DELL\\PycharmProjects\\pythonProject\\Files\\Participants.csv @sms ')
       sql =conn.cursor().execute('PUT file:///C:\\Users\\DELL\\PycharmProjects\\pythonProject\\Files\\Exams.csv @sms')
       sql = conn.cursor().execute('PUT file:///C:\\Users\\DELL\\PycharmProjects\\pythonProject\\Files\\Parents.csv @sms')
       sql = conn.cursor().execute('PUT file:///C:\\Users\\DELL\\PycharmProjects\\pythonProject\\Files\\Events.csv @sms')
       # sql=conn.cursor().execute('remove @sms')#removing csv from stage area
       df = (sql, conn)
       print(df)
    except Exception as e:
       print(e)
    else:
       print('File loaded into staging area succesfully!!')


except Exception as e:
     print(e)
