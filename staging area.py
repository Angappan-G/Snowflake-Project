import pandas as pd
import json
import snowflake.connector as sf

#parsing JSON
try:
    with open('sf_login.json') as file:
        login = json.load(file)
except:
    print('Unable to load JSON')

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
        print("Couldn't able to validate login credentials,Error!!")
        print(e)
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

    ##Uploading csv to staging area
    try:
        sql1=("CREATE OR REPLACE FILE FORMAT MyCSVFORMAT1 TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER= 1 NULL_IF= ('NULL', 'null')")
        sql2="DESC FILE FORMAT MyCSVFORMAT"
        sql3="CREATE OR REPLACE STAGE SCHOOL2 FILE_FORMAT = CSVFORMAT"
        sql4 ='PUT file:///C:\\Users\\DELL\\PycharmProjects\\pythonProject\\Files\\Parents.csv @SCHOOL1'
        sql5='COPY INTO sms2 FROM @SCHOOL1 ON_ERROR = CONTINUE'
        sql6='SELECT * FROM sms2'
        df =pd.read_sql (sql1,conn)
        print(df)
        df = pd.read_sql(sql2, conn)
        print(df)
        df = pd.read_sql(sql3, conn)
        print(df)
        df = pd.read_sql(sql4, conn)
        print(df)
        df = pd.read_sql(sql5, conn)
        print(df)
        df = pd.read_sql(sql6, conn)
        print(df)

    except Exception as e:
        print(e),
    else:
        print('STAGE CREATED SUCCESSFULLY AND CSV FILE LOADED INTO STAGE AREA SUCCESSFULLY')




except Exception as e:
    print(e)
