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
    run_query(conn, sql)Q

    df = pd.read_sql(sql,conn)
    print(df)
    sql = 'SELECT * FROM PARTICIPANTS'
except Exception as e:
    print(e)
