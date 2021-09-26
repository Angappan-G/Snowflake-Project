import pandas as pd
import snowflake.connector as sf
import json
import csv,os,sys

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

    def fileCreation(fileName):
        with open(fileName,'w',newline='') as exams1:
            writer = csv.writer(exams1,delimiter = ',')
            print('Created Sucessfully')
    def fileCreation1(fileName):
        with open(fileName,'w',newline='') as participants1:
            writer = csv.writer(participants1,delimiter = ',')
            print('Created Sucessfully')
    print('files created')
    def fileCreation2(fileName):
        with open(fileName,'w',newline='') as parents:
            writer = csv.writer(parents,delimiter = ',')
            print('Created Sucessfully')
    def fileCreation3(fileName):
        with open(fileName,'w',newline='') as events:
            writer = csv.writer(events,delimiter = ',')
            print('Created Sucessfully')
    print('files created')
    def writingData(fileName,data):
        with open(fileName,'w',newline='') as exams1:
            writer = csv.writer(exams1,delimiter = ',')
            writer.writerow(['exam_id','stu_id','E_sub','E_type','E_date',
                             ' E_block','E_timing','E_session'])
            writer.writerows(data)
    print("Written data Sucessfully")
    def writingData1(fileName,data):
        with open(fileName,'w',newline='') as participants1:
            writer = csv.writer(participants1,delimiter = ',')
            writer.writerow(['p_id','p_name','p_age','p_gender','p_city','p_mobnum'])
            writer.writerows(data)
    print("Written data Sucessfully")
    def writingData2(fileName,data):
        with open(fileName,'w',newline='') as participants1:
            writer = csv.writer(participants1,delimiter = ',')
            writer.writerow(['p_id','p_name','p_age','p_gender','p_mail','p_mobnum','p_add'])
            writer.writerows(data)
    print("Written data Sucessfully")
    def writingData3(fileName,data):
        with open(fileName,'w',newline='') as events:
            writer = csv.writer(events,delimiter = ',')
            writer.writerow(['event_type','event_id','event_name','event_partid','event_sponsor',
                             'event_duration','event_guest'])
            writer.writerows(data)
    print("Written data Sucessfully")
    def dataCollection():
        exams1 = []
        no_exams = int(input("Enter the no. of exams: "))
        for i in range(no_exams):
            exam_id = input("Enter the exam id : ")
            stu_id = input("Enter the student id")
            E_sub = input("Enter the subject: ")
            E_type = input("Enter the type of exam : ")
            E_date= input("Enter the date  : ")
            E_block = input("Enter the block :")
            E_timing = input("Enter the timing : ")
            E_session = input("Enter the session : ")
            data = [exam_id, stu_id, E_sub, E_type, E_date, E_block, E_timing, E_session]
        exams1.append(data)
        return exams1

    def dataCollection1():
        participants1 = []
        no_participants = int(input("Enter the no. of participants: "))
        for i in range(no_participants):
            p_id = input("Enter the participant id : ")
            p_name = input("Enter the participant name : ")
            p_age = input("Enter the participant age : ")
            p_gender = input("Enter the participant gender : ")
            p_city = input("Enter the participant city :")
            p_mobnum = input("Enter the participant mobile number : ")

            data1 = [p_id,p_name,p_age,p_gender,p_city,p_mobnum]
        participants1.append(data1)
        return participants1
    def dataCollection2():
        parents = []
        no_parents = int(input("Enter the no. of parents: "))
        for i in range(no_parents):
            p_id = input("Enter the parent id : ")
            p_name = input("Enter the parent name : ")
            p_age = input("Enter the parent age : ")
            p_gender = input("Enter the parent gender  : ")
            p_mail = input("Enter the parent mail :")
            p_mobnum = input("Enter the parent mobile number : ")
            p_add = input("Enter the parent address: ")
            data2 = [p_id,p_name,p_age,p_gender,p_mail,p_mobnum,p_add]
        parents.append(data2)
        return parents
    def dataCollection3():
        events = []
        no_events = int(input("Enter the no. of events: "))
        for i in range(no_events):
            event_type = input("Enter the event type: ")
            event_id = input("Enter the event id: ")
            event_name = input("Enter the event name : ")
            event_partid = input("Enter the participant id: ")
            event_sponsor = input("Enter the event sponsor: ")
            event_dur = input("Enter the duration: ")
            event_guest = input("Enter the guest: ")

            data3 = [event_type,event_id,event_name,event_partid,event_sponsor,event_dur, event_guest]
        events.append(data3)
        return events
    print("1. Create EXAMS1 CSV file")
    print("2. Create PARTICIPANTS1 CSV file")
    print("3. Create Parents CSV file ")
    print("4. Create EVENTS CSV file")
    print("5. Writting Data in EXAMS1 CSV file")
    print("6. Writting Data in PARTICIPANTS1 CSV file")
    print("7. Writting Data in PARENTS CSV file")
    print("8. Writting Data in EVENTS CSV file")
    opt = int(input("Enter your option : "))
    if opt >= 1 or opt <= 8 :
        fileName = input("Enter the file_name: ")

    if opt == 1:
        fileCreation(fileName)
    elif opt == 2:
        fileCreation1(fileName)
    elif opt == 3:
        fileCreation2(fileName)
    elif opt == 4:
        fileCreation3(fileName)
    elif opt == 5:
        data = dataCollection()
        writingData(fileName,data)
    elif opt == 6:
        data = dataCollection1()
        writingData1(fileName,data)
    elif opt == 7:
        data = dataCollection2()
        writingData2(fileName,data)
    elif opt == 8:
        data = dataCollection3()
        writingData3(fileName,data)

except Exception as e:
    print(e)
