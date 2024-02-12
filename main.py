import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import pymysql
import csv
# connecting to mysql workbench (database)
try:
    connection = mysql.connect(host='localhost',database='Employees',user='root',password='*****')  #connection with MySQL
   
    if connection.is_connected():
        db_Info = connection.get_server_info() 
        print("Connected to MySQL Server version ", db_Info)  # getting the  server info
        cursor = connection.cursor()                          # initializing the cursor
        cursor.execute("select database();")                  # selecting the database diamond 'executing sql cmnd'
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        mycursor = connection.cursor()
        mycursor.execute('CREATE DATABASE Employees') 
except Error as e:
    print("Error while connecting to Database", e)     


csv_path="path of your csv file"
df = pd.read_csv(csv_path)
d_df=df

def read_csv():
    d_df = pd.read_csv(csv_path)  # Replace Excel_file_name with your excel sheet name
    column_name=list(d_df.columns.values)
    columnNames=column_name
    return columnNames
    # mycursor.execute('CREATE table Employee IF NOT EXISTS ') 
    # print(df)

columnNames=read_csv()

def getColumnDtypes(dataTypes):
    dataTypes=d_df.dtypes
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        elif (x == 'DATE'):
            dataList.append('DATE')
        else:
            dataList.append('varchar')

    return dataList
   
columnDataType = getColumnDtypes(d_df.dtypes)            
#print(columnDataType)
    
def createTable():
    createTableStatement = 'CREATE TABLE IF NOT EXISTS table_name ('
    for i in range(len(columnDataType)):
        createTableStatement += '\n' + columnNames[i] + ' ' + columnDataType[i] + '(255)'+ ','
    createTableStatement = createTableStatement.rstrip(',') + ' );'

    print(createTableStatement)
    mycursor.execute(createTableStatement)
    print("Columns inserted Successfully\n")

createTable()
    
def insertValues():   
    count=0
    with open(csv_path, mode='r') as csv_file:
        #read csv using reader class
        csv_reader = csv.reader(csv_file)
        #skip header
        header = next(csv_reader)
        #Read csv row wise and insert into table
        
        for row in csv_reader:
            #print(row)
            count+=1
            sql = 'INSERT INTO table_name (emp_no,dept_no,from_date,to_date) VALUES (%s,%s,%s,%s)'
            #sql = f'INSERT INTO employee (emp_no,birth_date,first_name,last_name,gender,hire_date) VALUES ({row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]})'
            #print(sql)
            mycursor.execute(sql, tuple(row))
            
            if count==1000:  #modify the count as per requirement
                print("Records inserted")
                break

insertValues()
connection.commit()                  #  Records pushed to MySQL workbench  




