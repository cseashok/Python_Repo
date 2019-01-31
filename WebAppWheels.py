# @Author: Nathan North,Pradeep Rawat
# @Org: GE Transportation
# @Project: Series X
# Copywrite - all rights reserved. DO NOT COPY OR DISTRIBUTE.

from flask import Flask, render_template, redirect, url_for, request, Response, session
from UserInterfaceDataHandler import UserInterfaceDataHandler
import os
import datetime
import csv
import pyhs2
import subprocess
import sys
sys.path.append('./Imports')
import Putil
from Putil import Putil
import Compiler
from Compiler import Compiler
import json

app = Flask(__name__)
app.config.from_object(__name__)
app.config['SECRET_KEY'] = 'Wheels'


def load_configuration():
    '''Simply loads the json register lookup file'''
    with open('register-lookup.json', 'r') as inputFile:
        config = json.load(inputFile)
    return config


uidh = UserInterfaceDataHandler()



def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    return s_return, s_output, s_err

def HwxConnection(table , path):
    host_parm = 'ip-10-228-3-43.ec2.internal'
    port_parm = '10000'
    auth_parm = 'KERBEROS'
    credentials = {'host': host_parm, 'port': port_parm, 'authMechanism': auth_parm , 'database': 'get_wheeltrue'}
    conn = pyhs2.connect(**credentials)
    with conn.cursor() as cur:
		#ins_query = 'LOAD DATA LOCAL INPATH '+path+' INSERT  INTO TABLE '+table+''
                ins_query = "load data inpath '/tmp/Wheeltrue/CsvData/"+table+".csv' into table "+table
                print ("Exectuing..."+ins_query)
                #exec ins_query
                cur.execute(ins_query)
                print ("Exectuing..."+ins_query)
                #cur.execute(ins_query)

## Call: dbConnect(db_parm, username_parm, host_parm, pw_parm)
def dbConnect(db_parm, username_parm, host_parm, pw_parm):
        credentials = {'host': host_parm, 'database': db_parm, 'user': username_parm, 'password': pw_parm}
        # Import the adapter and try to connect to the database
        try:
            conn = psycopg2.connect(**credentials)
            conn.autocommit = True  # auto-commit each entry to the database
        except Exception as e:
            print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " : Could not establish connection with database. Below is the exception\n" + str(e.message))
            print("Please check the log file for detail error report: /apps/vfton/node_script/server/logs/"+str(user_id)+".txt")
            sys.exit(1)
        # Define a dictionary cursor
        conn.cursor_factory = RealDictCursor
        cur = conn.cursor()
        return cur

## Call: dbQuery(cur, query)
def dbQuery(cur, query):
        # Execute a query
        try:
            cur.execute(query)
            print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " : Executed below query:\n" + str(query) + "\n")
        except Exception as e:
            print(strftime("%Y-%m-%d %H:%M:%S", gmtime()) + " : Execution of DB query failed. Below is the exception\n" + str(e.message))
            print("Please check the log file for detail error report: /apps/vfton/node_script/server/logs/"+str(user_id)+".txt")
            sys.exit(1)
        # Get each row
        rows = cur.fetchall()
        return rows

def update_status(cur,control_table,status,user_id):
#Function calls query to update the control table with the status of the process
        try:
            query="update " + control_table + " set status='"+ status +"' where user_id = '"+ user_id + "';"
            cur.execute(query)
            print ("Function : update_status() : Updating Control table with the status "+ control_table +"\n")
        except Exception as e:
            print("Function : update_status() : Execution of DB query failed with the error message\n" + str(e.message))
            print("Please check the log file for detail error report: /apps/vfton/node_script/server/logs/"+str(user_id)+".txt")
            sys.exit(1)
			
def index():

    serverTime = datetime.datetime.now()
    fmtServerTime = serverTime.strftime('%A, %b %d, %Y %H:%M:%S')
    recentData = []
    uidh.renew_predix_token()
    recentData = uidh.pull_most_recent_datapoint()
    newtables = uidh.configuration['Tables']
    for table in newtables:

        try:
            table
            print(table)
            newrequest_Data = {'sel1' : 'Calgary, AB' , 'getDataButton' : 'getData' , 'sel2' : table , 'allCheckBox':'True'}
            data = uidh.run_export_request(newrequest_Data)
	        newFileName= table+'.csv'
	        path = './CsvData/'+newFileName
            with open('./CsvData/'+newFileName, "w") as output:
                writer = csv.writer(output,lineterminator='\n')
                writer.writerow([data])
                (ret, out, err)= run_cmd(['hdfs', 'dfs', '-rm','/tmp/Wheeltrue/CsvData/'+table+'.csv'])
                (ret, out, err)= run_cmd(['hdfs', 'dfs', '-copyFromLocal',path,'/tmp/Wheeltrue/CsvData'])
                if (err != ''):
                    print "Script Errored out while copying data into HWX"
                    print "Original Error message:"+ err
                    sys.exit(1)
		HwxConnection(table , path)
        except NameError:
            print 'Api failed please restart'


def init():
    index()
    config = load_configuration()
    processor = Putil(config)
    processor.run_data_import()
    comp = Compiler(processor.data, config)
    comp.run_compilation()
    newFilenameSeventhTable = "DataAnalytics"
    freshfilename = newFilenameSeventhTable+'.csv'
    tablenew = "DataAnalytics"
    pathNew = './CsvData/'+freshfilename
    (ret, out, err)= run_cmd(['hdfs', 'dfs', '-rm','/tmp/Wheeltrue/CsvData/'+newFilenameSeventhTable+'.csv'])
    (ret, out, err)= run_cmd(['hdfs', 'dfs', '-copyFromLocal',pathNew,'/tmp/Wheeltrue/CsvData'])
    if (err != ''):
        print "Script Errored out while copying data into HWX"
        print "Original Error message:"+ err
        sys.exit(1)
    HwxConnection(tablenew , pathNew)



init()
