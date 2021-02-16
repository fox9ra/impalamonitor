import requests
import json
from time import sleep
import argparse
import datetime
from datetime import timedelta
import pymysql
import csv
import os

yesterday = str(datetime.date.today()-datetime.timedelta(1))
parser = argparse.ArgumentParser()
parser.add_argument("-dt", default=yesterday)
parser.add_argument("-mysql", default="default")
parser.add_argument("-impala", default="default")
args = parser.parse_args()
dt=args.dt
mysql=args.mysql
impala=args.impala
date_time_obj = datetime.datetime.strptime(dt, '%Y-%m-%d')
offset = 0

con = pymysql.connect(host=mysql,
                             user='drelephant',
                             password='drelephant',
                             database='drelephant',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = con.cursor()

def requests_impala(starttime, endtime, offset):
    payload = {'from': starttime, 'to': endtime, 'filter': 'executing=false', 'limit': '1000', 'offset': offset}
    creds = ('readonly', 'readonly')
    impalaurl = "http://" + impala + ":7180/api/v7/clusters/" + impala + "/services/impala/impalaQueries"
    r = requests.get(impalaurl, params=payload, auth=creds)
    urlnik = str(r.url)
    data = json.loads(r.text)
    return data, urlnik

def parse_json(data, dt, urlnik):
    with open(dt+'test.txt', "a") as f:
        for i, item in enumerate(data):
            queryId = item.get("queryId")
            queryType = item.get("queryType")
            startTime = item.get("startTime")
            queryState = item.get("queryState")
            duration_millis = int(item.get("duration_millis", 0))
            user = item.get("user")
            database = item.get("database")
            pool = item.get("attributes").get("pool")
            connected_user = item.get("attributes").get("connected_user")
            memory_per_node_peak = item.get("attributes").get("memory_per_node_peak")
            thread_cpu_time = item.get("attributes").get("thread_cpu_time")
            hdfs_bytes_read = item.get("attributes").get("hdfs_bytes_read")
            hdfs_bytes_written = item.get("attributes").get("hdfs_bytes_written")
            admission_wait = item.get("attributes").get("admission_wait")

            f.write(str(queryId)+","\
                    +str(queryType)+","\
                    +str(queryState)+"," \
                    +str(startTime) + "," \
                    +str(duration_millis)+"," \
                    +str(admission_wait) +"," \
                    +str(pool) + "," \
                    +str(user)+"," \
                    +str(connected_user) + "," \
                    +str(database)+","\
                    +str(memory_per_node_peak)+","\
                    +str(thread_cpu_time)+","\
                    +str(hdfs_bytes_read)+","\
                    +str(hdfs_bytes_written)+\
                    #+str(urlnik)+
                    '\n')

def getdata(starttime, endtime, offset):
    q = 1
    w = 0
    while (q > 0):
        data, urlnik = requests_impala(starttime, endtime, offset)
        print(urlnik)
        print("TR:" + str(len(data['queries'])))
        print("TRW:" + str(len(data['warnings'])))
        w = len(data['warnings'])
        q = len(data['queries'])
        offset += 1000
        parse_json(data['queries'], dt, urlnik)
        print(data['warnings'])
        if (w!=0):
            break
            qqq=data['warnings'][0].split(" ")
            #print(qqq[10])
            offset=0
            starttime=qqq[10]
            sleep(10)
            getdata(starttime, endtime, offset)

for a in range(24):
    starttime=date_time_obj
    date_time_obj = date_time_obj + timedelta(seconds=3600)
    st=starttime.strftime('%Y-%m-%dT%H:00:00.000Z')
    et=starttime.strftime('%Y-%m-%dT%H:59:59.999Z')
    getdata(st, et, offset)

with open(dt+'test.txt') as result:
    uniqlines = set(result.readlines())
    with open(dt+'res.txt', 'w') as rmdup:
        rmdup.writelines(set(uniqlines))

f = open(dt+'res.txt')
csv_data = csv.reader(f)

for row in csv_data:
    #print(row)
    cursor.execute("INSERT INTO impala_queries (queries_queryId,\
     queries_queryType,\
     queries_queryState,\
     queries_startTime,\
     queries_durationMillis,\
     queries_attributes_admission_wait,\
     queries_attributes_pool,\
     queries_user,\
     queries_attributes_connected_user,\
     queries_database,\
     queries_attributes_memory_per_node_peak_node,\
     queries_attributes_thread_cpu_time,\
     queries_attributes_hdfs_bytes_read,\
     queries_attributes_hdfs_bytes_written) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   row)

con.commit()
con.close()
#os.remove(dt+'res.txt')
#os.remove(dt+'test.txt')
