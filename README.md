# impalamonitor
Code to collect data from  Cloudera Manager API. and insert to mysqlDB

#for direct day:
impalaqueries.py -dt 2021-01-01 -mysql ${mysqlhost} -impala ${cdmAPIhost}

#collect yesterday:
impalaqueries.py -mysql ${mysqlhost} -impala ${cdmAPIhost}
