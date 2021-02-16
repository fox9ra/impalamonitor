import os
import datetime

start_date = datetime.date(2020, 11, 1)
end_date = datetime.date(2021, 2, 13)
delta = datetime.timedelta(days=1)

while start_date <= end_date:
    #print(start_date)
    st=str(start_date)
    script="impalaqueries.py -dt" + " " + st + " -mysql mysql -impala impala"
    print(script)
    os.system(script)
    start_date += delta

