import argparse
import datetime

yesterday = str(datetime.date.today()-datetime.timedelta(1))
parser = argparse.ArgumentParser()
parser.add_argument("-dt", default=yesterday)
args = parser.parse_args()
dt=args.dt
date_time_obj = datetime.datetime.strptime(dt, '%Y-%m-%d')
print(date_time_obj)
'''for a in range(23):
    starttime=date_time_obj
    endtime=date_time_obj+timedelta(seconds=3600)
    date_time_obj = date_time_obj + timedelta(seconds=3600)
    print(starttime.strftime('%Y-%m-%dT%H:00:00.000Z') + " " + endtime.strftime('%Y-%m-%dT%H:59:59.999Z'))'''
