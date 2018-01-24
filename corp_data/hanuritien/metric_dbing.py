#!/usr/bin/python
#-*- coding: utf-8 -*-
# Author : Seongmin Kim, https://github.com/smkim912

import time
import json
import csv
import os
import sys
import time
from datetime import datetime
from cmpInfluxDB import InfluxDBManager
import subprocess
import hati_metrics

VERSION = '0.6.180124'
DATA_PATH = './data/hanuritien'
DB_NAME = 'hatidb'
try:
    TABLE_NAME = sys.argv[1]
    METRIC_IDX = hati_metrics.metrics.index(TABLE_NAME)
except (ValueError,IndexError):
    print "Usage: #python metric_dbing.py METRIC_NAME"
    sys.exit(1)

print 'Run a insertion program (hanuritien CSV to InfluxDB)'
print 'Version: ' + VERSION

date = datetime.today().strftime("%Y%m%d_%H%M%S")
date = date + "-" + DB_NAME + "-" + TABLE_NAME + "-log"
log = open(date, 'w')

g_influxdbconn = InfluxDBManager(DB_NAME)

def csv_to_influx(path):
    reader = csv.reader(open(path))
    row_cnt = 0
    for row in reader:
        if row_cnt == 0:
            row_cnt += 1
            continue    #title row
        try:
            ts = time.mktime(time.strptime(row[1], '%Y-%m-%d %H:%M:%S'))
            nID         = row[0]
            nTime       = int(ts) * 1000000000
            nField      = {TABLE_NAME:row[METRIC_IDX]}
            g_influxdbconn.insert(TABLE_NAME,nID,nTime,nField)
            row_cnt += 1
        except (ValueError,UnicodeDecodeError) as e:
            print e
            log.write(e + "\n")
            return -1
    print path + ": 'csv_to_influx()' complete (%d-rows)" %(row_cnt)
    log.write(path + ": 'csv_to_influx()' complete (%d-rows)" %(row_cnt) + "\n")
    return 0


if __name__ == "__main__":
    #Statistics variables
    total_file_cnt = int(subprocess.check_output("find ./data/hanuritien -type f | wc -l", shell=True))
    file_cnt = 0
    #dir_cnt = 0
    total_time_cnt = 0

    print 
    for (path, dir, files) in os.walk(DATA_PATH):
        #dir_cnt += 1        
        start_time = time.time()
        for filename in files:
            ext = os.path.splitext(filename)[-1]
            if ext == '.csv':
                file_cnt += 1
                dir_ret_code = csv_to_influx("%s/%s" % (path, filename))
                if dir_ret_code == -1:
                    print "%s/%s" % (path, filename) + " has error."
                    log.write("%s/%s" % (path, filename) + " has error." + "\n")
        end_time = time.time()
        ctime = end_time - start_time
        total_time_cnt += ctime
        print path + ": dir time took %d" %(ctime) + "s. (total: %d)" %(total_time_cnt)
        log.write(path + ": dir time took %d" %(ctime) + "s. (total: %d)" %(total_time_cnt) + "\n")
        print "Progress...[%d/%d(%d%%)]" % (file_cnt, 
                total_file_cnt, 
                (float(file_cnt)/float(total_file_cnt))*100)
        log.write("Progress...[%d/%d(%d%%)]" % (file_cnt, 
                total_file_cnt, 
                (float(file_cnt)/float(total_file_cnt))*100) + "\n")

    log.close()
