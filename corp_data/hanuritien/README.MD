### version 0.6
 - new files
   remove: 'dbing.py', 'cmpInfluxDB.py'(old), 'speed_dbing.py', 'speed_cmpInfluxDB.py'
   add: metric_dbing.py: main procedure based on dynamic metric
   add: hati_metrics.py: metric name list
   add: cmpInfluxDB.py: based on dynamic metric
 - sys.argu => metric db table 
   Usage: #python metric_dbing.py METRIC_NAME
   e.g., #python metric_dbing.py Speed
 - log file name += metric name (minor)
 - printing progress bug fix (minor)

### version 0.5
 - timestamp bug fix (minor)

### version 0.4
 - batch insertion (3MiB-unit)
 - One metric table => {"time: timestamp","tags: set_id","fields: speed"}
 - new file name => speed_dbing.py / speed_cmpInfluxDB.py

### version 0.3
 - batch insertion (file-unit)

### version 0.2
 - only one table & "Set_ID" move to 'tags'
 - log file
 - too slow (because one by one insertion)
 
### version 0.1
 - dbing.py : main procedure
 - cmpInfluxDB.py : insert function for the influxdb

### CSV 파일
- 기간 : 6개월, 582~583개 자동차
- 파일 갯수 : 108,114 개 ( find ./ -name *.csv | wc -l )
- 라인 수 : 2,003,069,431 라인
  
