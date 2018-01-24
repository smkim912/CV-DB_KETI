# cmpInfluxDB.py
# coding=utf-8
import time
import sys
from influxdb import InfluxDBClient

BATCH_WM = 1024 * 1024 * 3

class InfluxDB :
	m_conn = None
	
	def __init__(self) :
		pass

	def open(self, dbname) :
		bConti = True
		while bConti :
			try :
				self.m_dConn = InfluxDBClient('127.0.0.1', 8086, '', '', dbname)
				bConti = False
			except Exception, e :
#time.sleep(CmpGlobal.g_nConnectionRetryInterval)
				time.sleep(5)
				print "---------------------- influxdb connect fail ------------------------"

	def insertData(self, jsondata) :
		#print("Write points: {0}".format(jsondata))
		self.m_dConn.write_points(jsondata)



class InfluxDBManager :
	m_oDBConn = None
        json_body = []
	
	def __init__(self,dbname) :
	    self.m_oDBConn = InfluxDB() 
	    self.m_oDBConn.open(dbname)

	def insert(self, nTable, nID, nTime, nMetric) :
	    new_json = {
	        "measurement": nTable,
                "tags": {
                    "Set_ID" : nID
                },
		"fields": nMetric,
                "time": nTime
	    }
            if sys.getsizeof(self.json_body) + sys.getsizeof(new_json) > BATCH_WM:
                self.m_oDBConn.insertData(self.json_body)
                del self.json_body[:]
            self.json_body.append(new_json)
	    #self.m_oDBConn.insertData(json_body)
	    return 0
