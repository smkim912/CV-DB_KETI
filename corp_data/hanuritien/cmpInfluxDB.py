# cmpInfluxDB.py
# coding=utf-8
import time
from influxdb import InfluxDBClient

class InfluxDB :
	m_conn = None
	
	def __init__(self) :
		pass

	def open(self) :
		bConti = True
		while bConti :
			try :
				self.m_dConn = InfluxDBClient('127.0.0.1', 8086, '', '', 'hanuritien')
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
	
	def __init__(self) :
		self.m_oDBConn = InfluxDB() 
		self.m_oDBConn.open()

	def insert(self, nID, nTime, nDay_km, nTotal_km, nGPS_meter, nSpeed, nRPM, nGPS_latitude, nGPS_longitude, nGPS_azimuth, nAccel_x, nAccel_y, nDay_fuel, nTotal_fuel, nStatus, nSig_break) :
		json_body = [
			{
			    "measurement": nID,
			    "fields": {
                                "Daily_mileage"     :   nDay_km,
                                "Total_mileage"     :   nTotal_km,
                                "GPS_mileage"       :   nGPS_meter,
                                "Speed"             :   nSpeed,
                                "RPM"               :   nRPM,
                                "GPS_latitude"      :   nGPS_latitude,
                                "GPS_longitude"     :   nGPS_longitude,
                                "GPS_azimuth"       :   nGPS_azimuth,
                                "X-Acceleration"    :   nAccel_x,
                                "Y-Acceleration"    :   nAccel_y,
                                "Daily_fuel"        :   nDay_fuel,
                                "Total_uel"         :   nTotal_fuel,
                                "Status_code"       :   nStatus,
                                "Break signal"      :   nSig_break
			    },
                            "time": nTime
			}
		]
		self.m_oDBConn.insertData(json_body)
		return 0
