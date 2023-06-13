import logging
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import time

logger = logging.getLogger("JoinDatabases")

db = MySQLdb.connect(host='iris-storage', user='weatheringest', passwd='', db='Weather')

c = db.cursor()

c.execute('''SELECT tableName, predictionTime FROM setIndex''')

raw_idx = c.fetchall()

index = {}
for foo in raw_idx:
	index[foo[0]] = foo[1]

print(index)
i = 0
for timeStr, predTime in index.items():
	logger.info("Processing %s (%d of %d)..."%(timeStr, i, len(index)))
	c.execute("""DROP TEMPORARY TABLE IF EXISTS tmp""")
	c.execute("""CREATE TEMPORARY TABLE tmp SELECT * FROM %s"""%timeStr) 
	c.execute("""ALTER TABLE tmp ADD PredictionTime datetime""")
	c.execute("""UPDATE tmp SET PredictionTime='%s'"""%predTime.strftime('%Y-%m-%d %H:%M:%S'))
	c.execute("""INSERT IGNORE INTO WeatherData (PredictionTime, ValueTime, Resolution, Latitude, Longitude, Isobar, HGT, TMP, UGRD, VGRD) SELECT PredictionTime, ValueTime, Resolution, Latitude, Longitude, Isobar, HGT, TMP, UGRD, VGRD FROM tmp""")
	c.execute("""DROP TEMPORARY TABLE IF EXISTS tmp""")
	c.execute("""DROP TABLE %s"""%timeStr)
	c.execute("""DELETE FROM setIndex WHERE tableName = '%s'"""%timeStr)
	i +=1
db.commit()
