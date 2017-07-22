from datetime import datetime, timedelta
import urllib2
from ftplib import FTP
import MySQLdb
import _mysql_exceptions
import GRIBparser
import logging

logger = logging.getLogger(__name__)

def roundHalf(n):
    return str(round(n * 2) / 2)

def pad3(s):
    retval = "000"+str(s)
    return retval[-3:]

def updateDataset(valueTime):
    #Table name format: gfsYYYYMMDDHH (of prediction)
    db=MySQLdb.connect(host='delta.carlosgj.org', user='guest', passwd='',db="test_dataset")
    c = db.cursor()
    c.execute("USE test_dataset")
    #Get time of latest NOMADS prediction
    latestAvailable = findLatestPrediction()
    logger.info("Most recent prediction: %s"%latestAvailable.strftime('%Y%m%d%H'))
    presumptiveTableName = 'gfs'+latestAvailable.strftime('%Y%m%d%H')
    #print "Table should be called:", presumptiveTableName
    try:
        c.execute("SELECT 1 FROM %s LIMIT 1"%presumptiveTableName)
    except _mysql_exceptions.ProgrammingError as e:
        if e[0] == 1146:
            logger.info("Prediction table does not exist. Creating.")
            c.execute("CREATE TABLE %s (\
                ValueTime DATETIME, \
                Resolution ENUM('0.25', '0.50'), \
                Latitude FLOAT, \
                Longitude FLOAT, \
                Isobar SMALLINT, \
                HGT MEDIUMINT, \
                TMP FLOAT, \
                UGRD FLOAT, \
                VGRD FLOAT, \
                PRIMARY KEY (ValueTime, Resolution, Latitude, Longitude, Isobar) \
                )"%presumptiveTableName)
            c.execute("INSERT INTO setIndex (tableName, predictionTime) VALUES (%s, %s)",(presumptiveTableName, latestAvailable))
        else:
            raise
    #Check if desired value time already exists in table 
    c.execute("SELECT COUNT(*) FROM %s WHERE ValueTime=%%s"%presumptiveTableName, (valueTime,))
    if c.fetchone()[0] >0:
        logger.info("Data already exists in table %s for prediction time %s. Update not needed."%(presumptiveTableName, valueTime.strftime('%Y%m%d%H')))
        return
    else:
        logger.info("Getting new data...")
        url = generateURL(35, 33.5, -120, -117, valueTime, latestAvailable, 0.25)
        logger.debug(url)
        logger.info("Downloading GRIB...")
        data = downloadFile(url)
        logger.info("Download complete.")
        logger.info("Parsing GRIB...")
        processedData = GRIBparser.parseGRIBdata(data)
        logger.info("Parsing complete.")
        tupleCount = 0
        for grib in processedData:
            tupleCount += len(grib[1])
        logger.info("Processing a total of %d tuples..."%tupleCount)
        for i, grib in enumerate(processedData):
            logger.debug("Ingesting data from GRIB %d of %d..."%(i, len(processedData)))
            metadata = grib[0]
            tuples = grib[1]
            for tuple in tuples:
                sqlString = "INSERT INTO %s (ValueTime, Resolution, Latitude, Longitude, Isobar, %s) VALUES ('%s', '0.25', %s, %s, %s, %s) ON DUPLICATE KEY UPDATE %s=%s"%(presumptiveTableName, metadata["param"], metadata["valuetime"].strftime('%Y-%m-%d %H:%M:%S'), tuple[0], tuple[1], metadata["isobar"], tuple[2], metadata["param"], tuple[2])
                c.execute(sqlString)
        db.commit()

        url = generateURL(35, 33.5, -120, -117, valueTime, latestAvailable, 0.50)
        logger.debug(url)
        logger.info("Downloading GRIB...")
        data = downloadFile(url)
        logger.info("Download complete.")
        logger.info("Parsing GRIB...")
        processedData = GRIBparser.parseGRIBdata(data)
        logger.info("Parsing complete.")
        tupleCount = 0
        for grib in processedData:
            tupleCount += len(grib[1])
        logger.info("Processing a total of %d tuples..."%tupleCount)
        for i, grib in enumerate(processedData):
            logger.debug("Ingesting data from GRIB %d of %d..."%(i, len(processedData)))
            metadata = grib[0]
            tuples = grib[1]
            for tuple in tuples:
                sqlString = "INSERT INTO %s (ValueTime, Resolution, Latitude, Longitude, Isobar, %s) VALUES ('%s', '0.50', %s, %s, %s, %s) ON DUPLICATE KEY UPDATE %s=%s"%(presumptiveTableName, metadata["param"], metadata["valuetime"].strftime('%Y-%m-%d %H:%M:%S'), tuple[0], tuple[1], metadata["isobar"], tuple[2], metadata["param"], tuple[2])
                #print sqlString
                c.execute(sqlString)
        db.commit()


    return
    
def findLatestPrediction():
    currentTime = datetime.utcnow()
    mostRecentCycle = (currentTime.hour//6)*6
    ftp = FTP('ftp.ncep.noaa.gov')
    ftp.login()
    ftp.cwd('/pub/data/nccf/com/gfs/prod') 
    results = []
    retval = ftp.retrlines('NLST', results.append)
    dirName = "gfs.%s%s"%(currentTime.strftime('%Y%m%d'), str(mostRecentCycle))
    mostRecent = datetime.utcnow()
    mostRecent = mostRecent.replace(minute=0, second=0, microsecond=0)
    if dirName in results:
        mostRecent = mostRecent.replace(hour=mostRecentCycle)
        return mostRecent
    else:
        #we need to go back one
        if mostRecentCycle == 0: #Need to go to previous day
            mostRecent = mostRecent - timedelta(1) #subtract a day
            mostRecent = mostRecent.replace(hour=18)
            return mostRecent
        else:
            mostRecent = mostRecent.replace(hour=(mostRecentCycle - 6))
            return mostRecent
    
#This function assumes that predictionTime correctly falls on a forecast cycle.
#0.50 forecasts: data for 3-hour intervals to 240 hours out, 12-hour intervals thereafter
#0.25 forecasts: data for 1-hour intervals to 120 hours out, 3-hour intervals thereafter.
def generateURL(latTop, latBot, longLeft, longRight, dataTime, predictionTime, resolution):
    latTop = str(latTop)
    latBot = str(latBot)
    longLeft = str(longLeft)
    longRight = str(longRight)
    forecastCycle = predictionTime.strftime('%Y%m%d%H')
    forecastCycleHour = predictionTime.strftime('%H')
    forecastDelta= dataTime - predictionTime
    forecastHoursIdeal = forecastDelta.days*24+forecastDelta.seconds//3600
    if resolution == 0.25:
        if forecastHoursIdeal <= 120:
            forecastHour = forecastHoursIdeal
        else:
            forecastHour = int(3 * round(float(forecastHoursIdeal)/3))
    elif resolution == 0.5:
        if forecastHoursIdeal <= 240:
            forecastHour = int(3 * round(float(forecastHoursIdeal)/3))
        else:
            forecastHour = int(12 * round(float(forecastHoursIdeal)/12))
                
    if resolution == 0.25:
        bigURL = "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t"
    elif resolution == 0.5:
        bigURL = "http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?file=gfs.t"
    bigURL += forecastCycleHour
    if resolution == 0.25:
        bigURL += "z.pgrb2.0p25.f"
    elif resolution == 0.5:
        bigURL += "z.pgrb2full.0p50.f"
    bigURL += pad3(forecastHour)
    bigURL += "&lev_1_mb=on&lev_2_mb=on&lev_3_mb=on&lev_5_mb=on&lev_7_mb=on&";
    bigURL += "lev_10_mb=on&lev_20_mb=on&lev_30_mb=on&lev_50_mb=on&lev_70_mb=on&lev_100_mb=on&lev_150_mb=on&"
    bigURL += "lev_200_mb=on&lev_250_mb=on&lev_300_mb=on&lev_350_mb=on&lev_400_mb=on&lev_450_mb=on&lev_500_mb=on&"
    bigURL += "lev_550_mb=on&lev_600_mb=on&lev_650_mb=on&lev_700_mb=on&lev_750_mb=on&lev_800_mb=on&lev_850_mb=on&"
    bigURL += "lev_900_mb=on&lev_925_mb=on&lev_950_mb=on&lev_975_mb=on&lev_1000_mb=on&"
    if resolution == 0.5:
        bigURL += "lev_125_mb=on&lev_175_mb=on&lev_225_mb=on&lev_275_mb=on&lev_325_mb=on&lev_375_mb=on&lev_425_mb=on&"
        bigURL += "lev_475_mb=on&lev_525_mb=on&lev_575_mb=on&lev_625_mb=on&lev_675_mb=on&lev_725_mb=on&lev_775_mb=on&"
        bigURL += "lev_825_mb=on&lev_875_mb=on&"
    bigURL += "var_HGT=on&var_TMP=on&var_UGRD=on&var_VGRD=on&subregion=&leftlon="
    bigURL += longLeft;
    bigURL += "&rightlon=";
    bigURL += longRight;
    bigURL += "&toplat=";
    bigURL += latTop;
    bigURL += "&bottomlat=";
    bigURL += latBot;
    bigURL += "&dir=%2Fgfs.";
    bigURL += forecastCycle;
    return bigURL
    
def downloadFile(url):
    response = urllib2.urlopen(url)
    html = response.read()
    return html

    
if __name__=="__main__":
    logger.setLevel(logging.DEBUG)
    parserLog = logging.getLogger("GRIBparser")
    parserLog.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    parserLog.addHandler(ch)
    #downloadGrib(34, -118, 20)
    #predictionTime = findLatestPrediction()
    dataTime = datetime.strptime('2017072221', '%Y%m%d%H')
    #print findLatestPrediction()
    #url = generateURL(35, 33.5, -120, -117, dataTime, predictionTime, 0.25)
    #print url 
    #print type(downloadFile(url))
    updateDataset(dataTime)
    
