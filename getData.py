from datetime import datetime, timedelta
import urllib2
from ftplib import FTP
import MySQLdb
import _mysql_exceptions
import GRIBparser
import logging
from time import sleep

logger = logging.getLogger(__name__)

def roundHalf(n):
    return str(round(n * 2) / 2)

def pad3(s):
    retval = "000"+str(s)
    return retval[-3:]

def updateDataset(valueTime, resolution):
    resolutionStr = str(resolution)
    if resolution == 0.5:
        resolutionStr = '0.50'
    #Table name format: gfsYYYYMMDDHH (of prediction)
    db=MySQLdb.connect(host='localhost', user='guest', passwd='',db="test_dataset")
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
            db.commit()
        else:
            raise
    #Check if desired value time already exists in table 
    c.execute("SELECT COUNT(*) FROM %s WHERE ValueTime=%%s AND Resolution='%s'"%(presumptiveTableName, resolutionStr), (valueTime,))
    if c.fetchone()[0] >0:
        logger.info("Data already exists in table %s for prediction time %s. Update not needed."%(presumptiveTableName, valueTime.strftime('%Y%m%d%H')))
        return
    else:
        logger.info("Getting new data...")
        url = generateURL(36, 33, -121, -116, valueTime, latestAvailable, resolution)
        logger.debug(url)
        logger.info("Downloading GRIB...")
        data = downloadFile(url)
        logger.info("Download complete.")
        logger.info("Parsing GRIB...")
        processedData = GRIBparser.parseGRIBdata(data)
        logger.info("Parsing complete.")
        tupleCount = 0
        try:
            for grib in processedData:
                tupleCount += len(grib[1])
        except:
            errorfile = open("/var/log/weathererror.log", 'w')
            errorfile.write(data)
            errorfile.close()
            raise
        logger.info("Processing a total of %d tuples..."%tupleCount)
        for i, grib in enumerate(processedData):
            logger.debug("Ingesting data from GRIB %d of %d..."%(i, len(processedData)))
            metadata = grib[0]
            tuples = grib[1]
            for tuple in tuples:
                sqlString = "INSERT INTO %s (ValueTime, Resolution, Latitude, Longitude, Isobar, %s) VALUES ('%s', '%s', %s, %s, %s, %s) ON DUPLICATE KEY UPDATE %s=%s"%(presumptiveTableName, metadata["param"], metadata["valuetime"].strftime('%Y-%m-%d %H:%M:%S'), resolutionStr, tuple[0], tuple[1], metadata["isobar"], tuple[2], metadata["param"], tuple[2])
                c.execute(sqlString)
        db.commit()
    return
    
def findLatestPrediction():
    #return datetime(2019, 7, 11, 06)
    currentTime = datetime.utcnow()
    mostRecentCycle = (currentTime.hour//6)*6
    ftp = FTP('ftp.ncep.noaa.gov')
    ftp.login()
    ftp.cwd('/pub/data/nccf/com/gfs/prod') 
    results = []
    retval = ftp.retrlines('NLST', results.append)
    dirName = "gfs.%s%s"%(currentTime.strftime('%Y%m%d'), str(mostRecentCycle).zfill(2))
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

#0.50 forecasts: data for 3-hour intervals to 240 hours out, 12-hour intervals for 240 to 384 hours out. 
#0.25 forecasts: data at 1-hour intervals to 120 hours out, 3-hour intervals for 120 to 240 hours out, 12-hour intervals for 240 to 384 hours out.
def coerceForecastHour(forecastHoursIdeal, resolution):
    if resolution == 0.25:
        if forecastHoursIdeal <= 120:
            return forecastHoursIdeal
        elif forecastHoursIdeal > 120 and forecastHoursIdeal <= 240:
            return int(3 * round(float(forecastHoursIdeal)/3))
        elif forecastHoursIdeal <= 384:
            return int(12 * round(float(forecastHoursIdeal)/12))
        else:
            return 384
    elif resolution == 0.5:
        if forecastHoursIdeal <= 240:
            return int(3 * round(float(forecastHoursIdeal)/3))
        elif forecastHoursIdeal <= 384:
            return int(12 * round(float(forecastHoursIdeal)/12))
        else:
            return 384

#This function assumes that predictionTime correctly falls on a forecast cycle.
def generateURL(latTop, latBot, longLeft, longRight, dataTime, predictionTime, resolution):
    latTop = str(latTop)
    latBot = str(latBot)
    longLeft = str(longLeft)
    longRight = str(longRight)
    forecastCycle = predictionTime.strftime('%Y%m%d%%2F%H')
    forecastCycleHour = predictionTime.strftime('%H')
    forecastDelta= dataTime - predictionTime
    forecastHoursIdeal = forecastDelta.days*24+forecastDelta.seconds//3600
    forecastHour = coerceForecastHour(forecastHoursIdeal, resolution)
                
    if resolution == 0.25:
        bigURL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t"
    elif resolution == 0.5:
        bigURL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl?file=gfs.t"
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
    attemptCount = 0
    html = None
    while(attemptCount < 20):
        print "Attempt:", attemptCount
        attemptCount += 1
        try:
            response = urllib2.urlopen(url)
            html = response.read()
            print "html?:", (html is not None)
        except Exception as e:
            if str(e) == "HTTP Error 404: data file not present":
                print "File not present. Waiting and retrying..."
                sleep(30)
                continue
            else:
                raise
        break
    return html

def getAllLatestData():
    latestPrediction = findLatestPrediction()
    checkedHours = []
    for i in range (1, 385):
        forecastHour = coerceForecastHour(i, 0.50)
        if forecastHour not in checkedHours:
            valueTime = latestPrediction+timedelta(hours=forecastHour)
            updateDataset(valueTime, 0.50)
            checkedHours.append(forecastHour)
        else:
            logger.debug("Skipping %d..."%i)
    #checkedHours = []
    #for i in range (1, 385):
    #    forecastHour = coerceForecastHour(i, 0.25)
    #    if forecastHour not in checkedHours:
    #        valueTime = latestPrediction+timedelta(hours=forecastHour)
    #        updateDataset(valueTime, 0.25)
    #        checkedHours.append(forecastHour)
    #    else:
    #        logger.info("Skipping %d..."%i)
        
if __name__=="__main__":
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    #downloadGrib(34, -118, 20)
    #predictionTime = findLatestPrediction()
    #dataTime = datetime.strptime('2017072221', '%Y%m%d%H')
    #print findLatestPrediction()
    #url = generateURL(35, 33.5, -120, -117, dataTime, predictionTime, 0.25)
    #print url 
    #print type(downloadFile(url))
    #updateDataset(dataTime)
    logger.info("Beginning data ingest at %s..."%datetime.now().strftime('%Y%m%d %H%M%S'))
    getAllLatestData()
    logger.info("Completed data ingest at %s..."%datetime.now().strftime('%Y%m%d %H%M%S'))
    
