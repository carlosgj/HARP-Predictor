#import MySQLdb
import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()

from datetime import datetime
from collections import defaultdict
import logging
logger = logging.getLogger(__name__)

class AnalysisEngine():

    def __init__(self):
        self.wdb=MySQLdb.connect(host='weatherdata.kf5nte.org', user='readonly', passwd='',db="test_dataset")
        self.edb=MySQLdb.connect(host='elevationdata.kf5nte.org', user='readonly', passwd='',db="elevationdata")
        self.wc = self.wdb.cursor()
        self.ec = self.edb.cursor()
        self.latestPrediction = None
        self.MBTables = {}
        #print self.db
        #print self.c

    def getLatestPrediction(self):
        if self.latestPrediction is not None:
            return self.latestPrediction
        self.wc.execute("SELECT PredictionTime FROM WeatherData ORDER BY PredictionTime DESC LIMIT 1")
        raw = self.wc.fetchall()
        predictionTime = raw[0][0]
        self.latestPrediction = predictionTime.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("Using prediction %s"%self.latestPrediction)
        return self.latestPrediction

    def getHGTsforExact(self, latitude, longitude, resolution, time):
        resStr = "0.50"
        if resolution==0.25:
            resStr = '0.25'
        self.wc.execute("""SELECT Isobar, HGT from WeatherData WHERE PredictionTime = "%s" AND Latitude=%f AND Longitude=%f AND Resolution= '%s' AND ValueTime='%s'"""%(self.getLatestPrediction(), latitude, longitude, resStr, time.strftime('%Y-%m-%d %H:%M:%S')))
        results =  self.wc.fetchall()
        return results

    def buildMBTable(self, latitude, longitude, time):
        if (latitude, longitude, time) in self.MBTables:
            return self.MBTables[(latitude, longitude, time)]
        #print latitude, longitude, time.strftime('%Y%m%d %H:%M:%S')
        #res25 = getHGTsforExact(latitude, longitude, 0.25, time)
        res50 = self.getHGTsforExact(latitude, longitude, 0.50, time)
        #if not res25 and not res50:
        if not res50:
            logger.critical("Could not find HGTs for latitude %f, longitude %f, at %s. Balloon may be out of database limits."%(latitude, longitude, time.strftime('%Y-%m-%d %H:%M:%S')))
        res50Table = {}
        for i in res50:
            res50Table[i[0]] = i[1]
        #res25Table = {}
        #for i in res25:
        #    res25Table[i[0]] = i[1]
        allisobars = set(res50Table.keys())
        #allisobars = allisobars.union(set(res25Table.keys()))
        finalTable = {}
        for isobar in allisobars:
            val = None
            #if isobar in res25Table:
            #    val = res25Table[isobar]
            if isobar in res50Table:
                if val:
                    val = (val+res50Table[isobar])/2.
                else:
                    val = res50Table[isobar]
            finalTable[val] = isobar
        self.MBTables[(latitude, longitude, time)] = finalTable
        return finalTable

    def bracketElevation(self, latitude, longitude, time, elevation):
        table = self.buildMBTable(latitude, longitude, time)
        retDict = {
            'lowerIsobar':None,
            'lowerElevation':0,
            'upperIsobar':None, 
            'upperElevation':100000
            }
        lowerIsobar = None
        upperIsobar = None
        lowerElevation = 0
        upperElevation = 1000000
        if not table:
            return retDict
        sortedKeys = sorted(table.keys())
        if elevation < sortedKeys[0]:
            retDict['lowerElevation'] = retDict['upperElevation'] = sortedKeys[0]
            retDict['lowerIsobar'] = retDict['upperIsobar'] = table[sortedKeys[0]]
            logger.warning("Elevation %f is lower than minimum isobar elevation %f. Assuming %f."%(elevation, sortedKeys[0], sortedKeys[0]))
        for idx, hgt in enumerate(sortedKeys):
            if  hgt < elevation:
                retDict['lowerElevation']=hgt
                retDict['lowerIsobar'] = table[hgt]
            elif hgt > elevation:
                retDict['upperElevation'] = hgt
                retDict['upperIsobar'] = table[hgt]
                break
        return retDict

    def getZPlaneAverage(self, latitude, longitude, time, elevation):
        points = self.bracketElevation(latitude, longitude, time, elevation)
        if (not points['lowerIsobar']) or (not points['upperIsobar']):
            print latitude, longitude, time, elevation, points
            print "YOU NEED TO IMPLEMENT ERROR HANDLING MOTHERFUCKER"
            raise
            #TODO: error handling
        try:
            upperWeightingFactor = (float(elevation)-points['lowerElevation'])/(points['upperElevation']-points['lowerElevation'])
            lowerWeightingFactor = (points['upperElevation']-float(elevation))/(points['upperElevation']-points['lowerElevation'])
        except ZeroDivisionError:
            upperWeightingFactor = lowerWeightingFactor = 0.5
        meanPressure = upperWeightingFactor*points['upperIsobar']+lowerWeightingFactor*points['lowerIsobar']

        #Get weather data at lower point
        try:
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime="%s" AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.getLatestPrediction(), time.strftime("'%Y-%m-%d %H:%M:%S'"), latitude, longitude, points['lowerIsobar']))
        except:
            print "Regenerating cursor..."
            self.wc.close()
            self.wc = self.db.cursor()
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime="%s" AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.getLatestPrediction(), time.strftime("'%Y-%m-%d %H:%M:%S'"), latitude, longitude, points['lowerIsobar']))
        raw = self.wc.fetchall()
        lowerData = []
        for row in raw:
            lowerData.append({'Res':row[0], 'TMP':row[1], 'UGRD':row[2], 'VGRD':row[3]})

        #Get weather data at upper point
        try:
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime="%s" AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.getLatestPrediction(), time.strftime("'%Y-%m-%d %H:%M:%S'"), latitude, longitude, points['upperIsobar']))
        except:
            print "Regenerating cursor..."
            self.wc.close()
            self.wc = self.db.cursor()
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime="%s" AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.getLatestPrediction(), time.strftime("'%Y-%m-%d %H:%M:%S'"), latitude, longitude, points['upperIsobar']))

        raw = self.wc.fetchall()
        upperData = []
        for row in raw:
            upperData.append({'Res':row[0], 'TMP':row[1], 'UGRD':row[2], 'VGRD':row[3]})
    
        #Sanity check; each point should have at most one row from 0.25 data and one row from 0.50 data
        assert len([x for x in lowerData if x['Res']=='0.25']) <= 1
        assert len([x for x in lowerData if x['Res']=='0.50']) <= 1
        assert len([x for x in upperData if x['Res']=='0.25']) <= 1
        assert len([x for x in upperData if x['Res']=='0.50']) <= 1
    
        finalUpperData = {'TMP':0, 'UGRD':0, 'VGRD':0}
        for datum in upperData:
            for key in finalUpperData:
                finalUpperData[key] += datum[key]
        for i in finalUpperData:
            finalUpperData[i] /= float(len(upperData))
        
        finalLowerData = {'TMP':0, 'UGRD':0, 'VGRD':0}
        for datum in lowerData:
            for key in finalLowerData:
                finalLowerData[key] += datum[key]
        for i in finalLowerData:
            finalLowerData[i] /= float(len(lowerData))
        
        finalData = {'TMP':None, 'UGRD':None, 'VGRD':None}
        for key in finalData:
            finalData[key] = finalUpperData[key]*upperWeightingFactor+finalLowerData[key]*lowerWeightingFactor
        finalData['Pressure'] = meanPressure
        return finalData

    def findLatLongPoints(self, latitude, longitude):
        firstLatPoint = round(2*latitude)/2.
        if firstLatPoint < latitude:
            secondLatPoint = firstLatPoint + 0.5
        elif firstLatPoint > latitude:
            secondLatPoint = firstLatPoint - 0.5
        else:
            secondLatPoint = firstLatPoint
    
        firstLongPoint = round(2*longitude)/2.
        if firstLongPoint < longitude:
            secondLongPoint = firstLongPoint + 0.5
        elif firstLongPoint > longitude:
            secondLongPoint = firstLongPoint - 0.5
        else:
            secondLongPoint = firstLongPoint
        return (firstLatPoint, secondLatPoint), (firstLongPoint, secondLongPoint)

    def getWeatherDataAtPointAtPredictionTime(self, latitude, longitude, time, elevation):
        #First, find 4 nearest lat/lon points
        lats, longs = self.findLatLongPoints(latitude, longitude)
        minLat = min(lats)
        maxLat = max(lats)
        minLong = min(longs)
        maxLong = max(longs)
        #print minLat, maxLat, minLong, maxLong
        #Next, interpolate weather data at specified elevation at each point
        ZPlaneData = {}
        ZPlaneData['-x-y'] = self.getZPlaneAverage(minLat, minLong, time, elevation)
        ZPlaneData['-x-y'].update(Latitude=minLat, Longitude=minLong)
        ZPlaneData['-x+y'] = self.getZPlaneAverage(maxLat, minLong, time, elevation)
        ZPlaneData['-x+y'].update({'Latitude':maxLat, 'Longitude':minLong})
        ZPlaneData['+x-y'] = self.getZPlaneAverage(minLat, maxLong, time, elevation)
        ZPlaneData['+x-y'].update({'Latitude':minLat, 'Longitude':maxLong})
        ZPlaneData['+x+y'] = self.getZPlaneAverage(maxLat, maxLong, time, elevation)
        ZPlaneData['+x+y'].update({'Latitude':maxLat, 'Longitude':maxLong})
        posXWeightingFactor = (latitude - minLat) / (maxLat - minLat)
        negXWeightingFactor = (maxLat - latitude) / (maxLat - minLat)
        posYWeightingFactor = (longitude - minLong) / (maxLong - minLong)
        negYWeightingFactor = (maxLong - longitude) / (maxLong - minLong)
    
        negXPointData = {'TMP':None, 'UGRD':None, 'VGRD':None, 'Pressure':None}
        for key in negXPointData:
            negXPointData[key] = (ZPlaneData['-x-y'][key]*negYWeightingFactor) + (ZPlaneData['-x+y'][key]*posYWeightingFactor)
        posXPointData = {'TMP':None, 'UGRD':None, 'VGRD':None, 'Pressure':None}
        for key in posXPointData:
            posXPointData[key] = (ZPlaneData['+x-y'][key]*negYWeightingFactor) + (ZPlaneData['+x+y'][key]*posYWeightingFactor)
        #print negXPointData, posXPointData
    
        finalData = {'TMP':None, 'UGRD':None, 'VGRD':None, 'Pressure':None}
        for key in finalData:
            finalData[key] = (negXPointData[key]*negXWeightingFactor) + (posXPointData[key]*posXWeightingFactor)
        return finalData
    
    def getWeatherDataInterpolated(self, latitude, longitude, time, elevation):
        #Bracket time
        pred=self.getLatestPrediction()
        self.wc.execute("""SELECT DISTINCT(ValueTime) FROM WeatherData WHERE PredictionTime=\"%s\""""%pred)
        availableTimes = self.wc.fetchall()
        availableTimes = [x[0] for x in availableTimes]
        beforeTime = min(availableTimes)
        afterTime = max(availableTimes)
        assert beforeTime <= time
        assert afterTime >= time
        for predtime in availableTimes:
            if predtime > beforeTime and predtime < time:
                beforeTime = predtime
            if predtime < afterTime and predtime > time:
                afterTime = predtime
        beforeWeather = self.getWeatherDataAtPointAtPredictionTime(latitude, longitude, beforeTime, elevation)
        afterWeather = self.getWeatherDataAtPointAtPredictionTime(latitude, longitude, afterTime, elevation)
        beforeWeightingFactor = (afterTime-time).total_seconds()/(afterTime-beforeTime).total_seconds()
        afterWeightingFactor = (time-beforeTime).total_seconds() / (afterTime-beforeTime).total_seconds()
        finalData = {}
        for key in beforeWeather:
            finalData[key] = beforeWeather[key]*beforeWeightingFactor + afterWeather[key]*afterWeightingFactor
        return finalData

    def linterp(self, x, x1, x2, y1, y2):
        return ((x2 - x) / (x2 - x1)) * y1 + ((x - x1) / (x2 - x1)) * y2

    def getAltitudePoints(self, latitude, longitude):
        class point():
            latitude = None
            longitude = None
            elevation = None
            def __init__(self, lat, lon, ele):
                self.latitude = lat
                self.longitude = lon
                self.elevation = ele

        #self.c.execute("""USE Django""")
        self.ec.execute("""SELECT latitude, longitude, elevation FROM Predictor_elevationpoint WHERE latitude BETWEEN %f AND %f AND longitude BETWEEN %f AND %f"""%(latitude-0.0001, latitude+0.0001, longitude-0.0001, longitude+0.0001))
        results = self.ec.fetchall()
        #self.c.execute("""USE test_dataset""")
        if not results:
            logger.critical("Could not find altitude data for latitude %f, longitude %f. Balloon may be out of database limits."%(latitude, longitude))
        results = [point(x[0], x[1], x[2]) for x in results]
        return results

    def getAltitudeAtPoint(self, latitude, longitude):
        approximatePoints = self.getAltitudePoints(latitude, longitude)
        lowerLat = -1
        upperLat = 90
        lowerLon = -360
        upperLon = 361
        exactLat = None
        exactLon = None
        #print results
        for result in approximatePoints:
            if result.latitude == latitude and result.longitude == longitude:
                return result.elevation
            elif result.latitude == latitude:
                exactLat = result.latitude
            elif result.longitude == longitude:
                exactLon = result.longitude
            if result.latitude < latitude and result.latitude > lowerLat:
                lowerLat = result.latitude
            if result.latitude > latitude and result.latitude < upperLat:
                upperLat = result.latitude
            if result.longitude < longitude and result.longitude > lowerLon:
                lowerLon = result.longitude
            if result.longitude > longitude and result.longitude < upperLon:
                upperLon = result.longitude

        if exactLat:
            xpoints = [x for x in approximatePoints if x.latitude==exactLat and (x.longitude == lowerLon or x.longitude == upperLon)]
            assert len(xpoints) == 2
            return self.linterp(longitude, xpoints[0].longitude, xpoints[1].longitude, xpoints[0].elevation, xpoints[1].elevation)
        elif exactLon:
            ypoints = [x for x in approximatePoints if x.longitude==exactLon and (x.latitude == lowerLat or x.latitude == upperLat)]
            assert len(ypoints) == 2
            return self.linterp(latitude, ypoints[0].latitude, ypoints[1].latitude, ypoints[0].elevation, ypoints[1].elevation)
        else:
            ypoints1 = [x for x in approximatePoints if x.longitude==upperLon and (x.latitude == lowerLat or x.latitude == upperLat)]
            ypoints2 = [x for x in approximatePoints if x.longitude==lowerLon and (x.latitude == lowerLat or x.latitude == upperLat)]
            try:
                assert len(ypoints1) == 2
                assert len(ypoints2) == 2
            except:
                for pt in approximatePoints:
                    print pt.latitude, pt.longitude
                print "ypoints1:", ypoints1, " ypoints2:", ypoints2
                print latitude, longitude
                print lowerLat, upperLat, lowerLon, upperLon
                raise
            xpt1 = self.linterp(latitude, ypoints1[0].latitude, ypoints1[1].latitude, ypoints1[0].elevation, ypoints1[1].elevation)
            xpt2 = self.linterp(latitude, ypoints2[0].latitude, ypoints2[1].latitude, ypoints2[0].elevation, ypoints2[1].elevation)
            return self.linterp(longitude, lowerLon, upperLon, xpt2, xpt1)


    
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

#    print getAltitudeAtPoint(34.5, -118.5)
#    print getWeatherDataInterpolated(34.2,-118.6, datetime.strptime('201707251230', '%Y%m%d%H%M'), 12000)
    getZPlaneAverage(34.0, -118.0, datetime.strptime("2017-12-12 18:00:00", "%Y-%m-%d %H:%M:%S"),  235.915192451)
