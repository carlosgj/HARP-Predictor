#import MySQLdb
import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()

from datetime import datetime
from collections import defaultdict
import logging
logger = logging.getLogger(__name__)

"""
The basic purpose of this module is interpolation.
"""


class MissingDataException(Exception):
    pass

class AnalysisEngine():
    def __init__(self, predTime=None):
        self.wdb = None
        self.edb = None
        self.wc = None
        self.ec = None
        self.predTime = predTime
        self.MBTables = {}
        #print self.db
        #print self.c
        
    def start(self):
        self.wdb=MySQLdb.connect(host='weatherdata.kf5nte.org', user='readonly', passwd='',db="test_dataset")
        self.edb=MySQLdb.connect(host='elevationdata.kf5nte.org', user='readonly', passwd='',db="elevationdata")
        self.wc = self.wdb.cursor()
        self.ec = self.edb.cursor()
        
    def stop(self):
        self.wdb.close()
        self.edb.close()
    
    """
    Turns a datetime object into a SQL-compatable string (including quotes)
    @param time: The time
    @return: A string like 'YYYY-MM-DD HH:MM:SS'
    """
    def SQLTimeFmt(self, time):
        return time.strftime("'%Y-%m-%d %H:%M:%S'")
    
    """
    Gets the weather prediction runtime closest to the given time. 
    @param time: The target time
    @param below: Set to True to get the closest earler (i.e. less) prediction time. Returns closest later prediction time by default. 
    @param excludeExact: Set to True to get the _next_ earlier/later prediction if the given time exactly equals a prediction time. 
    @return: A datetime object
    """
    def getClosestPredictionTime(self, time, below=False, excludeExact=False):
        if excludeExact:
            if below:
                self.wc.execute("""SELECT PredictionTime FROM WeatherData WHERE PredictionTime < %s ORDER BY PredictionTime DESC LIMIT 1"""%self.SQLTimeFmt(time))
            else:
                self.wc.execute("""SELECT PredictionTime FROM WeatherData WHERE PredictionTime > %s ORDER BY PredictionTime ASC LIMIT 1"""%self.SQLTimeFmt(time))
        else:
            if below:
                self.wc.execute("""SELECT PredictionTime FROM WeatherData WHERE PredictionTime <= %s ORDER BY PredictionTime DESC LIMIT 1"""%self.SQLTimeFmt(time))
            else:
                self.wc.execute("""SELECT PredictionTime FROM WeatherData WHERE PredictionTime >= %s ORDER BY PredictionTime ASC LIMIT 1"""%self.SQLTimeFmt(time))
        return self.wc.fetchall()[0][0]
    
    """
    Gets an "HGT column" (i.e., isobar/HGT correspondences for a given lat/lon)
    @param latitude: Point latitude (not interpolated, must be an exact latitude in weather database)
    @param latitude: Point longitude (not interpolated, must be an exact longitude in weather database)
    @param resolution: Prediction resolution to use; either 0.25 or 0.5
    @param time: Value time for weather data (not interpolated, must be an exact time in weather database)
    @return A list of points [(<isobar>, <HGT>),...]
    """
    def getHGTsforExact(self, latitude, longitude, resolution, time):
        resStr = "0.50"
        if resolution==0.25:
            resStr = '0.25'
        self.wc.execute("""SELECT Isobar, HGT from WeatherData WHERE PredictionTime = %s AND Latitude=%f AND Longitude=%f AND Resolution= '%s' AND ValueTime=%s"""%(    self.SQLTimeFmt(self.predTime), latitude, longitude, resStr, self.SQLTimeFmt(time)))
        #print """SELECT Isobar, HGT from WeatherData WHERE PredictionTime = "%s" AND Latitude=%f AND Longitude=%f AND Resolution= '%s' AND ValueTime='%s'"""%(self.SQLTimeFmt(predTime), latitude, longitude, resStr, self.SQLTimeFmt(time))
        results =  self.wc.fetchall()
        return results

    """
    Gets a table that describes an "HGT column". On the first call for a given (lat, lon, time), actually queries the database.
    On subsequent calls for that column, returns an in-memory copy (to reduce redundant DB calls)
    @param latitude: Point latitude (not interpolated, must be an exact latitude in weather database)
    @param latitude: Point longitude (not interpolated, must be an exact longitude in weather database)
    @param time: Value time for weather data (not interpolated, must be an exact time in weather database)
    @return: A dict {<HGT>:<isobar>, ...}
    """
    def buildMBTable(self, latitude, longitude, time):
        if (latitude, longitude, time) in self.MBTables:
            return self.MBTables[(latitude, longitude, time)]
        #print latitude, longitude, time.strftime('%Y%m%d %H:%M:%S')
        #res25 = getHGTsforExact(latitude, longitude, 0.25, time)
        res50 = self.getHGTsforExact(latitude, longitude, 0.50, time)
        #if not res25 and not res50:
        if not res50:
            logger.critical("Could not find HGTs for latitude %f, longitude %f, at %s. Balloon may be out of weather database limits."%(latitude, longitude, time.strftime('%Y-%m-%d %H:%M:%S')))
            raise MissingDataException
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
    
    """
    This function finds the isobars (and corresponding HGT elevations) nearest above and nearest below a given elevation.
    @param latitude: Point latitude (not interpolated, must be an exact latitude in weather database)
    @param latitude: Point longitude (not interpolated, must be an exact longitude in weather database)
    @param time: Value time for weather data (not interpolated, must be an exact time in weather database)
    @param elevation: Elevation of the point
    @return: A dict {'lowerIsobar', 'lowerElevation', 'upperIsobar', 'upperElevation'}
    """
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
            retDict['lowerElevation'] = sortedKeys[0]
            retDict['upperElevation'] = sortedKeys[1]
            retDict['lowerIsobar'] = table[sortedKeys[0]]
            retDict['upperIsobar'] = table[sortedKeys[1]]
            logger.warning("Elevation %f is lower than minimum isobar elevation %f. Using lowest two elevations & isobars."%(elevation, sortedKeys[0]))
            return retDict
        for idx, hgt in enumerate(sortedKeys):
            if  hgt < elevation:
                retDict['lowerElevation']=hgt
                retDict['lowerIsobar'] = table[hgt]
            elif hgt > elevation:
                retDict['upperElevation'] = hgt
                retDict['upperIsobar'] = table[hgt]
                break
        return retDict

    """
    Gets weather data that is interpolated in elevation
    @param latitude: Point latitude (not interpolated, must be an exact latitude in weather database)
    @param latitude: Point longitude (not interpolated, must be an exact longitude in weather database)
    @param time: Value time for weather data (not interpolated, must be an exact time in weather database)
    @param elevation: Elevation of the point
    @return: A dict {'TMP', 'UGRD', 'VGRD', 'Pressure'}
    """
    def getZPlaneAverage(self, latitude, longitude, time, elevation):
        points = self.bracketElevation(latitude, longitude, time, elevation)
        if (not points['lowerIsobar']) or (not points['upperIsobar']):
            logger.critical(' '.join([str(latitude), str(longitude), str(time), str(elevation), str(points)]))
            logger.critical("YOU NEED TO IMPLEMENT ERROR HANDLING MOTHERFUCKER")
            raise
            #TODO: error handling
        try:
            upperWeightingFactor = (float(elevation)-points['lowerElevation'])/(points['upperElevation']-points['lowerElevation'])
            lowerWeightingFactor = (points['upperElevation']-float(elevation))/(points['upperElevation']-points['lowerElevation'])
        except ZeroDivisionError:
            upperWeightingFactor = lowerWeightingFactor = 0.5

        meanPressure = self.linterp(elevation, points['lowerElevation'], points['upperElevation'], points['lowerIsobar'], points['upperIsobar'])

        #Get weather data at lower point
        try:
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime=%s AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.SQLTimeFmt(self.predTime), self.SQLTimeFmt(time), latitude, longitude, points['lowerIsobar']))
        except:
            print "Regenerating cursor..."
            self.wc.close()
            self.wc = self.db.cursor()
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime=%s AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.SQLTimeFmt(self.predTime), self.SQLTimeFmt(time), latitude, longitude, points['lowerIsobar']))
        
        raw = self.wc.fetchall()
        lowerData = []
        for row in raw:
            lowerData.append({'Res':row[0], 'TMP':row[1], 'UGRD':row[2], 'VGRD':row[3]})

        #Get weather data at upper point
        try:
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime=%s AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.SQLTimeFmt(self.predTime), self.SQLTimeFmt(time), latitude, longitude, points['upperIsobar']))
        except:
            print "Regenerating cursor..."
            self.wc.close()
            self.wc = self.db.cursor()
            self.wc.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM WeatherData WHERE PredictionTime=%s AND ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(self.SQLTimeFmt(self.predTime), self.SQLTimeFmt(time), latitude, longitude, points['upperIsobar']))

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
            finalData[key] = self.linterp(elevation, points['lowerElevation'], points['upperElevation'], finalLowerData[key], finalUpperData[key])
        finalData['Pressure'] = meanPressure
        return finalData

    """
    Finds the four (lat, lon) points on a 0.5-degree grid that enclose the given (lat, lon) point
    E.g., if given (34.25, -118.7), will return (34.5, 34), (-118.5, -119)
    @param latitude: An arbirtary latitude
    @param longitude: An arbirtary longitude
    @return: (lat, lat), (lon, lon)
    """
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

    """
    Gets weather data at discrete value times (i.e., times which are guranteed to exist in the ValueTime column),
    but interpolated in elevation and lat/lon.
    @param latitude: The actual latitude of the point of interest
    @param longitude: The actual longitude of the point of interest
    @param elevation: The actual elevation of the point of interest
    @return: A dict {'TMP', 'UGRD', 'VGRD', 'Pressure'}
    """
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
    
        finalData = {'TMP':None, 'UGRD':None, 'VGRD':None, 'Pressure':None}
        for key in finalData:
            finalData[key] = self.linterpQuad(longitude, latitude, minLong, maxLong, minLat, maxLat, ZPlaneData['-x-y'][key], ZPlaneData['+x-y'][key], ZPlaneData['-x+y'][key], ZPlaneData['+x+y'][key])
            
        return finalData
    
    """
    Do the final time-interpolation of weather data at interpolated points in space
    """
    def getWeatherDataInterpolated(self, latitude, longitude, time, elevation):
        #Find the most recent predictionTime to the given time
        if not self.predTime:
            self.predTime = self.getClosestPredictionTime(time, below=True)
        #print "Predtime: ", predTime
        #Get all ValueTimes available in prediction, and find the ones that bracket the given time
        self.wc.execute("""SELECT DISTINCT(ValueTime) FROM WeatherData WHERE PredictionTime=\"%s\""""%self.predTime)
        availableTimes = self.wc.fetchall()
        availableTimes = [x[0] for x in availableTimes]
        beforeTime = min(availableTimes)
        afterTime = max(availableTimes)
        #print "Time: ", time
        if not beforeTime <= time:
            logger.critical("Weather data in this prediction not early enough")
            raise MissingDataExcecption
        if not afterTime >= time:
            logger.critical("Weather data in this prediction not late enough")
            raise MissingDataException

        for predtime in availableTimes:
            if predtime > beforeTime and predtime < time:
                beforeTime = predtime
            if predtime < afterTime and predtime > time:
                afterTime = predtime
        #print "Beforetime: ", beforeTime
        #print "AfterTime: ", afterTime
        
        if beforeTime == time or afterTime == time:
            #print "Using exact value time"
            return self.getWeatherDataAtPointAtPredictionTime(latitude, longitude, time, elevation)
        else:
            #Get spatially-interpolated datapoints
            beforeWeather = self.getWeatherDataAtPointAtPredictionTime(latitude, longitude, beforeTime, elevation)
            afterWeather = self.getWeatherDataAtPointAtPredictionTime(latitude, longitude, afterTime, elevation)
            
            #Compute temporal weighting factors
            beforeWeightingFactor = (afterTime-time).total_seconds()/(afterTime-beforeTime).total_seconds()
            afterWeightingFactor = (time-beforeTime).total_seconds() / (afterTime-beforeTime).total_seconds()
            
            #Compute weighted average of all weather metrics
            finalData = {}
            for key in beforeWeather:
                finalData[key] = beforeWeather[key]*beforeWeightingFactor + afterWeather[key]*afterWeightingFactor
            return finalData

    def linterp(self, x, x1, x2, y1, y2):
        #try:
        #    assert x <= max(x1, x2) and x >= min(x1, x2)
        #except AssertionError:
        #    logger.warning("X (%f) is not between %f and %f. Linear interpolation may be inaccurate."%(x, x1, x2))
            
        return ((x2 - x) / (x2 - x1)) * y1 + ((x - x1) / (x2 - x1)) * y2
        
    """
    Four-way linear interpolation of some function f(x, y)=z1
    @param x: x-coordinate of interpolated point
    @param y: y-coordinate of interpolated point
    @param x1: First known x-coordinate
    @param x2: Second known x-coordinate
    @param y1: First known y-coordinate
    @param y2: Second known y-coordinate
    @param z1: Value at point (x1, y1)
    @param z2: Value at point (x2, y1)
    @param z3: Value at point (x1, y2)
    @param z4: Value at point (x2, y2)
    """        
    def linterpQuad(self, x, y, x1, x2, y1, y2, z1, z2, z3, z4):
        #Find intermediate points (interpolate along x)
        i1 = self.linterp(x, x1, x2, z1, z2)
        i2 = self.linterp(x, x1, x2, z3, z4)
        
        #Interpolate along y
        return self.linterp(y, y1, y2, i1, i2)

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
            raise MissingDataException
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
                #print lowerLat, upperLat, lowerLon, upperLon
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
