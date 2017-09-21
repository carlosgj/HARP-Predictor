import MySQLdb
from datetime import datetime
from collections import defaultdict
import logging
logger = logging.getLogger(__name__)
db=MySQLdb.connect(host='delta.carlosgj.org', user='readonly', passwd='',db="test_dataset")
c = db.cursor()
latestTable = None
MBTables = {}

def getLatestTable():
    global latestTable
    if latestTable is not None:
        return latestTable
    c.execute("SHOW tables")
    raw = c.fetchall()
    tables = [int(x[0].lstrip("gfs")) for x in raw if x[0].startswith("gfs")]
    latestNum = max(tables)
    latestTable = "gfs"+str(latestNum)
    print "Using table:", latestTable
    return latestTable
    
def getHGTsforExact(latitude, longitude, resolution, time):
    resStr = "0.50"
    if resolution==0.25:
        resStr = '0.25'
    rows = c.execute("""SELECT Isobar, HGT from %s WHERE Latitude=%f AND Longitude=%f AND Resolution= '%s' AND ValueTime='%s'"""%(getLatestTable(), latitude, longitude, resStr, time.strftime('%Y-%m-%d %H:%M:%S')))
    results =  c.fetchall()
    return results
    
def buildMBTable(latitude, longitude, time):
    global MBTables
    if (latitude, longitude, time) in MBTables:
        return MBTables[(latitude, longitude, time)]
    #print latitude, longitude, time.strftime('%Y%m%d %H:%M:%S')
    #res25 = getHGTsforExact(latitude, longitude, 0.25, time)
    res50 = getHGTsforExact(latitude, longitude, 0.50, time)
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
    MBTables[(latitude, longitude, time)] = finalTable
    return finalTable
    
def bracketElevation(latitude, longitude, time, elevation):
    table = buildMBTable(latitude, longitude, time)
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
    sortedKeys = sorted(table.keys())
    for idx, hgt in enumerate(sortedKeys):
        if  hgt < elevation:
            retDict['lowerElevation']=hgt
            retDict['lowerIsobar'] = table[hgt]
        elif hgt > elevation:
            retDict['upperElevation'] = hgt
            retDict['upperIsobar'] = table[hgt]
            break
    return retDict
    
def getZPlaneAverage(latitude, longitude, time, elevation):
    points = bracketElevation(latitude, longitude, time, elevation)
    if (not points['lowerIsobar']) or (not points['upperIsobar']):
        print points
        return
        #TODO: error handling
    upperWeightingFactor = (float(elevation)-points['lowerElevation'])/(points['upperElevation']-points['lowerElevation'])
    lowerWeightingFactor = (points['upperElevation']-float(elevation))/(points['upperElevation']-points['lowerElevation'])
    meanPressure = upperWeightingFactor*points['upperIsobar']+lowerWeightingFactor*points['lowerIsobar']

    #Get weather data at lower point
    c.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM %s WHERE ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(getLatestTable(), time.strftime("'%Y-%m-%d %H:%M:%S'"), latitude, longitude, points['lowerIsobar']))
    raw = c.fetchall()
    lowerData = []
    for row in raw:
        lowerData.append({'Res':row[0], 'TMP':row[1], 'UGRD':row[2], 'VGRD':row[3]})
    
    #Get weather data at upper point
    c.execute("""SELECT Resolution, TMP, UGRD, VGRD FROM %s WHERE ValueTime=%s AND Latitude=%f AND Longitude=%f AND Isobar=%s"""%(getLatestTable(), time.strftime("'%Y-%m-%d %H:%M:%S'"), latitude, longitude, points['upperIsobar']))
    raw = c.fetchall()
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
    
def findLatLongPoints(latitude, longitude):
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
    
def getWeatherDataAtPointAtPredictionTime(latitude, longitude, time, elevation):
    #First, find 4 nearest lat/lon points
    lats, longs = findLatLongPoints(latitude, longitude)
    minLat = min(lats)
    maxLat = max(lats)
    minLong = min(longs)
    maxLong = max(longs)
    #print minLat, maxLat, minLong, maxLong
    #Next, interpolate weather data at specified elevation at each point
    ZPlaneData = {}
    ZPlaneData['-x-y'] = getZPlaneAverage(minLat, minLong, time, elevation)
    ZPlaneData['-x-y'].update(Latitude=minLat, Longitude=minLong)
    ZPlaneData['-x+y'] = getZPlaneAverage(maxLat, minLong, time, elevation)
    ZPlaneData['-x+y'].update({'Latitude':maxLat, 'Longitude':minLong})
    ZPlaneData['+x-y'] = getZPlaneAverage(minLat, maxLong, time, elevation)
    ZPlaneData['+x-y'].update({'Latitude':minLat, 'Longitude':maxLong})
    ZPlaneData['+x+y'] = getZPlaneAverage(maxLat, maxLong, time, elevation)
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
    
    
def getWeatherDataInterpolated(latitude, longitude, time, elevation):
    #Bracket time
    table=getLatestTable()
    c.execute("""SELECT DISTINCT(ValueTime) FROM %s"""%table)
    availableTimes = c.fetchall()
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
    beforeWeather = getWeatherDataAtPointAtPredictionTime(latitude, longitude, beforeTime, elevation)
    afterWeather = getWeatherDataAtPointAtPredictionTime(latitude, longitude, afterTime, elevation)
    beforeWeightingFactor = (afterTime-time).total_seconds()/(afterTime-beforeTime).total_seconds()
    afterWeightingFactor = (time-beforeTime).total_seconds() / (afterTime-beforeTime).total_seconds()
    finalData = {}
    for key in beforeWeather:
        finalData[key] = beforeWeather[key]*beforeWeightingFactor + afterWeather[key]*afterWeightingFactor
    return finalData

def linterp(x, x1, x2, y1, y2):
    return ((x2 - x) / (x2 - x1)) * y1 + ((x - x1) / (x2 - x1)) * y2

def getAltitudeAtPoint(latitude, longitude):
    class point():
        latitude = None
        longitude = None
        elevation = None
        def __init__(self, lat, lon, ele):
            self.latitude = lat
            self.longitude = lon
            self.elevation = ele
    c.execute("""USE Django""")
    c.execute("""SELECT latitude, longitude, elevation FROM Predictor_elevationpoint WHERE latitude BETWEEN %f AND %f AND longitude BETWEEN %f AND %f"""%(latitude-0.0001, latitude+0.0001, longitude-0.0001, longitude+0.0001))
    results = c.fetchall()
    c.execute("""USE test_dataset""")
    results = [point(x[0], x[1], x[2]) for x in results]

    lowerLat = -1
    upperLat = 90
    lowerLon = -360
    upperLon = 361
    exactLat = None
    exactLon = None
    #print results
    for result in results:
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
        xpoints = [x for x in results if x.latitude==exactLat and (x.longitude == lowerLon or x.longitude == upperLon)]
        assert len(xpoints) == 2
        return linterp(longitude, xpoints[0].longitude, xpoints[1].longitude, xpoints[0].elevation, xpoints[1].elevation)
    elif exactLon:
        ypoints = [x for x in results if x.longitude==exactLon and (x.latitude == lowerLat or x.latitude == upperLat)]
        assert len(ypoints) == 2
        return linterp(latitude, ypoints[0].latitude, ypoints[1].latitude, ypoints[0].elevation, ypoints[1].elevation)
    else:
        ypoints1 = [x for x in results if x.longitude==upperLon and (x.latitude == lowerLat or x.latitude == upperLat)]
        ypoints2 = [x for x in results if x.longitude==lowerLon and (x.latitude == lowerLat or x.latitude == upperLat)]
        try:
            assert len(ypoints1) == 2
            assert len(ypoints2) == 2
        except:
            print "ypoints1:", ypoints1, " ypoints2:", ypoints2
            print latitude, longitude
            raise
        xpt1 = linterp(latitude, ypoints1[0].latitude, ypoints1[1].latitude, ypoints1[0].elevation, ypoints1[1].elevation)
        xpt2 = linterp(latitude, ypoints2[0].latitude, ypoints2[1].latitude, ypoints2[0].elevation, ypoints2[1].elevation)
        return linterp(longitude, lowerLon, upperLon, xpt2, xpt1)


    
if __name__ == "__main__":
    print getAltitudeAtPoint(34.5, -118.5)
#    print getWeatherDataInterpolated(34.2,-118.6, datetime.strptime('201707251230', '%Y%m%d%H%M'), 12000)
