import MySQLdb
from datetime import datetime
db=MySQLdb.connect(host='delta.carlosgj.org', user='readonly', passwd='',db="test_dataset")
c = db.cursor()

def getLatestTable():
    c.execute("SHOW tables")
    raw = c.fetchall()
    tables = [int(x[0].lstrip("gfs")) for x in raw if x[0].startswith("gfs")]
    latestNum = max(tables)
    return "gfs"+str(latestNum)
    
def getHGTsforExact(latitude, longitude, resolution, time):
    resStr = "0.50"
    if resolution==0.25:
        resStr = '0.25'
    rows = c.execute("""SELECT Isobar, HGT from %s WHERE Latitude=%f AND Longitude=%f AND Resolution= '%s' AND ValueTime='%s'"""%(getLatestTable(), latitude, longitude, resStr, time.strftime('%Y-%m-%d %H:%M:%S')))
    return c.fetchall()
    
def buildMBTable(latitude, longitude, time):
    res25 = getHGTsforExact(latitude, longitude, 0.25, time)
    res50 = getHGTsforExact(latitude, longitude, 0.50, time)
    res50Table = {}
    for i in res50:
        res50Table[i[0]] = i[1]
    res25Table = {}
    for i in res25:
        res25Table[i[0]] = i[1]
    allisobars = set(res50Table.keys())
    allisobars = allisobars.union(set(res25Table.keys()))
    finalTable = {}
    for isobar in allisobars:
        val = None
        if isobar in res25Table:
            val = res25Table[isobar]
        if isobar in res50Table:
            if val:
                val = (val+res50Table[isobar])/2.
            else:
                val = res50Table[isobar]
        finalTable[isobar] = val
    return finalTable
    
def bracketElevation(latitude, longitude, time, elevation):
    table = buildMBTable(latitude, longitude, time)
    retDict = {
        'lowerIsobar':None,
        'lowerElevation':0,
        'upperIsobar':None, 
        'upperElevation':100000
        }
    for isobar, hgt in table.iteritems():
        if hgt > retDict['lowerElevation']  and hgt < elevation:
            retDict['lowerElevation'] = hgt
            retDict['lowerIsobar'] = isobar
        elif hgt < retDict['upperElevation'] and hgt > elevation:
            retDict['upperElevation'] = hgt
            retDict['upperIsobar'] = isobar
    return retDict
    
def getZPlaneAverage(latitude, longitude, time, elevation):
    points = bracketElevation(latitude, longitude, time, elevation)
    if (not points['lowerIsobar']) or (not points['upperIsobar']):
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
    
# Linear interpolation
#  x is the point of interest
#  x1 and x2 are the 2 points on either side of x
#  v1 and v2 are the values at x1 and x2 respectivly
#  returns the value at x
def lerp(x, x1, x2, v1, v2):
    return ((x2 - x) / (x2 - x1)) * v1 + ((x - x1) / (x2 - x1)) * v2

# TriLinear Interpolation
# x, y, & z is the point of interest
# vXXX is the value at the corners where XXX = z, y, x respectivly
# x1, x2 are the 2 points in the x dir, y & z represent the other 2 directions
# in our case, x = long, y=lat, & z = alt
def triLerp(x, y, z, v000, v001, v010, v011, v100, v101, v110, v111, x0, x1, y0, y1, z0, z1):
    # do the interpolation of the 4 points on the x plane
    x00 = lerp(x, x0, x1, v000, v100)
    x10 = lerp(x, x0, x1, v010, v110)
    x01 = lerp(x, x0, x1, v001, v101)
    x11 = lerp(x, x0, x1, v011, v111)
    # now the 2 in the y plane
    r0 = lerp(y, y0, y1, x00, x01)
    r1 = lerp(y, y0, y1, x10, x11)
    # and finally in the z plane
    return lerp(z, z0, z1, r0, r1)
    
def findLatLongPoints(latitude, longitude):
    firstLatPoint = round(4*latitude)/4.
    if firstLatPoint < latitude:
        secondLatPoint = firstLatPoint + 0.25
    elif firstLatPoint > latitude:
        secondLatPoint = firstLatPoint - 0.25
    else:
        secondLatPoint = firstLatPoint
    
    firstLongPoint = round(4*longitude)/4.
    if firstLongPoint < longitude:
        secondLongPoint = firstLongPoint + 0.25
    elif firstLongPoint > longitude:
        secondLongPoint = firstLongPoint - 0.25
    else:
        secondLongPoint = firstLongPoint
    return (firstLatPoint, secondLatPoint), (firstLongPoint, secondLongPoint)
    
def getWeatherDataAtPointAtTime(latitude, longitude, time, elevation):
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
if __name__ == "__main__":
    print getWeatherDataAtPointAtTime(34.2,-118.6, datetime.strptime('2017072318', '%Y%m%d%H'), 12000)