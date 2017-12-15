from osgeo import gdal
import os
import xml.etree.ElementTree as ET
import MySQLdb
import urllib

feetPerMeter = 3.28084

class FileIngestor():
    dbHost = None
    dbUser = None
    dbName = None
    dbTable = None
    def __init__(self, path, prefix):
        self.path = path
        self.prefix = prefix
        imgFilePath = os.path.join(self.path, "img%s.img"%self.prefix)
        self.gdalObj = gdal.Open(imgFilePath)
        self.dataArray = self.gdalObj.ReadAsArray()
        metadataPath = os.path.join(self.path, "%s_meta.xml"%self.prefix)
        tree = ET.parse(metadataPath)
        root = tree.getroot()
        bbinfo =  root.findall('idinfo')[0].findall('spdom')[0].findall('bounding')[0]
        self.west = float(bbinfo.findall('westbc')[0].text)
        self.east = float(bbinfo.findall('eastbc')[0].text)
        self.north = float(bbinfo.findall('northbc')[0].text)
        self.south = float(bbinfo.findall('southbc')[0].text)
        #print self.dataArray

    def ingestDB(self):
        db = MySQLdb.connect(host=self.dbHost, user=self.dbUser, db=self.dbName, passwd='')
        c = db.cursor()
        latPoints = self.dataArray.shape[0]
        longPoints = self.dataArray.shape[1]
        weInterval = self.east-self.west
        snInterval = self.north-self.south
        latIncrement = snInterval/latPoints
        longIncrement = weInterval/longPoints
        #print latIncrement, longIncrement
        #print weInterval, snInterval
        for yIdx, row in enumerate(self.dataArray):
            lat = self.north-(yIdx*latIncrement)
            print lat
            for xIdx, val in enumerate(row):
                elevFeet = val*feetPerMeter
                long = self.west+(xIdx*longIncrement)
                c.execute("""INSERT INTO %s (latitude, longitude, elevation) VALUES (%f, %f, %f) ON DUPLICATE KEY UPDATE elevation=VALUES(elevation)"""%(self.dbTable, lat, long, elevFeet))
                #print """INSERT INTO %s (latitude, longitude, elevation) VALUES (%f, %f, %f) ON DUPLICATE KEY UPDATE elevation=VALUES(elevation)"""%(self.dbTable, lat, long, elevFeet)
            db.commit()

if __name__ == "__main__":
    tempdir = "/root/elevData"
    centerLat = float(raw_input("Center latitude:"))
    centerLon = float(raw_input("Center longitude:"))
    assert centerLat % 1 == 0.5 and centerLon % 1 == 0.5
    cornerLat = centerLat + 0.5
    cornerLon = centerLon - 0.5
    urlPrefix = """https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/IMG/"""
    if cornerLon <  0:
        lonPrefix = 'w'
        cornerLon *= -1
    else:
        lonPrefix = 'e'
    if cornerLat < 0:
        latPrefix = 's'
        cornerLat *= -1
    else:
        latPrefix = 'n'
    gridname = latPrefix + str(int(cornerLat)) + lonPrefix + str(int(cornerLon))
    url = urlPrefix + gridname + ".zip"
    print "Downloading %s..."%url
    os.chdir(tempdir)
    os.mkdir(gridname)
    os.chdir(gridname)
    urllib.urlretrieve(url, os.path.join(tempdir, gridname, gridname+'.zip'))
    os.system("unzip %s.zip"%gridname)
    foo = FileIngestor(os.path.join(tempdir, gridname), "%s_13"%gridname)
    foo.dbHost = 'localhost'
    foo.dbUser = 'guest'
    foo.dbName = 'Django'
    foo.dbTable = 'Predictor_elevationpoint'
    foo.ingestDB()
