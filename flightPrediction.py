from datetime import datetime, timedelta
import Analysis
import copy 
import math
import logging
from termcolor import colored
logger = logging.getLogger(__name__)
iterationIntervalSeconds = 30

feetPerMeter = 3.28084
feetPerDegreeLatitude = 364000
feetPerDegreeLongitudeEquator = 365220

availableAltitudeSquares = []
availableWeatherSquares = []

class geoTimePoint():
    latitude=None
    longitude=None
    elevation=None #Geophysical height, not AGL, in ft
    time=None #In UTC
    def __init__(self, latitude=None, longitude=None, elevation=None, time=None):
        self.latitude = latitude
        self.longitude = longitude
        self.elevation = elevation
        self.time = time
    def __str__(self):
        return "%s, %s, %s ft, %s UTC"%(self.latitude, self.longitude, self.elevation, self.time.strftime('%Y%m%d %H%M%S'))

class Prediction():
    #These need to be initialized
    launchPoint=None #GeoPoint object
    burstPressure=None #In mb
    burstAltitude=None
    ascentRate=None #in f/s
    descentRate=None #in f/s
    ugrdMultiplier = 1.
    vgrdMultiplier = 1.
    def __init__(self, launchPoint, ascentRate, descentRate, burstPressure=None, burstAltitude=None, engine=None):
        self.launchPoint = launchPoint
        self.burstPressure = burstPressure
        self.burstAltitude = burstAltitude
        self.ascentRate = ascentRate
        self.descentRate = descentRate
        self.engine = engine
    #These are calculated
    burstPoint=None #GeoPoint object
    landingPoint=None #GeoPoint object
    path=[]
    def __str__(self):
        return "Prediction: launch from %f, %f at %s; ^%f v%f"%(self.launchPoint.latitude, self.launchPoint.longitude, str(self.launchPoint.time), self.ascentRate, self.descentRate)

    def run(self):
        self.engine.start()
        assert (self.burstPressure is not None) or (self.burstAltitude is not None)
        self.phase = 'ascent'
        ascendingFlag = True
        self.currentPoint = self.launchPoint
        print "Launch point:", self.launchPoint
        exactElevationFlag = 0
        staleGroundAlt = None
        while(True):
            if exactElevationFlag == 0:
                try:
                    groundAlt = self.engine.getAltitudeAtPoint(self.currentPoint.latitude, self.currentPoint.longitude)
                except AssertionError:
                    return
                staleGroundAlt = groundAlt
                if (self.currentPoint.elevation-groundAlt) > 5000:
                    exactElevationFlag = 5
            else:
                exactElevationFlag -= 1
                groundAlt = staleGroundAlt
            #print exactElevationFlag, groundAlt, prediction.currentPoint.elevation
            #print prediction.currentPoint.latitude, prediction.currentPoint.longitude, groundAlt, roughelevation
            if ascendingFlag and self.currentPoint.elevation > groundAlt:
                ascendingFlag = False
            self.currentPoint.groundAlt = groundAlt
            #Copy point into path
            self.path.append(copy.deepcopy(self.currentPoint))
            #Get weather at point
            try:
                weather = self.engine.getWeatherDataInterpolated(self.currentPoint.latitude, self.currentPoint.longitude, self.currentPoint.time, self.currentPoint.elevation/feetPerMeter)
            except:
                print self.currentPoint
                raise
            newpoint = geoTimePoint(time=self.currentPoint.time+timedelta(seconds=iterationIntervalSeconds))
            if self.phase == "ascent":
                newpoint.elevation = self.currentPoint.elevation + (iterationIntervalSeconds*self.ascentRate)
            elif self.phase == "descent":
                newpoint.elevation = self.currentPoint.elevation - (iterationIntervalSeconds*self.descentRate)
            deltaXFeet = weather['UGRD']*feetPerMeter*iterationIntervalSeconds*self.ugrdMultiplier
            deltaYFeet = weather['VGRD']*feetPerMeter*iterationIntervalSeconds*self.vgrdMultiplier
            feetPerDegreeLongitude = feetPerDegreeLongitudeEquator*math.cos(math.radians(self.currentPoint.latitude))
            newpoint.latitude = self.currentPoint.latitude + (deltaYFeet/feetPerDegreeLatitude)
            newpoint.longitude = self.currentPoint.longitude + (deltaXFeet/feetPerDegreeLongitude)
            self.currentPoint = newpoint
            
            #Check if newpoint is in a grid square we have terrain data for
            gridSquareTuple = (int(newpoint.latitude)+0.5, int(newpoint.longitude)-0.5)
            if len(availableAltitudeSquares) > 0 and gridSquareTuple not in availableAltitudeSquares:
                print colored("Balloon exited terrain data area at %f, %f."%(newpoint.latitude, newpoint.longitude), 'red')
                break

            #check if newpoint is in a grid square that we have weather data for
            if len(availableWeatherSquares) > 0 and gridSquareTuple not in availableWeatherSquares:
                print colored("Balloon exited weather data area at %f, %f."%(newpoint.latitude, newpoint.longitude), 'red')
                break


            if self.phase == 'ascent':
                if (self.burstPressure is not None) and weather["Pressure"] < self.burstPressure:
                    self.phase = "descent"
                    self.burstPoint = newpoint
                elif (self.burstAltitude is not None) and self.currentPoint.elevation > self.burstAltitude:
                    self.phase = "descent"
                    self.burstPoint = newpoint
                    print colored("Balloon burst at %f, %f"%(newpoint.latitude, newpoint.longitude), 'green')
            if self.phase=='descent':
                if self.currentPoint.elevation < groundAlt:
                    print colored("Balloon landed at %f, %f. Terminating."%(newpoint.latitude, newpoint.longitude), 'green')
                    self.landingPoint = newpoint
                    break
            #print newpoint
            #break #debugging
        self.engine.stop()


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    analysisLogger = logging.getLogger("Analysis")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    analysisLogger.addHandler(ch)
    #launchPoint = geoTimePoint(34.237, -118.254, 1000, datetime.utcnow())
    launchPoint = geoTimePoint(34.237, -118.254, 1794, datetime(2017, 12, 25, 6))
    thisEng = Analysis.AnalysisEngine()
    pred = Prediction(launchPoint, 7, 15, burstAltitude=60000, engine=thisEng)
    pred.run()
