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

def runPrediction(prediction):
    assert (prediction.burstPressure is not None) or (prediction.burstAltitude is not None)
    prediction.phase = 'ascent'
    ascendingFlag = True
    prediction.currentPoint = prediction.launchPoint
    print "Launch point:", prediction.launchPoint
    exactElevationFlag = 0
    staleGroundAlt = None
    while(True):
        if exactElevationFlag == 0:
            groundAlt = prediction.engine.getAltitudeAtPoint(prediction.currentPoint.latitude, prediction.currentPoint.longitude)
            staleGroundAlt = groundAlt
            if (prediction.currentPoint.elevation-groundAlt) > 5000:
                exactElevationFlag = 5
        else:
            exactElevationFlag -= 1
            groundAlt = staleGroundAlt
        #print exactElevationFlag, groundAlt, prediction.currentPoint.elevation
        #print prediction.currentPoint.latitude, prediction.currentPoint.longitude, groundAlt, roughelevation
        if ascendingFlag and prediction.currentPoint.elevation > groundAlt:
            ascendingFlag = False
        prediction.currentPoint.groundAlt = groundAlt
        #Copy point into path
        prediction.path.append(copy.deepcopy(prediction.currentPoint))
        #Get weather at point
        try:
            weather = prediction.engine.getWeatherDataInterpolated(prediction.currentPoint.latitude, prediction.currentPoint.longitude, prediction.currentPoint.time, prediction.currentPoint.elevation/feetPerMeter)
        except:
            print prediction.currentPoint
            raise
        newpoint = geoTimePoint(time=prediction.currentPoint.time+timedelta(seconds=iterationIntervalSeconds))
        if prediction.phase == "ascent":
            newpoint.elevation = prediction.currentPoint.elevation + (iterationIntervalSeconds*prediction.ascentRate)
        elif prediction.phase == "descent":
            newpoint.elevation = prediction.currentPoint.elevation - (iterationIntervalSeconds*prediction.descentRate)
        deltaXFeet = weather['UGRD']*feetPerMeter*iterationIntervalSeconds*prediction.ugrdMultiplier
        deltaYFeet = weather['VGRD']*feetPerMeter*iterationIntervalSeconds*prediction.vgrdMultiplier
        feetPerDegreeLongitude = feetPerDegreeLongitudeEquator*math.cos(math.radians(prediction.currentPoint.latitude))
        newpoint.latitude = prediction.currentPoint.latitude + (deltaYFeet/feetPerDegreeLatitude)
        newpoint.longitude = prediction.currentPoint.longitude + (deltaXFeet/feetPerDegreeLongitude)
        prediction.currentPoint = newpoint
        
        #Check if newpoint is in a grid square we have terrain data for
        gridSquareTuple = (int(newpoint.latitude)+0.5, int(newpoint.longitude)-0.5)
        if len(availableAltitudeSquares) > 0 and gridSquareTuple not in availableAltitudeSquares:
            print colored("Balloon exited terrain data area at %f, %f."%(newpoint.latitude, newpoint.longitude), 'red')
            break

        if prediction.phase == 'ascent':
            if (prediction.burstPressure is not None) and weather["Pressure"] < prediction.burstPressure:
                prediction.phase = "descent"
                prediction.burstPoint = newpoint
            elif (prediction.burstAltitude is not None) and prediction.currentPoint.elevation > prediction.burstAltitude:
                prediction.phase = "descent"
                prediction.burstPoint = newpoint
                print colored("Balloon burst at %f, %f"%(newpoint.latitude, newpoint.longitude), 'green')
        if prediction.phase=='descent':
            if newpoint.elevation < groundAlt:
                print colored("Balloon landed. Terminating.", 'green')
                prediction.landingPoint = newpoint
                break
        #print newpoint
        #break #debugging



if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    analysisLogger = logging.getLogger("Analysis")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    analysisLogger.addHandler(ch)
    launchPoint = geoTimePoint(34.237, -118.254, 1000, datetime.utcnow())
    thisEng = Analysis.AnalysisEngine()
    pred = Prediction(launchPoint, 7.2, 16, burstAltitude=60000, engine=thisEng)
    runPrediction(pred)
