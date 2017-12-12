from datetime import datetime, timedelta
import Analysis
import copy 
import math
import logging
logger = logging.getLogger(__name__)
iterationIntervalSeconds = 30

feetPerMeter = 3.28084
feetPerDegreeLatitude = 364000
feetPerDegreeLongitudeEquator = 365220

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
    def __init__(self, launchPoint, ascentRate, descentRate, burstPressure=None, burstAltitude=None):
        self.launchPoint = launchPoint
        self.burstPressure = burstPressure
        self.burstAltitude = burstAltitude
        self.ascentRate = ascentRate
        self.descentRate = descentRate
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
    while(True):
        pts = Analysis.getAltitudePoints(prediction.currentPoint.latitude, prediction.currentPoint.longitude)
        try:
            roughelevation = max([x.elevation for x in pts])
        except ValueError:
            logger.error("Balloon reached edge of altitude data at %f, %f, %f ft."%(prediction.currentPoint.latitude, prediction.currentPoint.longitude, prediction.path[-1].groundAlt))
            break
        if (len(prediction.path) == 0 or prediction.currentPoint.elevation > (roughelevation + 1000)) and not ascendingFlag:
            groundAlt = roughelevation
        else:
            groundAlt = Analysis.getAltitudeAtPoint(prediction.currentPoint.latitude, prediction.currentPoint.longitude)
        #print prediction.currentPoint.latitude, prediction.currentPoint.longitude, groundAlt, roughelevation
        if ascendingFlag and prediction.currentPoint.elevation > roughelevation:
            ascendingFlag = False
        prediction.currentPoint.groundAlt = groundAlt
        #Copy point into path
        prediction.path.append(copy.deepcopy(prediction.currentPoint))
        #Get weather at point
        try:
            weather = Analysis.getWeatherDataInterpolated(prediction.currentPoint.latitude, prediction.currentPoint.longitude, prediction.currentPoint.time, prediction.currentPoint.elevation/feetPerMeter)
        except:
            print prediction.currentPoint
        newpoint = geoTimePoint(time=prediction.currentPoint.time+timedelta(seconds=iterationIntervalSeconds))
        if prediction.phase == "ascent":
            newpoint.elevation = prediction.currentPoint.elevation + (iterationIntervalSeconds*prediction.ascentRate)
        elif prediction.phase == "descent":
            newpoint.elevation = prediction.currentPoint.elevation - (iterationIntervalSeconds*prediction.descentRate)
        deltaXFeet = weather['UGRD']*feetPerMeter*iterationIntervalSeconds
        deltaYFeet = weather['VGRD']*feetPerMeter*iterationIntervalSeconds
        feetPerDegreeLongitude = feetPerDegreeLongitudeEquator*math.cos(math.radians(prediction.currentPoint.latitude))
        newpoint.latitude = prediction.currentPoint.latitude + (deltaYFeet/feetPerDegreeLatitude)
        newpoint.longitude = prediction.currentPoint.longitude + (deltaXFeet/feetPerDegreeLongitude)
        prediction.currentPoint = newpoint
        if prediction.phase == 'ascent':
            if (prediction.burstPressure is not None) and weather["Pressure"] < prediction.burstPressure:
                prediction.phase = "descent"
                prediction.burstPoint = newpoint
            elif (prediction.burstAltitude is not None) and prediction.currentPoint.elevation > prediction.burstAltitude:
                prediction.phase = "descent"
                prediction.burstPoint = newpoint
                print "Balloon burst at ",newpoint.latitude, newpoint.longitude
        if prediction.phase=='descent':
            if newpoint.elevation < groundAlt:
                print "Balloon landed. Terminating."
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
    pred = Prediction(launchPoint, 7.2, 16, burstAltitude=50000)
    runPrediction(pred)
