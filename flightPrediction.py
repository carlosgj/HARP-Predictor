from datetime import datetime, timedelta
import Analysis
import copy 
import math

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
        self.ascentRate = ascentRate
        self.descentRate = descentRate
    #These are calculated
    burstPoint=None #GeoPoint object
    landingPoint=None #GeoPoint object
    path=[]

def runPrediction(prediction):
    assert (prediction.burstPressure is not None) or (prediction.burstAltitude is not None)
    prediction.phase = 'ascent'
    prediction.currentPoint = prediction.launchPoint
    print "Launch point:", prediction.launchPoint
    while(True):
        #Copy point into path
        prediction.path.append(copy.deepcopy(prediction.currentPoint))
        #Get weather at point
        weather = Analysis.getWeatherDataInterpolated(prediction.currentPoint.latitude, prediction.currentPoint.longitude, prediction.currentPoint.time, prediction.currentPoint.elevation)
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
            elif (prediction.burstAltitude is not None) and prediction.elevation > prediction.burstAltitude:
                prediction.phase = "descent"
        print newpoint
        #break #debugging



if __name__ == "__main__":
    launchPoint = geoTimePoint(34.237, -118.254, 1000, datetime.utcnow())
    pred = Prediction(launchPoint, 7.2, 16, burstAltitude=100000)
    runPrediction(pred)