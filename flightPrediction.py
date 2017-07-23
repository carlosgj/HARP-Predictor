class geoTimePoint():
    latitude=None
    longitude=None
    elevation=None #Geophysical height, not AGL, in m
    time=None #In UTC

class Prediction():
    #These need to be initialized
    launchPoint=None #GeoPoint object
    launchTime=None #Datetime object in UTC
    burstPressure=None #In mb
    ascentRate=None
    descentRate=None
    #These are calculated
    burstPoint=None #GeoPoint object
    landingPoint=None #GeoPoint object
    path=[]

def runPrediction(