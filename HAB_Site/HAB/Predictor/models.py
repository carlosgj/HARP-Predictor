from __future__ import unicode_literals

from django.db import models
import numpy as np

# Create your models here.
class launchLocation(models.Model):
    name = models.CharField("Name", max_length=25, unique=True)
    description = models.CharField("Description", max_length=200, blank=True)
    latitude = models.FloatField("Latitude")
    longitude = models.FloatField("Longitude")
    altitude = models.IntegerField("Altitude")
    def __str__(self):
        return "%s (%s)"%(self.name, self.description)

class Prediction(models.Model):
    launchTime = models.DateTimeField("Launch time")
    
    launchPoint = models.ForeignKey(launchLocation, on_delete=models.CASCADE, null=True)
    
    burstTime = models.DateTimeField("Burst time", null=True)
    burstLatitude = models.FloatField("Burst latitude", null=True)
    burstLongitude = models.FloatField("Burst longitude", null=True)
    burstAltitude = models.IntegerField("Burst altitude", null=True)
    
    landingTime = models.DateTimeField("Landing time", null=True)
    landingLatitude = models.FloatField("Landing latitude", null=True)
    landingLongitude = models.FloatField("Landing longitude", null=True)
    landingAltitude = models.IntegerField("Landing altitude", null=True)

    usingPrediction = models.DateTimeField("Prediction time")
    
    ascentRate = models.FloatField("Ascent rate")
    descentRate = models.FloatField("Descent rate")

class PredictionPoint(models.Model):
    prediction = models.ForeignKey(Prediction, on_delete=models.CASCADE)
    latitude = models.FloatField()
    longitude = models.FloatField()
    altitude = models.IntegerField()
    groundElevation = models.IntegerField()
    time = models.DateTimeField()
    pressure = models.FloatField(null=True)
    temperature = models.IntegerField(null=True)
    ugrd = models.FloatField(null=True)
    vgrd = models.FloatField(null=True)

class elevationPoint(models.Model):
    latitude = models.FloatField("Latitude")
    longitude = models.FloatField("Longitude")
    elevation = models.FloatField("Elevation")
    def __str__(self):
        return "%f, %f: %f"%(self.latitude, self.longitude, self.elevation)

def linterp(x, x1, x2, y1, y2):
    return ((x2 - x) / (x2 - x1)) * y1 + ((x - x1) / (x2 - x1)) * y2

def getAltitudeAtPoint(latitude, longitude):
    results = elevationPoint.objects.filter(latitude__range=(latitude-0.0001, latitude+0.0001), longitude__range=(longitude-0.0001, longitude+0.0001))

    lowerLat = -1
    upperLat = 90
    lowerLon = -360
    upperLon = 361
    exactLat = None
    exactLon = None

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
        assert len(ypoints1) == 2
        assert len(ypoints2) == 2
        xpt1 = linterp(latitude, ypoints1[0].latitude, ypoints1[1].latitude, ypoints1[0].elevation, ypoints1[1].elevation)
        xpt2 = linterp(latitude, ypoints2[0].latitude, ypoints2[1].latitude, ypoints2[0].elevation, ypoints2[1].elevation)
        return linterp(longitude, lowerLon, upperLon, xpt2, xpt1)
