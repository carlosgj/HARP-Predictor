# This script is designed to analyze the suitability of launch times for landing near a particular point.


import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()
import datetime
from math import sin, cos, asin, degrees, radians, sqrt, atan2
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
from copy import copy 
import os

#From https://stackoverflow.com/questions/48271838/plotting-with-matplotlib-typeerror-float-argument-must-be-a-string-or-a-numb
# and https://stackoverflow.com/questions/47404653/pandas-0-21-0-timestamp-compatibility-issue-with-matplotlib
import pandas.plotting._converter as pandacnv
pandacnv.register()

CURDIR = os.path.dirname(os.path.abspath(__file__))
EARTH_RADIUS = 2.0904e+7
FEET_PER_MILE = 5280

target_point = (34.675, -117.799)


resultsDir = os.path.join(CURDIR, "trending")

class FlightPrediction():
    def calc(self):
        if self.landingLatitude is None:
            return False
        lat1 = radians(self.launchLatitude)
        lon1 = radians(self.launchLongitude)
        lat2 = radians(self.landingLatitude)
        lon2 = radians(self.landingLongitude)
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        centralAngle = 2*asin(sqrt(sin(dlat/2.)**2+cos(lat1)*cos(lat2)*sin(dlon/2)**2))
        self.groundDistance = centralAngle*EARTH_RADIUS
        
        self.bearingToLanding = degrees(atan2(dlon, dlat))
        
        errorlat = lat2 - radians(target_point[0])
        errorlon = lon2 - radians(target_point[1])
        errorCentralAngle = 2*asin(sqrt(sin(errorlat/2.)**2+cos(radians(target_point[0]))*cos(lat2)*sin(errorlon/2)**2))
        self.errorDistance = errorCentralAngle*EARTH_RADIUS
        
    def __init__(self):
        self.id = -1
        self.launchTime = None
        self.landingTime = None
        self.launchLatitude = None
        self.launchLongitude = None
        self.landingLatitude = None
        self.landingLongitude = None
        self.landed = False
    def __str__(self):
        return "Prediction for launch at %s from %f %f, landing at %s"%(self.launchTime.strftime("%Y/%m/%d %H:%M:%S"), self.launchLatitude, self.launchLongitude, self.landingTime.strftime("%Y/%m/%d %H:%M:%S"))

def wrapSubtractAngle(a1, a2):
    res = a1 - a2
    if res < -180:
        res += 360
    elif res > 180:
        res -= 360
    return res
        
def ingestData():
    wdb=MySQLdb.connect(host='weatherdata.kf5nte.org', user='readonly', passwd='',db="Django")
    wc = wdb.cursor()

    paramSets = {}
    wc.execute("""SELECT id, burstAltitude, ascentRate, descentRate FROM Predictor_balloonparameterset""")
    results = wc.fetchall()
    for res in results:
        paramSets[res[0]] = {'burstAltitude':res[1], 'ascentRate':res[2], 'descentRate':res[3]}
    
    predCache = []
    launchPoints = set([])
    wc.execute("""SELECT id, launchTime, landingTime, landingLatitude, landingLongitude, landingAltitude, launchPoint_id, parameters_id FROM Predictor_prediction WHERE landingTime IS NOT NULL""")
    results = wc.fetchall()
    for result in results:
        this = FlightPrediction()
        this.id = result[0]
        this.launchTime = result[1]
        this.landingTime = result[2]
        this.landingLatitude = result[3]
        this.landingLongitude = result[4]
        this.launchLocation = result[6]
        this.ascentRate = paramSets[result[7]]['ascentRate']
        this.descentRate = paramSets[result[7]]['descentRate']
        this.burstAltitude = paramSets[result[7]]['burstAltitude']
        this.paramSet = result[7]
        this.landed = True
        predCache.append(this)
        launchPoints.add(result[6])
        
    #Get last known points for flights which didn't land
    wc.execute("""SELECT id, launchTime, launchPoint_id, parameters_id FROM Predictor_prediction WHERE landingTime IS NULL""")
    results = wc.fetchall()
    for result in results:
        this = FlightPrediction()
        this.id = result[0]
        this.launchTime = result[1]
        this.launchLocation = result[2]
        this.landed = False
        this.ascentRate = paramSets[result[3]]['ascentRate']
        this.descentRate = paramSets[result[3]]['descentRate']
        this.burstAltitude = paramSets[result[3]]['burstAltitude']
        this.paramSet = result[3]
        wc.execute("""SELECT latitude, longitude FROM Predictor_predictionpoint WHERE prediction_id=%d ORDER BY time DESC LIMIT 1"""%this.id)
        pt = wc.fetchall()
        this.landingLatitude = pt[0][0]
        this.landingLongitude = pt[0][1]
        predCache.append(this)
        launchPoints.add(result[2])

    for lp in launchPoints:
        wc.execute("""SELECT latitude, longitude FROM Predictor_launchlocation WHERE id=%d"""%lp)
        results = wc.fetchall()
        assert len(results) == 1
        lat = results[0][0]
        lon = results[0][1]
        for pred in predCache:
            if pred.launchLocation == lp:
                pred.launchLatitude = lat
                pred.launchLongitude = lon
                
    for pred in predCache:
        pred.calc()
        
    wdb.close()

    predCache.sort(key = lambda x: x.launchTime)
    return predCache, launchPoints, paramSets

def plotAllPoints(predCache, paramSet, launchLocation, resultsRootDir):
    launchTimes = []
    errorDistances = []
    modelPred = None
    for pred in predCache:
        if pred.paramSet==paramSet and pred.launchLocation== launchLocation:
            modelPred = pred
            if pred.landed:
                launchTimes.append(pred.launchTime)
                errorDistances.append(pred.errorDistance/FEET_PER_MILE)
                print pred.id, pred.launchTime, pred.errorDistance
        
    firstDate = min(launchTimes)
    firstDate = firstDate.replace(day=1)
    firstDate = firstDate.replace(hour=0)
    firstDate = firstDate.replace(minute=0)
    firstDate = firstDate.replace(second=0)
    lastDate = max(launchTimes)
    thisPlot = firstDate
    distanceDir = os.path.join(resultsRootDir, "target_distance")
    os.mkdir(distanceDir)
    while True:
        endDate = copy(thisPlot)
        if endDate.month < 12:
            endDate = endDate.replace(month=endDate.month+1)
        else:
            endDate = endDate.replace(year=endDate.year+1, month=1)
        print thisPlot, endDate
                
        plt.plot(launchTimes, errorDistances, 'b.', ls="None")
        plt.xticks(np.arange(thisPlot, endDate, datetime.timedelta(days=10)), rotation=90)
        plt.gca().xaxis.set_major_locator(matplotlib.dates.DayLocator())
        plt.gca().xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%d"))
        plt.xlim(thisPlot, endDate)
        plt.xlabel("Date (%s)"%thisPlot.strftime("%Y-%m"))
        plt.ylabel("Distance from target (miles)")
        plt.grid(b=True)
        plt.title("Target Error Distance Time Plot:\n%0.1f ft/s ascent, %0.1f ft/s descent, burst at %d ft\nLaunch from %0.4f %0.4f"%(modelPred.ascentRate, modelPred.descentRate, modelPred.burstAltitude, modelPred.launchLatitude, modelPred.launchLongitude))
        filename = "%s.pdf"%thisPlot.strftime("%Y-%m")
        plt.savefig(os.path.join(distanceDir, filename), bbox_inches='tight', pad_inches=0.5)
        plt.close()
        thisPlot = endDate
        if endDate >= lastDate:
            break
            
def plotYearComposite(predCache, paramSet, launchLocation, resultsRootDir):
    launchTimes = []
    errorDistances = []
    modelPred = None
    for pred in predCache:
        if pred.paramSet==paramSet and pred.launchLocation== launchLocation:
            modelPred = pred
            if pred.landed:
                launchTimes.append(pred.launchTime.timetuple().tm_yday)
                errorDistances.append(pred.errorDistance/FEET_PER_MILE)
                print pred.id, pred.launchTime, pred.errorDistance
    plt.plot(launchTimes, errorDistances, 'b.', ls="None")
    #plt.xticks(np.arange(thisPlot, endDate, datetime.timedelta(days=10)), rotation=90)
    #plt.gca().xaxis.set_major_locator(matplotlib.dates.DayLocator())
    #plt.gca().xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%d"))
    #plt.xlim(thisPlot, endDate)
    plt.xlabel("Day of Year")
    plt.ylabel("Distance from target (miles)")
    plt.grid(b=True)
    plt.title("Target Error Distance Composite Plot:\n%0.1f ft/s ascent, %0.1f ft/s descent, burst at %d ft\nLaunch from %0.4f %0.4f"%(modelPred.ascentRate, modelPred.descentRate, modelPred.burstAltitude, modelPred.launchLatitude, modelPred.launchLongitude))
    filename = "Composite_Target_Error.pdf"
    plt.savefig(os.path.join(resultsRootDir, filename), bbox_inches='tight', pad_inches=0.5)
    plt.close()
    

def generateLaTeXReport(launchLocations, paramSets):
    fileText = """\\documentclass[]{article}
\\usepackage{fullpage}
\\usepackage{pdfpages}
%opening
\\title{High-Altitude Balloon Flight Prediction Trending Report}
\\author{Carlos Gross Jones}

\\begin{document}

\\maketitle
\\newpage
"""
    for launchLocation in launchLocations:
        fileText += "\\section{Launch Location %d}\n"%launchLocation
        for paramSet in paramSets.keys():
            fileText += "\\subsection{Parameter Set %d}\n"%paramSet
            fileText += "\\subsubsection{Target Error Distances}\n"
            for pdf in os.listdir(os.path.join(resultsDir, "LaunchLoc%d"%launchLocation, "ParamSet%d"%paramSet, "target_distance")):
                fileText += "\\includepdf[landscape=true,pages=-]{%s}\n"%os.path.join("LaunchLoc%d"%launchLocation, "ParamSet%d"%paramSet, "target_distance", pdf).replace(os.path.sep, '/')
    fileText += "\\end{document}\n"
    fob = open(os.path.join(resultsDir, "TrendingReport.tex"), 'w+')
    fob.write(fileText)
    fob.close()
    
if __name__=="__main__":
    cache, launchLocations, paramSets = ingestData()
    for launchLocation in launchLocations:
        print "Processing launchpoint %d..."%launchLocation
        thisLLDir = os.path.join(resultsDir, "LaunchLoc%d"%launchLocation)
        os.mkdir(thisLLDir)
        for paramSet in paramSets.keys():
            thisParamSetDir = os.path.join(thisLLDir, "ParamSet%d"%paramSet)
            os.mkdir(thisParamSetDir)
            plotAllPoints(cache, paramSet, launchLocation, thisParamSetDir)
            plotYearComposite(cache, paramSet, launchLocation, thisParamSetDir)
    generateLaTeXReport(launchLocations, paramSets)
    #plotDayTrending(cache)
    