import flightPrediction
#import MySQLdb
import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()
import logging
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count as cpu_count
#pool = ThreadPool(1)
print cpu_count(), " CPUs"
logger = logging.getLogger(__name__)
import Analysis
import traceback
import multiprocessing
import time

logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

wdb=MySQLdb.connect(host='weatherdata.kf5nte.org', user='guest', passwd='',db="Django")
edb = MySQLdb.connect(host='elevationdata.kf5nte.org', user='readonly', passwd='',db="elevationdata")
wc = wdb.cursor()
ec = edb.cursor()

#Get terrain data limits
gridsquares = []
for lat in range(31, 38):
    for lon in range(-121, -114):
        centerlat = lat+0.5
        centerlon = lon+0.5
        ec.execute("""SELECT * FROM Predictor_elevationpoint WHERE latitude=%f AND longitude=%f LIMIT 1"""%(centerlat, centerlon))
        if len(ec.fetchall()) > 0:
            #We have data for this square
            gridsquares.append((centerlat, centerlon))
print gridsquares
#assert False
flightPrediction.availableAltitudeSquares = gridsquares



#c.execute("""SELECT launchTime, launchLatitude, launchLongitude, launchAltitude, ascentRate, descentRate, burstAltitude, id FROM Predictor_prediction""")
#preds = c.fetchall()

#c.execute("""DELETE FROM Predictor_predictionpoint""")
#c.execute("""DELETE FROM Predictor_prediction""")

intervalHours = 4
ascentRate = 7
descentRate = 15
burstAltitude=60000

interval = datetime.timedelta(hours=intervalHours)

#Get time of lastest weather data
wc.execute("""USE test_dataset""")
wc.execute("""SELECT PredictionTime FROM WeatherData ORDER BY PredictionTime DESC LIMIT 1""")
latestPred = wc.fetchall()[0]
latestPredTime = latestPred[0]

#Get all launch locations
wc.execute("""USE Django""")
wc.execute("""SELECT id, latitude, longitude, altitude, isActive FROM Predictor_launchlocation""")
_launchpoints = wc.fetchall()
launchpoints = []
for i in _launchpoints:
    if i[4]:
        this  = {}
        this['id'] = i[0]
        this['latitude'] = i[1]
        this['longitude'] = i[2]
        this['altitude'] = i[3]
        launchpoints.append(this)

allPredictions = []
currentStep = 0
while currentStep*intervalHours < 384:
    for launchpoint in launchpoints:
        launchPt = flightPrediction.geoTimePoint(launchpoint['latitude'], launchpoint['longitude'], launchpoint['altitude'], latestPredTime+(currentStep*interval))
        #print launchPt
        thisPred = flightPrediction.Prediction(launchPt, ascentRate, descentRate, burstAltitude=burstAltitude)
        thisPred.path = []
        thisPred.launchPointID = launchpoint['id']
        allPredictions.append(thisPred)
    currentStep += 1

wc.close()


def runit(pred, engine, tempid):
    pred.engine = engine
    thisdb=MySQLdb.connect(host='weatherdata.kf5nte.org', user='guest', passwd='',db="Django")
    thisc = thisdb.cursor()
    thisc.close()
    thisc = thisdb.cursor()
    print tempid, "- Running prediction for ", pred.launchPoint.time
    print "Using conn ", wdb, " cursor ", thisc
    try:
        flightPrediction.runPrediction(pred)
    #except AssertionError:
        #pass
    except:
        #logger.error("%d- Could not run prediction for launchpoint %d at %s using prediction from %s. Last point:%s"%(tempid, pred.launchPointID, pred.launchPoint.time.strftime("%Y-%m-%d %H:%M:%S"), latestPredTime.strftime("%Y-%m-%d %H:%M:%S"), str(pred.path[-1])))
        raise
    #else:
    if True:
        #c.execute("""INSERT INTO Predictor_prediction (launchPoint_id, launchTime, burstTime, burstLatitude, burstLongitude, landingTime, landingLatitude, landingLongitude, usingPrediction, ascentRate, descentRate, burstAltitude, landingAltitude) VALUES (%d, '%s', '%s', %f, %f, '%s', %f, %f, '%s', %f, %f, %d, %d)"""%(pred.launchPointID, pred.launchPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.burstPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.burstPoint.latitude, pred.burstPoint.longitude, pred.landingPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.landingPoint.latitude, pred.landingPoint.longitude, latestPredTime.strftime('%Y-%m-%d %H:%M:%S'), ascentRate, descentRate, pred.burstPoint.elevation, pred.landingPoint.elevation))
        thisc.execute("""INSERT INTO Predictor_prediction (launchPoint_id, launchTime, usingPrediction, ascentRate, descentRate) VALUES (%d, '%s', '%s', %f, %f)"""%(pred.launchPointID, pred.launchPoint.time.strftime('%Y-%m-%d %H:%M:%S'), latestPredTime.strftime('%Y-%m-%d %H:%M:%S'), ascentRate, descentRate))
        thisPredictionID = thisc.lastrowid
        #db.commit()
        if pred.burstPoint:
            thisc.execute("""UPDATE Predictor_prediction SET burstTime='%s', burstLatitude=%f, burstLongitude=%f, burstAltitude=%d WHERE id=%d"""%(pred.burstPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.burstPoint.latitude, pred.burstPoint.longitude, pred.burstPoint.elevation, thisPredictionID))
        if pred.landingPoint:
            thisc.execute("""UPDATE Predictor_prediction SET landingTime='%s', landingLatitude=%f, landingLongitude=%f, landingAltitude=%d WHERE id=%d"""%(pred.landingPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.landingPoint.latitude, pred.landingPoint.longitude, pred.landingPoint.elevation, thisPredictionID))
        if not pred.path:
            assert False
        #db.commit()
        for point in pred.path:
            #print point
            try:
                thisc.execute("""INSERT INTO Predictor_predictionpoint (latitude, longitude, altitude, groundElevation, time, prediction_id) VALUES (%f, %f, %f, %f, '%s', %d)"""%(point.latitude, point.longitude, point.elevation, point.groundAlt, point.time.strftime('%Y-%m-%d %H:%M:%S'), thisPredictionID))
            except:
                print "can't find ", thisPredictionID
                raise
    #print "Committing ", thisdb
    thisdb.commit()
    thisc.close()
    thisdb.close()

logger.setLevel(logging.DEBUG)
analysisLogger = logging.getLogger("Analysis")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
analysisLogger.addHandler(ch)


#for pred in allPredictions:
#    print pred.launchPoint.time
#assert False

if __name__ == "__main__":
    #e1 = Analysis.AnalysisEngine()

    #pool.map(go, allPredictions)
    loopcount = 0
    lastPred = None
    for i, pred in enumerate(allPredictions):
        eng = Analysis.AnalysisEngine()
        p = multiprocessing.Process(target=runit, args=(pred,eng,i))
        #runit(pred,eng, i)
        p.start()
        lastPred = p
        #p.join()
        loopcount += 1
        #if loopcount == 30:
        #break
        time.sleep(6)
    lastPred.join()
    print "Committing ",wdb
    wdb.commit()


