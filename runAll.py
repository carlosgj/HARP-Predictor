import flightPrediction
import MySQLdb
import logging
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import cpu_count as cpu_count
pool = ThreadPool(1)
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


db=MySQLdb.connect(host='localhost', user='guest', passwd='',db="Django")
c = db.cursor()

#c.execute("""SELECT launchTime, launchLatitude, launchLongitude, launchAltitude, ascentRate, descentRate, burstAltitude, id FROM Predictor_prediction""")
#preds = c.fetchall()

c.execute("""DELETE FROM Predictor_predictionpoint""")
c.execute("""DELETE FROM Predictor_prediction""")

intervalHours = 4
ascentRate = 7
descentRate = 15
burstAltitude=60000

interval = datetime.timedelta(hours=intervalHours)

#Get time of lastest weather data
c.execute("""USE test_dataset""")
c.execute("""SELECT * FROM setIndex ORDER BY predictionTime DESC LIMIT 1""")
latestPred = c.fetchall()[0]
latestPredTable = latestPred[0]
latestPredTime = latestPred[1]

#Get all launch locations
c.execute("""USE Django""")
c.execute("""SELECT id, latitude, longitude, altitude FROM Predictor_launchlocation""")
_launchpoints = c.fetchall()
launchpoints = []
for i in _launchpoints:
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

def runit(pred):
    print "Running prediction for ", pred.launchPoint.time
    try:
        flightPrediction.runPrediction(pred)
    except:
        logger.error("Could not run prediction for launchpoint %d at %s using prediction from %s. Last point:%s"%(pred.launchPointID, pred.launchPoint.time.strftime("%Y-%m-%d %H:%M:%S"), latestPredTime.strftime("%Y-%m-%d %H:%M:%S"), str(pred.path[-1])))
        #raise
    else:
        #c.execute("""INSERT INTO Predictor_prediction (launchPoint_id, launchTime, burstTime, burstLatitude, burstLongitude, landingTime, landingLatitude, landingLongitude, usingPrediction, ascentRate, descentRate, burstAltitude, landingAltitude) VALUES (%d, '%s', '%s', %f, %f, '%s', %f, %f, '%s', %f, %f, %d, %d)"""%(pred.launchPointID, pred.launchPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.burstPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.burstPoint.latitude, pred.burstPoint.longitude, pred.landingPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.landingPoint.latitude, pred.landingPoint.longitude, latestPredTime.strftime('%Y-%m-%d %H:%M:%S'), ascentRate, descentRate, pred.burstPoint.elevation, pred.landingPoint.elevation))
        c.execute("""INSERT INTO Predictor_prediction (launchPoint_id, launchTime, usingPrediction, ascentRate, descentRate) VALUES (%d, '%s', '%s', %f, %f)"""%(pred.launchPointID, pred.launchPoint.time.strftime('%Y-%m-%d %H:%M:%S'), latestPredTime.strftime('%Y-%m-%d %H:%M:%S'), ascentRate, descentRate))
        thisPredictionID = c.lastrowid
        if pred.burstPoint:
            c.execute("""UPDATE Predictor_prediction SET burstTime='%s', burstLatitude=%f, burstLongitude=%f, burstAltitude=%d WHERE id=%d"""%(pred.burstPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.burstPoint.latitude, pred.burstPoint.longitude, pred.burstPoint.elevation, thisPredictionID))
        if pred.landingPoint:
            c.execute("""UPDATE Predictor_prediction SET landingTime='%s', landingLatitude=%f, landingLongitude=%f, landingAltitude=%d WHERE id=%d"""%(pred.landingPoint.time.strftime('%Y-%m-%d %H:%M:%S'), pred.landingPoint.latitude, pred.landingPoint.longitude, pred.landingPoint.elevation, thisPredictionID))
        if not pred.path:
            assert False
        for point in pred.path:
            #print point
            c.execute("""INSERT INTO Predictor_predictionpoint (latitude, longitude, altitude, groundElevation, time, prediction_id) VALUES (%f, %f, %f, %f, '%s', %d)"""%(point.latitude, point.longitude, point.elevation, point.groundAlt, point.time.strftime('%Y-%m-%d %H:%M:%S'), thisPredictionID))


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
for pred in allPredictions:
    runit(pred)
    db.commit()
    #assert False
db.commit()

