import flightPrediction
import MySQLdb
import logging
import datetime
logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


db=MySQLdb.connect(host='delta.carlosgj.org', user='guest', passwd='',db="Django")
c = db.cursor()

#c.execute("""SELECT launchTime, launchLatitude, launchLongitude, launchAltitude, ascentRate, descentRate, burstAltitude, id FROM Predictor_prediction""")
#preds = c.fetchall()

c.execute("""DELETE FROM Predictor_predictionpoint""")
c.execute("""DELETE FROM Predictor_prediction""")

intervalHours = 2
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

currentStep = 0
while currentStep*intervalHours < 384:
    for launchpoint in launchpoints:
        launchPt = flightPrediction.geoTimePoint(launchpoint['latitude'], launchpoint['longitude'], launchpoint['altitude'], latestPredTime+(currentStep*interval))
        print launchPt
        thisPred = flightPrediction.Prediction(launchPt, ascentRate, descentRate, burstAltitude=burstAltitude)
        thisPred.path = []
        try:
            flightPrediction.runPrediction(thisPred)
        except:
            logger.error("Could not run prediction for lauchpoint %d at %s using prediction from %s."%(launchpoint['id'], launchPt.time.strftime("%Y-%m-%d %H:%M:%S"), latestPredTime.strftime("%Y-%m-%d %H:%M:%S")))
            #raise
        else:
            c.execute("""INSERT INTO Predictor_prediction (launchPoint_id, launchTime, burstTime, burstLatitude, burstLongitude, landingTime, landingLatitude, landingLongitude, usingPrediction, ascentRate, descentRate, burstAltitude, landingAltitude) VALUES (%d, '%s', '%s', %f, %f, '%s', %f, %f, '%s', %f, %f, %d, %d)"""%(launchpoint['id'], launchPt.time.strftime('%Y-%m-%d %H:%M:%S'), thisPred.burstPoint.time.strftime('%Y-%m-%d %H:%M:%S'), thisPred.burstPoint.latitude, thisPred.burstPoint.longitude, thisPred.landingPoint.time.strftime('%Y-%m-%d %H:%M:%S'), thisPred.landingPoint.latitude, thisPred.landingPoint.longitude, latestPredTime.strftime('%Y-%m-%d %H:%M:%S'), ascentRate, descentRate, thisPred.burstPoint.elevation, thisPred.landingPoint.elevation))
            thisPredictionID = c.lastrowid
            for point in thisPred.path:
                c.execute("""INSERT INTO Predictor_predictionpoint (latitude, longitude, altitude, time, prediction_id) VALUES (%f, %f, %f, '%s', %d)"""%(point.latitude, point.longitude, point.elevation, point.time.strftime('%Y-%m-%d %H:%M:%S'), thisPredictionID))
    currentStep += 1
db.commit()

