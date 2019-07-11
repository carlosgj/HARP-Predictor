import pymysql as MySQLdb
MySQLdb.install_as_MySQLdb()

import logging
logger = logging.getLogger(__name__)

from datetime import datetime

DRY_RUN = False

def main():
    db = MySQLdb.connect(host='weatherdata.kf5nte.org', user='guest', passwd='',db="Django")
    c = db.cursor()

    c.execute("""SELECT launchTime FROM Predictor_prediction GROUP BY launchTime""")
    launchtimes = [x[0] for x in c.fetchall()]

    c.execute("""SELECT launchPoint_id FROM Predictor_prediction GROUP BY launchPoint_id""")
    launchpoints = [x[0] for x in c.fetchall()]

    c.execute("""SELECT parameters_id FROM Predictor_prediction GROUP BY parameters_id""")
    paramSets = [x[0] for x in c.fetchall()]

    for launchtime in launchtimes:
        for lp in launchpoints:
            for paramSet in paramSets:
                c.execute("""SELECT id, usingPrediction FROM Predictor_prediction WHERE launchTime='%s' AND launchPoint_id=%d AND parameters_id=%d"""%(launchtime.strftime('%Y-%m-%d %H:%M:%S'), lp, paramSet))
                preds = [(x[0], x[1]) for x in c.fetchall()]
                #print launchtime, preds
                #assert False
                if len(preds) > 1:
                    newest = max(preds, key=lambda x:x[1])
                    #print newest
                    #assert False
                    logger.info("Found %d predictions for launch time %s, launchpoint %d, parameter set %d. Most recent: %s. Will delete others."%(len(preds), launchtime.strftime('%Y-%m-%d %H:%M:%S'), lp, paramSet, newest[1].strftime('%Y-%m-%d %H:%M:%S')))
                    #Delete earlier preds
                    for pred in preds:
                        if pred[0] != newest[0]:
                            logger.debug("Deleting trajectory points for prediction %d"%pred[0])
                            if DRY_RUN:
                                print """DELETE FROM Predictor_predictionpoint WHERE prediction_id=%d"""%pred[0]
                            else:
                                c.execute("""DELETE FROM Predictor_predictionpoint WHERE prediction_id=%d"""%pred[0])

                            logger.debug("Deleting prediction ID %d"%pred[0])
                            if DRY_RUN:
                                print """DELETE FROM Predictor_prediction WHERE id=%d"""%pred[0]
                            else:
                                c.execute("""DELETE FROM Predictor_prediction WHERE id=%d"""%pred[0])


    if not DRY_RUN:
        db.commit()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
