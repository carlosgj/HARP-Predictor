import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
import MySQLdb

db=MySQLdb.connect(host='weatherdata.kf5nte.org', user='readonly', passwd='',db="test_dataset")
c = db.cursor()

#ANALYSIS_LATITUDE = 33 
#ANALYSIS_LONGITUDE = -121
#ANALYSIS_ISOBAR = 100

c.execute("""SELECT * FROM setIndex""")
predictionSets = c.fetchall()
c.execute("""SELECT DISTINCT(Isobar) FROM %s"""%predictionSets[0][0])
all_isobars = [x[0] for x in c.fetchall()]
c.execute("""SELECT DISTINCT(Latitude) FROM %s"""%predictionSets[0][0])
all_lats = [x[0] for x in c.fetchall()]
c.execute("""SELECT DISTINCT(Longitude) FROM %s"""%predictionSets[0][0])
all_longs = [x[0] for x in c.fetchall()]
print all_isobars
print all_lats
print all_longs
for ANALYSIS_ISOBAR in all_isobars:
    if ANALYSIS_ISOBAR < 550:
        continue
    print "Analyzing isobar %d..."%ANALYSIS_ISOBAR
    for ANALYSIS_LATITUDE in all_lats:
        print "Analyzing latitude %f..."%ANALYSIS_LATITUDE
        for ANALYSIS_LONGITUDE in all_longs:
            print "Analyzing longitude %f..."%ANALYSIS_LONGITUDE
            predictedValuesDict = {}
            for predictionSet in predictionSets:
                tableName = predictionSet[0]
                predictionTime = predictionSet[1]
                c.execute("""SELECT ValueTime, HGT, TMP, UGRD, VGRD FROM %s WHERE Latitude=%f AND Longitude=%f AND Isobar=%d"""%(tableName, ANALYSIS_LATITUDE, ANALYSIS_LONGITUDE, ANALYSIS_ISOBAR))
                thisPointPredictions = c.fetchall()
                for prediction in thisPointPredictions:
                    valueTime = prediction[0]
                    valueTimeStr = valueTime.strftime("%y-%m-%d %H:%M:%S")
                    if valueTimeStr not in predictedValuesDict:
                        predictedValuesDict[valueTimeStr] = []
                    dataDict = {
                        "predTime": predictionTime,
                        "HGT": prediction[1],
                        "TMP": prediction[2],
                        "UGRD": prediction[3],
                        "VGRD": prediction[4],
                        }
                    predictedValuesDict[valueTimeStr].append(dataDict)
                    
            slopes = {'TMP':[], 'HGT':[], 'UGRD':[], 'VGRD':[]}
                    
            for valueTime, testItem in predictedValuesDict.iteritems():
                testItem.sort(key=lambda x: x['predTime'])
                normalizationValues = testItem[-1]
                normalizedValues = []
                for foo in testItem:
                    normalizedData = {}
                    for key, val in foo.iteritems():
                        if key != 'predTime':
                            if normalizationValues[key] != 0:
                                normalizedData[key] = abs((float(val)-normalizationValues[key])/normalizationValues[key])
                            else:
                                print normalizationValues
                                normalizedData[key] = 0
                        else:
                            normalizedData[key] = val-normalizationValues[key]
                    normalizedValues.append(normalizedData)
                #print normalizedValues
                xvals = np.array([x['predTime'].total_seconds()/3600 for x in normalizedValues])
                xvalColumn = xvals[:,np.newaxis]

                for key in normalizationValues.keys():
                    if key != 'predTime':
                        a, _, _, _ = np.linalg.lstsq(xvalColumn, [x[key] for x in normalizedValues])
                        slopes[key].append(a[0])

            for key in slopes:
                print key, np.mean(slopes[key])
            c.execute("""INSERT INTO Quality (Latitude, Longitude, Isobar, HGT, TMP, UGRD, VGRD) VALUES (%f, %f, %d, %f, %f, %f, %f)"""%(ANALYSIS_LATITUDE, ANALYSIS_LONGITUDE, ANALYSIS_ISOBAR, np.mean(slopes['HGT']), np.mean(slopes['TMP']), np.mean(slopes['UGRD']), np.mean(slopes['VGRD']) ) )
            db.commit()
            #plt.plot(xvals, [x['TMP'] for x in normalizedValues], 'ro-', xvals, [x['HGT'] for x in normalizedValues], 'bo-', xvals, [x['UGRD'] for x in normalizedValues], 'go-', xvals, [x['VGRD'] for x in normalizedValues], 'yo-')
            #plt.show()