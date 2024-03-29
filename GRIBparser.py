import struct
import datetime
import logging
import re
from GRIBtables import *
#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
    
def parseGRIBfile(filepath):
    fob = open(filepath, 'rb')
    content = fob.read()
    fob.close()
    return parseGRIBdata(content)
    
def parseGRIBdata(content):
    
    results = [] #list of ({GRIBdata}, [tuples]) tuples
    if not content[0:4]==b"GRIB":
        logger.error("Invalid data: does not start with \"GRIB\".")
        return
    gribs = re.split(rb"(?:^|7777)(?:GRIB|$)", content)
    for i, chunk in enumerate(gribs):
        if chunk:
            logger.info("Parsing GRIB #%d of %d"%(i, len(gribs)))
            results.append(parseGRIB(b"GRIB"+chunk+b"7777"))
            #break #debugging
    return results

def convertlatlon(val) :
    signval = 1
    if (val & 0x80000000) == 0x80000000 :
        signval = -1
        val = val & 0x7fffffff
    signed = val * signval
    signed /= 1000000.
    if signed > 180:
        signed -= 360
    return signed

def parseGRIB(content):
    gribData = {
        "isobar":None,
        "param":None,
        "valuetime":None,
        "xScanDir": None,
        "yScanDir": None,
        }
    parameter = None
    disciplineIdx = content[6]
    discipline = disciplines.get(disciplineIdx, None)
    if discipline:
        logger.info("Discipline: %s"%discipline)
    else:
        logger.error("Invalid discipline (%d)!"%disciplineIdx)
        logger.debug(content.encode('hex'))
        return
    GRIBedition = content[7]
    if GRIBedition != 2:
        logger.error("Invalid GRIB edition (%d)!"%GRIBedition)
    logger.debug("GRIB edition: %d"%GRIBedition)
    GRIBlength = struct.unpack('>Q', content[8:16])[0]
    logger.debug("GRIB length: %d"%GRIBlength)
    latLonTuples = []
    refTime = None
    content=content[16:]
    while len(content) > 0:
        logger.debug("\tParsing section...")
        sectLength = struct.unpack('>L', content[:4])[0]
        logger.debug("\t\tSection length: %d"%sectLength)
        print(content[:6])
        sectNum = content[4]
        #logger.debug( "Section number:", sectNum
        if sectNum == 1:
            logger.debug("\t\tIdentification section...")
            origCenter = struct.unpack('>H', content[5:7])[0]
            logger.debug( "\t\tOriginating center: %d"%origCenter)
            origSubCenter = struct.unpack('>H', content[7:9])[0]
            logger.debug( "\t\tOriginating subcenter: %d"%origSubCenter)
            masterTableVersion = content[9]
            logger.debug( "\t\tGRIB master tables version: %d"%masterTableVersion)
            localTableVersion = content[10]
            logger.debug( "\t\tGRIB local tables version: %d"%localTableVersion)
            refTimeSigIdx = content[11]
            refTimeSig = refTimeSignificances.get(refTimeSigIdx, None)
            if refTimeSig:
                logger.debug( "\t\tReference time significance: %s"%refTimeSig)
            year = struct.unpack('>H', content[12:14])[0]
            month = content[14]
            day = content[15]
            hour = content[16]
            minute = content[17]
            second = content[18]
            refTime = datetime.datetime(year, month, day, hour, minute, second)
            logger.debug( "\t\tReference time:"+str(refTime))
            prodStatIdx = content[19]
            prodStat = productionStatuses.get(prodStatIdx, None)
            if prodStat:
                logger.debug( "\t\tProduction status: %s"%prodStat)
            dataTypeIdx = content[20]
            dataType = dataTypes.get(dataTypeIdx, None)
            logger.debug( "\t\tType of processed data: %s"%dataType)
        elif sectNum == 3:
            logger.debug("\t\tGrid definition section...")
            defSourceIdx = content[5]
            if defSourceIdx != 0:
                logger.error("Grid defined by originating center")
                return
            dataPointCount = struct.unpack('>L', content[6:10])[0]
            logger.debug( "\t\tNumber of data points: %d"%dataPointCount)
            if content[10] != 0 or content[11] != 0:
                logger.error("Complex grid format. Aborting.")
                return
            gridDefTempIdx = struct.unpack('>H', content[12:14])[0]
            gridDefTemp = gridDefTemplates.get(gridDefTempIdx, None)
            logger.debug("\t\tGrid definition template: %s"%gridDefTemp)
            if gridDefTempIdx == 0:
                earthShapeIdx = content[14]
                earthShape = earthShapes.get(earthShapeIdx, None)
                logger.debug("\t\tShape of earth: %s"%earthShape)
                assert earthShapeIdx == 6
                assert content[15] == 0
                assert content[16] == 0
                assert content[17] == 0
                assert content[18] == 0
                assert content[19] == 0
                assert content[20] == 0
                assert content[21] == 0
                assert content[22] == 0
                assert content[23] == 0
                assert content[24] == 0
                assert content[25] == 0
                assert content[26] == 0
                assert content[27] == 0
                assert content[28] == 0
                assert content[29] == 0
                pointsAlongParallel = struct.unpack('>L', content[30:34])[0]
                logger.debug("\t\tPoints along parallel: %d"%pointsAlongParallel)
                pointsAlongMeridian = struct.unpack('>L', content[34:38])[0]
                logger.debug("\t\tPoints along meridian: %d"%pointsAlongMeridian)
                assert content[38] == 0
                assert content[39] == 0
                assert content[40] == 0
                assert content[41] == 0
                angleSubdivisions = struct.unpack('>L', content[42:46])[0]
                #assert angleSubdivisions == 0xffffffff
                firstPointLat = struct.unpack('>L', content[46:50])[0]
                firstPointLat = convertlatlon(firstPointLat)
                logger.debug("\t\tFirst point latitude: %f"%firstPointLat)
                firstPointLon = struct.unpack('>L', content[50:54])[0]
                firstPointLon = convertlatlon(firstPointLon)
                logger.debug("\t\tFirst point longitude: %f"%firstPointLon)
                assert content[54] == 48
                lastPointLat = struct.unpack('>L', content[55:59])[0]
                lastPointLat = convertlatlon(lastPointLat)
                logger.debug("\t\tLast point latitude: %f"%lastPointLat)
                lastPointLon = struct.unpack('>L', content[59:63])[0]
                lastPointLon = convertlatlon(lastPointLon)
                logger.debug("\t\tLast point longitude: %f"%lastPointLon)
                assert content[71] & 0x3F == 0 #adjacent points in W/E are consecutive
                if content[71] & 64 == 64: #Points in the first row or column scan W to E, Points in the first row or column scan S to N, adjacent points in W/E are consecutive
                    gribData["yScanDir"] = 1 #Points in the first row or column scan S to N
                else:
                    gribData["yScanDir"] = -1 #Points in the first row or column scan N to S
                if content[71] & 128 == 128:
                    gribData["xScanDir"] = -1 #Points in the first row or column scan E to W
                else:
                    gribData["xScanDir"] = 1 #Points in the first row or column scan W to E

            else:
                logger.error("\tGrid handling not implemented for template: %s. Aborting."%gridDefTemp)
                return
            
            
        elif sectNum == 4:
            logger.debug( "\t\tProduct definition section...")
            numCoordValues = struct.unpack('>H', content[5:7])[0]
            assert numCoordValues == 0
            prodDefTempIdx = struct.unpack('>H', content[7:9])[0]
            prodDefTemp = prodDefTemplates.get(prodDefTempIdx, None)
            if prodDefTemp:
                logger.debug("\t\tProduct definition template: %s"%prodDefTemp)
            else:
                logger.error("\tProduct definition template %d not implemented. Aborting."%prodDefTempIdx)
                return
            paramCatIdx = content[9]
            disciplineCategories = parameterCategories.get(disciplineIdx, None)
            if discipline and (disciplineCategories is not None):
                paramCategory = disciplineCategories.get(paramCatIdx, None)
                if paramCategory:
                    logger.debug("\t\tParameter category: %s"%paramCategory["Category Name"])
                    parameterIdx = content[10]
                    parameter = paramCategory.get(parameterIdx, None)
                    if parameter:
                        logger.debug("\t\tParameter: %s"%parameter)
                        gribData["param"] = parameter
                    else:
                        logger.error("\tParameter %d for category %s not implemented. Aborting."%(parameterIdx, paramCategory["Category Name"]))
                        return
                else:
                    logger.error("\tParameter category %d not implemented. Aborting."%paramCatIdx)
                    return
            else:
                logger.error("\tParameter categories for discipline %d not implemented. Aborting."%disciplineIdx)
                return
            genProcIdx = content[11]
            genProc = generatingProcesses.get(genProcIdx, None)
            if genProc:
                logger.debug("\t\tGenerating process: %s"%genProc)
            assert content[17] ==1
            forecastHour = struct.unpack('>L',content[18:22])[0]
            logger.debug("\t\tForecast hour: %d"%forecastHour)
            interval = datetime.timedelta(hours=forecastHour)
            gribData["valuetime"]=refTime+interval
            try:
                assert content[22] == 100
            except:
                logger.error("content[22] (type of first fixed surface) = %d. Expected 100."%content[22])
                return
            try:
                assert content[23] == 0
            except:
                logger.error("content[23] (scale of first fixed surface) = %d. Expected 0."%content[23])
                return
            mbPress = struct.unpack('>L', content[24:28])[0]
            mbPress /= 100.
            logger.debug("\t\tPressure (mb): %f"%mbPress)
            gribData["isobar"] = int(mbPress)
            assert content[28] == 255
            
        elif sectNum == 5:
            logger.debug("\t\tData representation section...")
            numDataPoints = struct.unpack('>L', content[5:9])[0]
            logger.debug("\t\tNumber of data points: %d"%numDataPoints)
            dataRepTempIdx = struct.unpack('>H', content[9:11])[0]
            dataRepTemp = dataRepresentationTemplates.get(dataRepTempIdx, None)
            if dataRepTemp:
                logger.debug("\t\tData representation template: %s"%dataRepTemp)
            else:
                logger.error("\tData representation template not found for index %d"%dataRepTempIdx)
            if dataRepTempIdx != 0:
                logger.error("\tData representation template \"%s\" not implemented. Aborting."%dataRepTemp)
                return
            referenceValue = struct.unpack('>f', content[11:15])[0]
            logger.debug("\t\tReference value (R): %d"%referenceValue)
            
            binaryScaleFactor = struct.unpack('>H', content[15:17])[0]
            logger.debug("\t\tBinary scale factor (E): %d"%binaryScaleFactor)
            if (binaryScaleFactor & 0x8000) != 0:
                logger.warning("\t\tBSF might be negative!")
            
            decimalScaleFactor = struct.unpack('>H', content[17:19])[0]
            logger.debug("\t\tDecimal scale factor (D): %d"%decimalScaleFactor)
            if (decimalScaleFactor & 0x8000) != 0:
                logger.warning("\t\tDSF might be negative!")
            
            bitsPerPoint = content[19]
            logger.debug("\t\tBits per datapoint: %d"%bitsPerPoint)
                
        elif sectNum == 6:
            logger.debug("\t\tBit map section...")
            assert content[5] == 255
        elif sectNum == 7:
            logger.debug("\t\tData section...")
            blob = content[5:sectLength]
            #convert blob to binary array
            binArray = []
            dataPoints = []
            for char in blob:
                binArray += [(char>>n)&1 for n in range(0,8)[::-1]]
            for i in range(numDataPoints):
                thisPointBinArray = binArray[i*bitsPerPoint:(i+1)*bitsPerPoint]
                thisPoint = 0
                for binVal in thisPointBinArray[::-1]:
                    #print thisPoint, binVal,
                    thisPoint |= binVal
                    thisPoint = thisPoint << 1
                    #print thisPoint
                thisPoint = thisPoint >> 1
                adjustedPoint = (referenceValue + float(thisPoint) * 2**binaryScaleFactor) / 10**decimalScaleFactor
                dataPoints.append(adjustedPoint)
            #print dataPoints
            
            #maxPoint = max(dataPoints)
            #minPoint = min(dataPoints)
            #scalingFactor = 10/(maxPoint-minPoint)
            #scaledPoints = [(x-minPoint)*scalingFactor for x in dataPoints]
            #print [round(x, 1) for x in dataPoints]
            #fob = open("C:\\Users\\carlosj\\Documents\\HAB\\Predictor\\testdata.csv", 'w')
            #for row in [list(x) for x in zip(*[iter(dataPoints)]*pointsAlongParallel)[::-1]]:
            #    #print [round(x, 1) for x in row]
            #    fob.write(','.join([str(round(x, 1)) for x in row]))
            #    fob.write('\r\n')
            #fob.close()
            
            if gribData["yScanDir"] == 1:
                currentLat = firstPointLat
            elif gribData["yScanDir"] == -1:
                currentLat = lastPointLat
            
            latDelta = (lastPointLat - firstPointLat) / (pointsAlongMeridian-1)
            longDelta = (lastPointLon - firstPointLon) / (pointsAlongParallel-1)
            
            dataPointIndex = 0
            for y in range(pointsAlongMeridian):
            
                if gribData["xScanDir"] == 1:
                    currentLon = firstPointLon
                elif gribData["xScanDir"] == -1:
                    currentLon = lastPointLon
                    
                for x in range(pointsAlongParallel):
                    latLonTuples.append((currentLat, currentLon, dataPoints[dataPointIndex]))
                    dataPointIndex += 1
                    currentLon += gribData["xScanDir"]*longDelta
                currentLat += gribData["yScanDir"]*latDelta
            
                    
        else:   
            logger.error("\tError! Unknown section number:", sectNum)
            #logger.debug( content[:20]
            
        content = content[sectLength:]
        if content[:4] == b"7777":
            logger.debug("Returning %d tuples."%len(latLonTuples))
            return (gribData, latLonTuples)
            


if __name__=="__main__":
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    #parseGRIBfile("C:\\Users\\carlosj\\Documents\\HAB\\Predictor\\gfs.t12z.pgrb2.0p25.f010")
    results = parseGRIBfile("C:\\Users\\carlosj\\Documents\\HARP\\Predictor\\Discipline40\\weathererrorl.log.1")
    #for i, foo in enumerate(results):
        #print i+1, foo[0]
    
