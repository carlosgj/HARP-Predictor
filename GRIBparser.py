import struct
import datetime
import logging
from GRIBtables import *
#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)
    
def parseGRIBfile(filepath):
    fob = open(filepath, 'rb')
    content = fob.read()
    fob.close()
    if not content[0:4]=="GRIB":
        logger.error("Invalid file: does not start with \"GRIB\".")
        return
    gribs = content.split("GRIB")
    for i, chunk in enumerate(gribs):
        if chunk:
            logger.warning("Parsing GRIB #%d of %d"%(i, len(gribs)))
            parseGRIB("GRIB"+chunk)
            #break #debugging

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
        "valuetime":None
        }
    parameter = None
    disciplineIdx = ord(content[6])
    discipline = disciplines.get(disciplineIdx, None)
    if discipline:
        logger.info("Discipline: %s"%discipline)
    else:
        logger.error("Invalid discipline (%d)!"%disciplineIdx)
        return
    GRIBedition = ord(content[7])
    if GRIBedition != 2:
        logger.error("Invalid GRIB edition (%d)!"%GRIBedition)
    logger.debug("GRIB edition: %d"%GRIBedition)
    GRIBlength = struct.unpack('>Q', content[8:16])[0]
    logger.debug("GRIB length: %d"%GRIBlength)
    
    content=content[16:]
    while len(content) > 0:
        logger.debug("\tParsing section...")
        sectLength = struct.unpack('>L', content[:4])[0]
        logger.debug("\t\tSection length: %d"%sectLength)
        sectNum = ord(content[4])
        #logger.debug( "Section number:", sectNum
        if sectNum == 1:
            logger.debug("\t\tIdentification section...")
            origCenter = struct.unpack('>H', content[5:7])[0]
            logger.debug( "\t\tOriginating center: %d"%origCenter)
            origSubCenter = struct.unpack('>H', content[7:9])[0]
            logger.debug( "\t\tOriginating subcenter: %d"%origSubCenter)
            masterTableVersion = ord(content[9])
            logger.debug( "\t\tGRIB master tables version: %d"%masterTableVersion)
            localTableVersion = ord(content[10])
            logger.debug( "\t\tGRIB local tables version: %d"%localTableVersion)
            refTimeSigIdx = ord(content[11])
            refTimeSig = refTimeSignificances.get(refTimeSigIdx, None)
            if refTimeSig:
                logger.debug( "\t\tReference time significance: %s"%refTimeSig)
            year = struct.unpack('>H', content[12:14])[0]
            month = ord(content[14])
            day = ord(content[15])
            hour = ord(content[16])
            minute = ord(content[17])
            second = ord(content[18])
            refTime = datetime.datetime(year, month, day, hour, minute, second)
            logger.debug( "\t\tReference time:"+str(refTime))
            prodStatIdx = ord(content[19])
            prodStat = productionStatuses.get(prodStatIdx, None)
            if prodStat:
                logger.debug( "\t\tProduction status: %s"%prodStat)
            dataTypeIdx = ord(content[20])
            dataType = dataTypes.get(dataTypeIdx, None)
            logger.debug( "\t\tType of processed data: %s"%dataType)
        elif sectNum == 3:
            logger.debug("\t\tGrid definition section...")
            defSourceIdx = ord(content[5])
            if defSourceIdx != 0:
                logger.error("Grid defined by originating center")
                return
            dataPointCount = struct.unpack('>L', content[6:10])[0]
            logger.debug( "\t\tNumber of data points: %d"%dataPointCount)
            if ord(content[10]) != 0 or ord(content[11]) != 0:
                logger.error("Complex grid format. Aborting.")
                return
            gridDefTempIdx = struct.unpack('>H', content[12:14])[0]
            gridDefTemp = gridDefTemplates.get(gridDefTempIdx, None)
            logger.debug("\t\tGrid definition template: %s"%gridDefTemp)
            if gridDefTempIdx == 0:
                earthShapeIdx = ord(content[14])
                earthShape = earthShapes.get(earthShapeIdx, None)
                logger.debug("\t\tShape of earth: %s"%earthShape)
                assert earthShapeIdx == 6
                assert ord(content[15]) == 0
                assert ord(content[16]) == 0
                assert ord(content[17]) == 0
                assert ord(content[18]) == 0
                assert ord(content[19]) == 0
                assert ord(content[20]) == 0
                assert ord(content[21]) == 0
                assert ord(content[22]) == 0
                assert ord(content[23]) == 0
                assert ord(content[24]) == 0
                assert ord(content[25]) == 0
                assert ord(content[26]) == 0
                assert ord(content[27]) == 0
                assert ord(content[28]) == 0
                assert ord(content[29]) == 0
                pointsAlongParallel = struct.unpack('>L', content[30:34])[0]
                logger.debug("\t\tPoints along parallel: %d"%pointsAlongParallel)
                pointsAlongMeridian = struct.unpack('>L', content[34:38])[0]
                logger.debug("\t\tPoints along meridian: %d"%pointsAlongMeridian)
                assert ord(content[38]) == 0
                assert ord(content[39]) == 0
                assert ord(content[40]) == 0
                assert ord(content[41]) == 0
                angleSubdivisions = struct.unpack('>L', content[42:46])[0]
                assert angleSubdivisions == 0xffffffff
                firstPointLat = struct.unpack('>L', content[46:50])[0]
                firstPointLat = convertlatlon(firstPointLat)
                logger.debug("\t\tFirst point latitude: %f"%firstPointLat)
                firstPointLon = struct.unpack('>L', content[50:54])[0]
                firstPointLon = convertlatlon(firstPointLon)
                logger.debug("\t\tFirst point longitude: %f"%firstPointLon)
                assert ord(content[54]) == 48
                lastPointLat = struct.unpack('>L', content[55:59])[0]
                lastPointLat = convertlatlon(lastPointLat)
                logger.debug("\t\tLast point latitude: %f"%lastPointLat)
                lastPointLon = struct.unpack('>L', content[59:63])[0]
                lastPointLon = convertlatlon(lastPointLon)
                logger.debug("\t\tLast point longitude: %f"%lastPointLon)
                assert ord(content[71]) == 64 #Points in the first row or column scan W to E, Points in the first row or column scan S to N, adjacent points in W/E are consecutive
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
            paramCatIdx = ord(content[9])
            disciplineCategories = parameterCategories.get(disciplineIdx, None)
            if discipline:
                paramCategory = disciplineCategories.get(paramCatIdx, None)
                if paramCategory:
                    logger.debug("\t\tParameter category: %s"%paramCategory["Category Name"])
                    parameterIdx = ord(content[10])
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
            genProcIdx = ord(content[11])
            genProc = generatingProcesses.get(genProcIdx, None)
            if genProc:
                logger.debug("\t\tGenerating process: %s"%genProc)
            assert ord(content[17]) ==1
            forecastHour = struct.unpack('>L',content[18:22])
            logger.debug("\t\tForecast hour: %d"%forecastHour)
            print ord(content[22])
            assert ord(content[22]) == 100
            assert ord(content[23]) == 0
            mbPress = struct.unpack('>L', content[24:28])[0]
            mbPress /= 100.
            logger.debug("\t\tPressure (mb): %f"%mbPress)
            gribData["isobar"] = int(mbPress)
            assert ord(content[28]) == 255
            
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
            
            bitsPerPoint = ord(content[19])
            logger.debug("\t\tBits per datapoint: %d"%bitsPerPoint)
                
        elif sectNum == 6:
            logger.debug("\t\tBit map section...")
            assert ord(content[5]) == 255
        elif sectNum == 7:
            logger.debug("\t\tData section...")
            blob = content[5:sectLength]
            #convert blob to binary array
            binArray = []
            dataPoints = []
            for char in blob:
                binArray += [(ord(char)>>n)&1 for n in range(0,8)[::-1]]
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
            fob = open("C:\\Users\\carlosj\\Documents\\HAB\\Predictor\\testdata.csv", 'w')
            for row in [list(x) for x in zip(*[iter(dataPoints)]*pointsAlongParallel)[::-1]]:
                #print [round(x, 1) for x in row]
                fob.write(','.join([str(round(x, 1)) for x in row]))
                fob.write('\r\n')
            fob.close()
            
            #currentLat = firstPointLat
            #latDelta = (lastPointLat - firstPointLat) / (pointsAlongMeridian-1)
            #longDelta = (lastPointLon - firstPointLon) / (pointsAlongParallel-1)
            #latLonTuples = []
            #dataPointIndex = 0
            #for y in range(pointsAlongMeridian):
            #    currentLon = firstPointLon
            #    for x in range(pointsAlongParallel):
            #        latLonTuples.append((currentLat, currentLon, dataPoints[dataPointIndex]))
            #        dataPointIndex += 1
            #        currentLon += longDelta
            #    currentLat += latDelta
                    
        else:   
            logger.error("\tError! Unknown section number:", sectNum)
            #logger.debug( content[:20]
            
        content = content[sectLength:]
        if content[:4] == "7777":
            print gribData
            break


if __name__=="__main__":
    parseGRIBfile("C:\\Users\\carlosj\\Documents\\HAB\\Predictor\\gfs.t12z.pgrb2.0p25.f010")