import struct
import datetime
import logging
#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

disciplines = {0:"Meteorological", 1:"Hydrological", 2:"Land Surface", 3:"Space", 4:"Space (Validation)", 10:"Oceanographic"}

refTimeSignificances = {0:"Analysis", 1:"Start of forecast", 2:"Verifying time of forecast", 3:"Observation time", 255:None}

productionStatuses = {0:"Operational", 1:"Operational test", 2:"Research", 3:"Re-analysis", 4:"TIGGE", 5:"TIGGE test", 6:"S2S operational", 7:"S2S test", 8:"UERRA", 9:"UERRA test", 255:None}

dataTypes = {0:"Analysis", 1:"Forecast", 2:"Analysis and forecast", 3:"Control forecast", 4:"Perturbed forecast", 5:"Control and perturbed forecast", 6:"Processed satellite observations", 7:"Processed RADAR observations", 8:"Event probability", 192:"Experimental", 255:None}

gridDefTemplates = {0:"Latitude/longitude", 1:"Rotated latitude/longitude", 2:"Stretched latitude/longitude", 3:"Rotated and stretched latitude/longitude", 4:"Variable resolution latitude/longitude", 5:"Variable resolution rotated latitude/longitude", 10:"Mercator", 12:"Transverse mercator", 20:"Polar stereographic projection", 30:"Lambert conformal", 31:"Albers equal area", 40:"Gaussian latitude/longitude", 41:"Rotated Gaussian latitude/longitude", 42:"Stretched Gaussian latitude/longitude", 43:"Rotated and stretched Gaussian latitude/longitude", 50:"Spherical harmonic coefficients", 51:"Rotated spherical harmonic coefficients", 52:"Stretched spherical harmonic coefficients", 53:"Rotated and stretched spherical harmonic coefficients", 90:"Space view perspective or orthographic", 100:"Triangular grid based on an icosahedron", 101:"General unstructured grid", 110:"Equatorial azimuthal equidistant projection", 120:"Azimuth-range projection", 140:"Lambert azimuthal equal-area projection", 204:"Curvilinear orthogonal grids", 1000:"Cross-section grid with points equally spaced on the horizontal", 1100:"Hovmoller grid with points equally spaced on the horizontal", 1200:"Time section grid", 32768:"Rotated latitude/longitude (Arakawa staggered e-grid)", 32769:"Rotated latitude/longitude (Arakawa non-e staggered grid"}


earthShapes = {0:"Earth assumed spherical with radius = 6,367,470.0 m",
    1:"Earth assumed spherical with radius specified (in m) by data producer",
    2:"Earth assumed oblate spheriod with size as determined by IAU in 1965 (major axis = 6,378,160.0 m, minor axis = 6,356,775.0 m, f = 1/297.0)",
    3:"Earth assumed oblate spheriod with major and minor axes specified (in km) by data producer",
    4:"Earth assumed oblate spheriod as defined in IAG-GRS80 model (major axis = 6,378,137.0 m, minor axis = 6,356,752.314 m, f = 1/298.257222101)",
    5:"Earth assumed represented by WGS84 (as used by ICAO since 1998) (Uses IAG-GRS80 as a basis)",
    6:"Earth assumed spherical with radius = 6,371,229.0 m",
    7:"Earth assumed oblate spheroid with major and minor axes specified (in m) by data producer",
    8:"Earth model assumed spherical with radius 6,371,200 m, but the horizontal datum of the resulting Latitude/Longitude field is the WGS84 reference frame",
    9:"Earth represented by the OSGB 1936 Datum, using the Airy_1830 Spheroid, the Greenwich meridian as 0 Longitude, the Newlyn datum as mean sea level, 0 height"
    }
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
            break #debugging

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
    logger.info("GRIB edition: %d"%GRIBedition)
    GRIBlength = struct.unpack('>Q', content[8:16])[0]
    logger.info("GRIB length: %d"%GRIBlength)
    
    content=content[16:]
    while len(content) > 0:
        logger.debug("\tParsing section...")
        sectLength = struct.unpack('>L', content[:4])[0]
        logger.debug("\t\tSection length: %d"%sectLength)
        sectNum = ord(content[4])
        #logger.debug( "Section number:", sectNum
        if sectNum == 1:
            logger.info("\t\tIdentification section...")
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
            logger.info("\t\tGrid definition section...")
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
                assert ord(content[71]) == 64
            else:
                logger.error("\tGrid handling not implemented for template: %s. Aborting."%gridDefTemp)
                return
            
            
        elif sectNum == 4:
            logger.info( "\t\tProduct definition section...")
            
        elif sectNum == 5:
            logger.info("\t\tData representation section...")
            
        elif sectNum == 6:
            logger.info("\t\tBit map section...")
            
        elif sectNum == 7:
            logger.info("\t\tData section...")
            
        else:   
            logger.error("\tError! Unknown section number:", sectNum)
            #logger.debug( content[:20]
            
        #logger.debug( '\n'
        content = content[sectLength:]
        if content[:4] == "7777":
            break


if __name__=="__main__":
    parseGRIBfile("C:\\Users\\carlosj\\Documents\\HAB\\Predictor\\gfs.t12z.pgrb2full.0p50.f009")