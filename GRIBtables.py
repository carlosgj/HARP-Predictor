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

prodDefTemplates = {
    0:"Analysis or forecast at a horizontal level or in a horizontal layer at a point in time"
    }

parameterCategories = {
    0:{ #Meteorological products
        0:{
            "Category Name":"Temperature",
            0:"TMP",
            1:"VTMP",
            2:"POT"
            },
        1:{
            "Category Name":"Moisture"
            },
        2:{
            "Category Name":"Momentum",
            0:"WDIR",
            1:"WIND",
            2:"UGRD",
            3:"VGRD"
            },
        3:{
            "Category Name":"Mass",
            0:"PRES",
            1:"PRMSL",
            2:"PTEND",
            3:"ICAHT",
            4:"GP",
            5:"HGT",
            6:"DIST"
            },
        4:{
            "Category Name":"Short-wave radiation"
            },
        5:{
            "Category Name":"Long-wave radiation"
            },
        6:{
            "Category Name":"Cloud"
            },
        7:{
            "Category Name":"Thermodynamic stability indicies"
            }
        }
    }

dataRepresentationTemplates = {
    0:"Grid Point Data - Simple Packing",
    1:"Matrix Value at Grid Point - Simple Packing",
    2:"Grid Point Data - Complex Packing",
    3:"Grid Point Data - Complex Packing and Spatial Differencing",
    4:"Grid Point Data - IEEE Floating Point Data",
    40:"Grid Point Data - JPEG2000 Compression",
    41:"Grid Point Data - PNG Compression",
    50:"Spectral Data - Simple Packing",
    51:"Spectral Data - Complex Packing",
    61:"Grid Point Data - Simple Packing With Logarithm Pre-processing",
    200:"Run Length Packing With Level Values"
    }

generatingProcesses = {
    0:"Analysis",
    1:"Initialization",
    2:"Forecast",
    3:"Bias Corrected Forecast",
    4:"Ensemble Forecast",
    5:"Probability Forecast",
    6:"Forecast Error",
    7:"Analysis Error",
    8:"Observation",
    9:"Climatological",
    10:"Probability-Weighted Forecast",
    11:"Bias-Corrected Ensemble Forecast",
    12:"Post-processed Analysis",
    13:"Post-processed Forecast",
    14:"Nowcast",
    15:"Hindcast",
    16:"Physical Retrieval",
    17:"Regression Analysis",
    18:"Difference Between Two Forecasts",
    192:"Forecast Confidence Indicator",
    193:"Probability-matched Mean",
    194:"Neighborhood Probability",
    195:"Bias-Corrected and Downscaled Ensemble Forecast",
    196:"Perturbed Analysis for Ensemble Initialization"
    }