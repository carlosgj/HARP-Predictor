import flightPrediction

def MonteCarlo(pred):
    #generate varied predictions
    preds = []
    ascentRateDeltas = [0.5, -0.5]
    descentRateDeltas = [0.5, -0.5]
    burstHeightDeltas = [5000, -5000]
    ugrdMultipliers = [0.9, 0.1]
    
