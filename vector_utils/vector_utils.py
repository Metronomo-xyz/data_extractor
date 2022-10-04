import math

def fastCosine(vec1, vec2):
    r = ((vec1 * vec2).sum()) / (math.sqrt(vec1.sum()) * (math.sqrt(vec2.sum())))
    return r