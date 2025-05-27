
from math import radians, sin, cos, sqrt, atan2
def harvesine(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points on the Earth using the Haversine formula.
    """

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    r = 6371  # Radius of Earth in kilometers. Use 3956 for miles
    return c * r

