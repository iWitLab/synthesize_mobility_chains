

from math import radians, cos, sin, asin, sqrt, floor
from decimal import *


AVG_EARTH_RADIUS = 6371  # in km

class DistanceCalculator:
    distCache = {}

    def __init__(self):
        print("Distance cache was created")






    def haversine(self, point1, point2 ,miles=False):
        """ Calculate the great-circle distance bewteen two points on the Earth surface.

        :input: two 2-tuples, containing the latitude and longitude of each point
        in decimal degrees.

        Example: haversine((45.7597, 4.8422), (48.8567, 2.3508))

        :output: Returns the distance bewteen the two points.
        The default unit is kilometers. Miles can be returned
        if the ``miles`` parameter is set to True.

        """

        # check if cached
        dist = self.get_from_cache(point1, point2)
        if dist is not None:
           return dist

        # unpack latitude/longitude
        lat1, lng1 = point1
        lat2, lng2 = point2

        # convert all latitudes/longitudes from decimal degrees to radians
        lat1, lng1, lat2, lng2 = map(radians, (lat1, lng1, lat2, lng2))

        # calculate haversine
        lat = lat2 - lat1
        lng = lng2 - lng1
        d = sin(lat * 0.5) ** 2 + cos(lat1) * cos(lat2) * sin(lng * 0.5) ** 2
        h = 2 * 1000 * AVG_EARTH_RADIUS * asin(sqrt(d))
        if miles:
            return h * 0.621371  # in miles
        else:
            self.cache_distance(point1,point2,h)
            return h  # in meters



    def cache_distance (self,point1,point2, distance):
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        lat1,long1 = point1
        lat1_e = lat1*1
        long1_e = long1*1
        lat2, long2 = point2
        lat2_e = lat2*1
        long2_e = long2*1
        self.distCache[(lat1_e,long1_e,lat2_e,long2_e)] = distance



    def get_from_cache (self, point1, point2):
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        lat1, long1 = point1
        lat1_e = lat1*1
        long1_e = long1*1
        lat2, long2 = point2
        lat2_e = lat2*1
        long2_e = long2*1
        if (lat1_e,long1_e,lat2_e,long2_e) in self.distCache:
            return self.distCache[(lat1_e,long1_e,lat2_e,long2_e)]
        if (lat2_e,long2_e,lat1_e,long1_e) in self.distCache:
            return self.distCache[(lat2_e,long2_e,lat1_e,long1_e)]

    def estimate_distance_is_smaller_than_threshold (self, point1,point2, threshold):
        precision = -1
        #5 decimal place - precision of 1.11 m
        if threshold > 1.11:
            precision = 7
        # 4 decimal place - precision of 11.1 m
        if threshold > 11.1:
            precision = 6
        # 3 decimal place - precision of 111 m
        if threshold > 111:
            precision = 5
        # 2 decimal place - precision of 1.11 km
        if threshold > 1110:
            precision = 4
        # 2 decimal place - precision of 11.1 km
        if threshold > 11100:
            precision = 3

        lat1, long1 = point1
        lat2, long2 = point2

        if precision > -1:
            getcontext().prec = precision
            getcontext().rounding = ROUND_DOWN
            if lat1*1 == lat2*1 and long1*1 == long2*1:
                return True
        dist = self.haversine(point1,point2)
        if dist <= threshold:
            return True
        else:
            return  False

    def estimate_are_similar_points (self, point1, point2):
        precision = 6
        lat1, long1 = point1
        lat2, long2 = point2
        getcontext().prec = precision
        getcontext().rounding = ROUND_DOWN
        if lat1*1 == lat2*1 and long1*1 == long2*1:
            return True
        return False




