

# coding: utf-8




# meters
distance_threshold = 1000
# seconds
stop_thrshold = 60 * 60
# seconds
new_time_based_chain_t = 30 * 60
# seconds
stop_point_duration_t = 0.5 * 60 * 60
# meter
stop_point_roaming_t = 1000
# seconds
short_path_t = 5 * 60
#seconds
max_time_oulier_filter = 30 * 60
#meter per second (= 200 km/h)
max_meter_per_second_outlier_filter = 55.5556
#meter per second (= 100 km/h)
#max_meter_per_second_outlier_filter = 27.7778


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def count_split_chains_time_based(locations):
    prev_time = locations[0].date
    out = 1
    # chains = []
    for location in locations:
        cur_time = location.date
        delta_time = cur_time - prev_time
        if delta_time.seconds + delta_time.days * 24 * 360 > new_time_based_chain_t:
            out = out + 1
        prev_time = location.date
    return out


def split_chains_time_based(locations):
    prev_time = locations[0].date
    out = []
    chains = []
    for location in locations:
        cur_time = location.date
        delta_time = cur_time - prev_time
        if delta_time.seconds + delta_time.days * 24 * 360 <= new_time_based_chain_t:
            chains.append(location)
        else:
            out.append(chains)
            chains = []
            chains.append(location)
        prev_time = location.date
    out.append(chains)
    return out


def is_diameter_smaller_than_threshold(locations_in, init_j_star,threshold, distCalc):
    for i in range(0, len(locations_in)):
        for j in range(init_j_star , len(locations_in)):
            if not distCalc.estimate_distance_is_smaller_than_threshold((locations_in[i].lat, locations_in[i].long), (locations_in[j].lat, locations_in[j].long),threshold):
                return False
    return True





def is_diameter_bigger_than_threshold(locations_in, threshold, distCalc):
    for i in range(0, len(locations_in)):
        for j in range(len(locations_in)-1,i ,-1):
            if distCalc.haversine((locations_in[i].lat, locations_in[i].long), (locations_in[j].lat, locations_in[j].long), False) > threshold:
                return True
    return False


def stop_points_find_j_star(locations, start_ind, threshold):
    start_ind_time = locations[start_ind].date
    for i in range(start_ind + 1, len(locations)):
        time_inter = locations[i].date - start_ind_time
        if time_inter.seconds + time_inter.days * 24 * 3600 >= threshold:
            return True, i
        #start_ind_time = locations[i].date
    return False, -1


def stop_points_find_j_star_dia(locations_in, init_j_star,threshold, distCals):
    j = init_j_star + 1
    while j < len(locations_in):
        if not is_diameter_smaller_than_threshold(locations_in[0:j + 1],j ,threshold, distCals):
            return True, j - 1
        j += 1
    return True, len(locations_in) - 1

    #j = len(locations) - 1
    #while j > start_ind:
     #   if is_diameter_smaller_than_threshold(locations[start_ind:j + 1],init_j_star,threshold, distCals):
      #      return True, j
       # j -= 1
    #return False, -1



def merge_close_points (locations):
    disCalc = DistanceCalculator()
    locations_out = []
    prev_point = (locations[0].lat, locations[0].long)
    first_point_in_series =  0
    i = 0
    for i in range(1,len(locations)):
        #if the points can't be merged
        if not(disCalc.estimate_are_similar_points(prev_point, (locations[i].lat, locations[i].long))):
            if first_point_in_series < i - 1:
                locations_out.append(locations[first_point_in_series])
            locations_out.append(locations[i-1])
            first_point_in_series = i
        prev_point =(locations[i].lat, locations[i].long)

    if first_point_in_series < i :
        locations_out.append(locations[first_point_in_series])
    locations_out.append(locations[i])
    return locations_out


def find_median_time (times):
    if len(times) > 0:
        if len(times) == 1:
            return times[0]
        if len(times) % 2 == 1:
            return times [int((len(times)-1)/2)]
        else:
            time_inter = times[int(len(times) /2)] - times[int((len(times)/2) - 1)]
            return times[int((len(times)/2) - 1)] + timedelta (0,int((time_inter.days * 24 * 3600 + time_inter.seconds)/2))


def merge_gas_stations (trajectory):
    out = []
    prev_sp = trajectory[0][1]
    times = []
    times.append(trajectory[0][0])
    for i in range(1,len(trajectory)):
        #if the points can be merged
        if prev_sp == trajectory[i][1]:
            times.append(trajectory[i][0])
        else:
            out.append((find_median_time(times),prev_sp))
            times = []
            times.append(trajectory[i][0])
        prev_sp =trajectory[i][1]

    out.append((find_median_time(times),prev_sp))
    return out



def add_location_to_chain (chain, location):
    if len(chain) > 0:
        prev_loc = chain[len(chain) - 1]
        if not (prev_loc.lat == location.lat and prev_loc.long == location.long):
            chain.append(location)
    else:
        chain.append(location)
    return  chain


def esp_linear_neighbors (q,traj):
    disCalc = DistanceCalculator()

    left = q
    right = q
    # find the leftmost index that satisfy the esp_linear_neighbors:
    cont = True
    index = q - 1
    while cont and index >= 0:
        if disCalc.haversine ((traj[q].lat,traj[q].long), (traj[index].lat,traj[index].long)) <= stop_point_roaming_t:
            left = q
        else:
            cont = False
        index = index - 1
    # find the rightmost index that satisfy the esp_linear_neighbors:
    cont = True
    index = q + 1
    while cont and index <= len(traj) - 1:
        if disCalc.haversine((traj[q].lat, traj[q].long), (traj[index].lat, traj[index].long)) <= stop_point_roaming_t:
            right = q
        else:
            cont = False
        index = index + 1
    return left,right





def TrajDBSCAN (locations):
    out = []
    processed = []
    time_inter = locations[len(locations) - 1].date - locations[0].date
    # start algorithm only if the time interval between the start and end of chain > stop_point_duration_t
    if time_inter.seconds + time_inter.days * 24 * 3600 > stop_point_duration_t:
        while i < len(locations):
            if i not in processed:
                processed.append(i)
                left, right = esp_linear_neighbors (i,locations)
                time_inter = locations[right].date - locations[left].date
                if time_inter.seconds + time_inter.days * 24 * 3600 > stop_point_duration_t:
                    stop = []
                    stop.append(i)
                    for j in range(left, right + 1):
                        if j not in processed:
                            stop.append(j)
                            left_j, right_j = esp_linear_neighbors(j, locations)

def compute_mean (trajectory, type):
    getcontext().prec = 6
    sum = 0
    if type == "lat":
        for point in trajectory:
            sum += point.lat
    else:
        for point in trajectory:
            sum += point.long
    return sum/len(trajectory)

#http://dl.acm.org/citation.cfm?id=1921596
def stay_points_detection (trajectory):
    out = []
    chain = []
    stay_points = []
    disCalc = DistanceCalculator()
    i = 0
    while i < len(trajectory):
        j = i + 1
        while j < len(trajectory):
            dist = disCalc.haversine ((trajectory[i].lat, trajectory[i].long), (trajectory[j].lat, trajectory[j].long))
            if dist > stop_point_roaming_t:
                time_interval = trajectory[j-1].date - trajectory[i].date
                #if stay point
                if time_interval.seconds + time_interval.days * 24 * 3600 > stop_point_duration_t:
                    s_lat = compute_mean(trajectory[i:j],"lat")
                    s_long = compute_mean(trajectory[i:j],"long")
                    chain = add_location_to_chain(chain,Row(date=trajectory[i].date, lat=s_lat, long=s_long))
                    out.append(chain)
                    chain = []
                    chain = add_location_to_chain(chain,Row(date=trajectory[j].date, lat=s_lat, long=s_long))
                    chain = add_location_to_chain(chain,trajectory[j])
                    stay_points.append(Row(lat = s_lat,long = s_long, start = trajectory[i].date, end = trajectory[j-1].date))
                #if not stay point (the time interval is not big enough)
                else:
                    for ind in range(i, j+1):
                        chain = add_location_to_chain(chain,trajectory[ind])
                i = j
                break;
            j += 1
        if j == len(trajectory):
            time_interval = trajectory[j - 1].date - trajectory[i].date
            # if stay point
            if time_interval.seconds + time_interval.days * 24 * 3600 > stop_point_duration_t:
                s_lat = compute_mean(trajectory[i:j], "lat")
                s_long = compute_mean(trajectory[i:j], "long")
                chain = add_location_to_chain(chain,Row(date=trajectory[i].date, lat=s_lat, long=s_long))
                stay_points.append(Row(lat=s_lat, long=s_long, start=trajectory[i].date, end=trajectory[j - 1].date))
            else:
                for ind in range(i, j):
                    chain = add_location_to_chain(chain, trajectory[ind])
            break;
    out.append(chain)
    return out




# http://www.kentarotoyama.org/papers/Hariharan_2004_Project_Lachesis.pdf
def split_chains_stop_points_based(locations):
    out = []
    chain = []
    disCalc = DistanceCalculator()
    time_inter = locations[len(locations) - 1].date - locations[0].date
    # start algorithm only if the time interval between the start and end of chain > stop_point_duration_t
    if time_inter.seconds + time_inter.days * 24 * 3600 > stop_point_duration_t:
        i = 0
        continue_1 = True
        while i < len(locations) and continue_1:
            is_p_stop, j_star_init = stop_points_find_j_star(locations, i, stop_point_duration_t)
            #######   Diameter condition ######
            # continue only if j* exists
            if is_p_stop:
                # check if the diameter > stop_point_roaming_t
                is_not_stop_dia = is_diameter_bigger_than_threshold(locations[i : j_star_init + 1], stop_point_roaming_t,disCalc)
                if is_not_stop_dia:
                    chain = add_location_to_chain(chain,locations[i])
                    i += 1
                else:
                    last_ind = len(locations)
                    is_stop, j_star = stop_points_find_j_star_dia(locations[i:last_ind],j_star_init - i,stop_point_roaming_t, disCalc)
                    j_star += i
                    if is_stop:
                        s_lat = compute_mean (locations[i:j_star + 1],"lat")
                        s_long = compute_mean (locations[i:j_star + 1],"long")
                        chain = add_location_to_chain(chain, Row(date=locations[i].date, lat=s_lat, long=s_long))
                        out.append(chain)
                        chain = []
                        chain = add_location_to_chain(chain, Row(date=locations[j_star].date, lat=s_lat, long=s_long))
                    i = j_star + 1
            else:
                continue_1 = False
                for ind in range(i,len(locations)):
                    chain = add_location_to_chain(chain, locations[ind])
                #chain.append(locations[i:len(locations)])
        if len(chain) > 0:
            out.append(chain)
    else:
        for ind in range(0, len(locations)):
            chain = add_location_to_chain(chain, locations[ind])
        #chain.append(locations[0:len(locations)])
        if len(chain) > 0:
                out.append(chain)
    return out



def match_gas_station(locations, sp):
    chains_out = []
    if locations is not None:
        for location in locations:
            gas_station, dist = find_closest_SP((location.lat, location.long), sp)
            if dist != -1:
                chains_out.append((location.date,gas_station))
        if len(chains_out) > 1:
            return chains_out






# http://www.plumislandmedia.net/mysql/haversine-mysql-nearest-loc/
def find_closest_SP(point, sp):
    disCalc = DistanceCalculator()
    min_sp = 0;
    min_dist = 1000;
    lat1, lng1 = point
    minlat, maxlat, minlong, maxlong = disCalc.get_raduis_boundries (point,distance_threshold)
    for s in sp:
        sp_lat, sp_long = sp.get(s)
        if minlat <= sp_lat <= maxlat and minlong <= sp_long <= maxlong:
            dist = disCalc.haversine(point, sp.get(s))
            if dist <= distance_threshold and dist <= min_dist:
                min_dist = dist
                min_sp = s
    if min_sp != 0:
        return min_sp, min_dist
    else:
        return -1, -1



def detect_jump (trajectory):
    disCalc = DistanceCalculator()
    for i in range(1,len(trajectory) - 1):
        time_intr = trajectory[i].date - trajectory[i-1].date
        if time_intr.seconds + time_intr.days * 24 * 360 <= max_time_oulier_filter:
            dist = disCalc.haversine((trajectory[i].lat, trajectory[i].long), (trajectory[i-1].lat, trajectory[i-1].long))
            if time_intr.seconds + time_intr.days * 24 * 360  > 0:
                if dist / float(time_intr.seconds + time_intr.days * 24 * 360) > max_meter_per_second_outlier_filter:
                    return i
            else:
                if dist > 100:
                    return i

    return -1


def sudden_jumps_removal (trajectory):
    jumps_flag = True
    while jumps_flag:
        first_jump = detect_jump(trajectory)
        if first_jump > -1:
            #find all the point with the same server and quality as the first jump point
            server = trajectory[first_jump].server
            quality = trajectory[first_jump].quality
            cont = True
            index = first_jump
            while cont and index < len(trajectory)-1:
                if trajectory[index].server != server or trajectory[index].quality != quality:
                    cont = False
                else:
                    index += 1
            if first_jump  >= index - first_jump :
                for i in range (first_jump,index):
                    trajectory.pop(first_jump)
            else:
                for i in range (0,first_jump+1):
                    trajectory.pop(0)
        else:
            jumps_flag = False
    return format_row(trajectory)



def format_row (trajectory):
    out = []
    for point in trajectory:
        out.append(Row(date=point.date, lat=point.lat, long=point.long))
    return out



def remove_ouliters(locations):
    out = []
    out.append(locations[0])
    disCalc = DistanceCalculator()
    for i in range(1, len(locations)):
        time_intr = locations[i].date - out[len(out) - 1].date
        if time_intr.seconds + time_intr.days * 24 * 360 <= max_time_oulier_filter:
            dist = disCalc.haversine ((locations[i].lat, locations[i].long), (out[len(out) - 1].lat, out[len(out) - 1].long))
            if time_intr.seconds + time_intr.days * 24 * 360 > 0:
                if dist/float(time_intr.seconds + time_intr.days * 24 * 360) <= max_meter_per_second_outlier_filter:
                    out.append(locations[i])
            #else:
                #if there are 2 records at the same time at different coordinates
                #if locations[i].lat != out[len(out) - 1].lat or locations[i].long != out[len(out) - 1].long:
                    #out.pop()
        else:
            out.append(locations[i])
    return out






def smooth_data (locations):
    out = []
    out.append(locations[0])

    if len(locations) > 3:
        time_inter = locations[2].date - locations[0].date
        if time_inter.seconds + time_inter.days * 24 * 360 <= 3 * 60:
            lat = (locations[0].lat + locations[1].lat + locations[2].lat)/3
            long = (locations[0].long + locations[1].long + locations[2].long) / 3

            out.append(Row(date=locations[1].date, lat=lat, long=long))
        else:
            out.append(locations[1])

        for i in range (2,len (locations) - 2):
            time_inter =  locations[i+2].date - locations[i - 2].date
            if time_inter.seconds + time_inter.days * 24 * 360 <= 5*60:
                lat = (locations[i-2].lat + locations[i-1].lat + locations[i].lat + locations[i+1].lat + locations[i+2].lat)/5
                long = (locations[i - 2].long + locations[i - 1].long + locations[i].long + locations[i + 1].long + locations[
                    i + 2].long) / 5
                out.append(Row(date=locations[i].date, lat=lat, long=long))
            else:
                out.append(locations[i])

        time_inter = locations[len(locations) - 1].date - locations[len(locations) - 3].date
        if time_inter.seconds + time_inter.days * 24 * 360 <= 3 * 60:
            index = len(locations) - 1
            lat = (locations[index - 2].lat + locations[index - 1].lat + locations[index].lat) / 3
            long = (locations[index - 2].long + locations[index - 1].long + locations[index].long) / 3
            out.append(Row(date=locations[index - 1].date, lat=lat, long=long))
        else:
            out.append(locations[len(locations) - 2])

        out.append(locations[len(locations) - 1])
        return out
    else:
        return locations







def remove_errors(list, sp):
    # print(list)
    if len(list) > 0:
        station_id = -1
        prev_time = list[0][0]
        out = []
        for pair in list:
            if station_id != -1:
                delta_time = pair[0] - prev_time
                if delta_time.seconds + delta_time.days * 24 * 360 <= stop_thrshold:
                    if (haversine(sp.get(station_id), sp.get(pair[1])) > distance_threshold):
                        out.append(pair)
                else:
                    out.append(pair)
            else:
                out.append(pair)
            station_id = pair[1]
            prev_time = pair[0]
        return out


def process_chain_for_print(locations):
    out = []
    for row in locations:
        out.append((str(row.date),str(row.lat), str(row.long)))
    return out

def process_sp_chain_for_print(locations):
    out = []
    for row in locations:
        out.append((str(row[0]),str(row[1])))
    return out


def parseToRow(line):
    x = line.split('$')
    out = []
    for r in x:
        try:
            l = r.split("||")
            date = datetime.strptime(l[0], "%Y-%m-%d %H:%M:%S")
            lat = Decimal(l[1])
            long = Decimal(l[2])
            quality = int(l[3])
            server = l[4]
            out.append(Row(date=date, lat=lat, long=long, quality = quality, server = server))
        except ValueError:
            print("format date error")
    return out


def filter_short_chain(locations):
    time_inter = locations[len(locations) - 1].date - locations[0].date
    if time_inter.seconds + time_inter.days * 24 * 3600 >= short_path_t:
        return True
    else:
        return False


from pyspark import SparkConf, SparkContext
from pyspark import BasicProfiler
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from datetime import datetime
from datetime import timedelta
import hashlib
from DistanceCalculator import DistanceCalculator
from decimal import *
import time
from math import radians, cos, sin, asin, sqrt, floor




# conf = SparkConf().set("spark.python.profile", "true")


class MyCustomProfiler(BasicProfiler):
    def show(self, id):
        print("My custom profiles for RDD:%s" % id)


conf = SparkConf().setMaster("yarn-client").setAppName("Ayelet").\
    set("spark.shuffle.service.enabled", True).set("spark.dynamicAllocation.enabled", True).set(
    "spark.dynamicAllocation.minExecutors", 1). \
    set("spark.shuffle.blockTransferService", "nio").set("spark.task.maxFailures", 10).set("spark.executor.memory",
                                                                                           "4g").set(
    "spark.rdd.compress", True). \
    set("spark.storage.memoryFraction", 1).set("spark.core.connection.ack.wait.timeout", 600).set(
    "spark.akka.frameSize", 50) \
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
    .set("spark.driver.memory","3g")
    # .set("spark.default.parallelism", 6000)
# .set("spark.kryoserializer.buffer.max.mb",1000)
# .set("spark.yarn.executor.memoryOverhead",1000)

# .set("spark.executor.instances",100)

sc = SparkContext(conf=conf)
sc.addPyFile('/home/ayelet/Trajectories_processing/DistanceCalculator.py')
#conf.registerKryoClasses(Array(classOf[DistanceCalculator]))
# quiet_logs( sc )
sqlContext = SQLContext(sc)

for index in range(10):
	locations = sc.textFile("/user/hive/warehouse/tau.db/lat_lon_original_comp_propdelay/*").map(lambda x: x.split(' ')).filter(lambda r: len(r) == 7).\
	filter(lambda r : r[2].startswith('2013-01')).filter(lambda r : int(r[4][0]) == index)

	locations_byUser = locations.map(lambda r: (r[4], r[2] + " " + r[3] + "||" + r[0] + "||" + r[1] + "||" + r[5] + "||" + r[6])).reduceByKey(
    	lambda a, b: a + "$" + b,10000).mapValues(parseToRow). \
    	filter(lambda r: len(r[1]) > 1).mapValues(sorted)

	locations_byUser =  locations_byUser.mapValues(remove_ouliters)
	locations_byUser= locations_byUser.flatMapValues(split_chains_time_based)
	locations_byUser=locations_byUser.filter(lambda r: filter_short_chain(r[1])).\
    	mapValues(merge_close_points)
	locations_byUser = locations_byUser.filter(lambda r: len(r[1]) > 1).\
	flatMapValues(stay_points_detection).\
    	filter(lambda r : len(r[1]) > 1)
	locations_byUser.mapValues(process_chain_for_print).saveAsTextFile("output_chains_" + str(index)  )


	# gas_stations = sc.textFile("gas_stations_reduced_2km.txt").map(lambda x : x.split("\t")).filter(lambda x: x[1] != '' or x[2] != '').\
    	# map(lambda x : Row(id = x[0], lat = Decimal(x[1]), long = Decimal(x[2])))
	# c_gas_stations = gas_stations.map(lambda x : (x.id, (x.lat,x.long))).collectAsMap()
	# rdd_matches = locations_byUser.map(lambda r : (r[0],match_gas_station(r[1],c_gas_stations))).filter(lambda r: r[1] is not None)
	# rdd_matches.mapValues(merge_gas_stations).filter(lambda r : len(r[1]) > 1).mapValues(process_sp_chain_for_print).\
    	# saveAsTextFile("output_sp_" + str(index)  )
	# print("done " + str(index))
