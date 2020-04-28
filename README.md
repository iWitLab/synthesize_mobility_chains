# mobility_chains
The repository contains code to analyze mobility traces to create movement chains

## Chains_generation
Chains_generation.py - Pyspark script that generates user transactions from the raw data. The script accepts as input a hive table lat_lon_original_comp_propdelay, and produces the following:

1. Cleans the raw data

2. Generate trajectories

Note that the run might fail when running on all data at once. To overcome this, ran it each time separately for imsi users that starts with a different digit (for example: all imsi users that start with 0, 1, 2 and so on). This is the loop on index in the script. 
Notice that this script uses DistanceCalculator.py (that calculates the haversine distance between points)
Output: user trajectories. The trajectories are saved in chunks (many small files) and in different folders for each index (starting imsi index). 

## Parse_chains

Parse_chains.py - the script combines all the user trajectories into one file. 

Output: one file with all the trajectories. The name of the file is output_chains.txt

The structure of the file is the following:
Imsi user id
Latitude1, longitude1, date1
Latitude2, longitude2, date2

Imsi user id
Latitude1, longitude1, date1
Latitude2, longitude2, date2

## Flatten_chains

Flatten_chains.py - a script that generate a flatten format from output_chains.txt. Now each row represents only the first and the last point of a user trajectory.
Output: stop_points.txt


Build_chains_osrm.ipynb - a jupyter notebook to generate all driving times using osrm. To run this make sure you download and run OSRM and download Israel maps. The script uses the file stations reduced 1.5km within data range.txt (list of all gas stations)
Outputs: 
Driving durations between all package-moving stations: gas_stations_durations.txt
Driving durations between all the first points in all the trajectories to all gas stations: orig_gas_stations_durations.txt
Driving durations between all the last points in all the trajectories to all gas stations: gas_stations_dest_durations.txt
Driving durations between all first and last points of the trajectories: origin_dest_durations.txt
main_create_trajs.py - code that maps trajectories (start and end points) to chains of stop points. This script is part of the project spatial-crowdsourcing.

Output: chains_paths_5min_5perc.txt. The file includes imsi, details about the original trajectory (start_long_2_dec,start_lat_2_dec,end_long_2_dec,end_lat_2_dec, orig_time - the original time of the trajectory) and the chain of mapped stop points (stations) and the time it takes to drive from the origin point to destination point when going through all the stop points according to OSRM (total_time)

Paths_transitions.txt - the most important file. Each row contains details about a segment in a chain (two consecutive stop points in a chain): imsi, origin stop point, destination stop point, origin time, destination time, driving duration.


## main

main.py (part of the project  spatial-crowdsourcing) - the script creates the routing network and calculate shortest paths.
Outputs: 8 files (one for each participation rate) with details about shortest paths between pairs of stop points at rounded hours. The structures of the files:
Orig_station - origin gas station id
Dest_station - destination gas station id
Time - delivery time
Duration - shortest path duration (with intermediate stop points - multi-hop)
Hops - number of hops (in multi-hop)
Min_hops - minimum number of hops required for delivery (not necessarily the shortest delivery)
Duration_from_time - delivery duration (one-hop)
Wait_in_stations - total wait time in stop points

## Other scripts

Data analysis: coverage duration and probability.ipynb. Uses gas_stations_durations.txt 

Network_capacity.py, network_capacity_no_hops.py - scripts to calculate the maximum flow networks and their success rate (for multi-hop and one-hop architectures respectively). Uses the file stations demands.txt. 
Data analysis: success rates by councils heatmaps.ipynb. Uses success_rate_by_councils_1_packages_1_perc-demand_2.csv and no_hops_success_rate_by_councils_1_packages_1_perc-demand_2.csv



 


