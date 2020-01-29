# mobility_chains
Code to analyze mobility traces to create movement chains
Input Data: Hive tables lat_lon_original_comp_propdelay/

Chains_generation.py - Pyspark script that generates user transactions from the raw data. The script does the following:
Cleans the raw data (only 2013 data + removes outliers)
Generate trajectories
* The run failed when I ran it on all data at once. To overcome this, I ran it each time separately for imsi users that starts with a different digit (for example: all imsi users that start with 0, 1, 2 and so on). This is the loop on index in the script. 
Notice that this script uses DistanceCalculator.py (that calculates the haversine distance between points)
Output: user trajectories. The trajectories are saved in chunks (many small files) and in different folders for each index (starting imsi index). 



Parse_chains.py - script the combines all the user trajectories into one file. 
Output: one file with all the trajectories (19 GB). The name of the file is output_chains.txt (it’s not a good name but I don’t want to change it now so things won’t break). * I deleted it from the machine, but you can use the link above to download it. 
The structure of the file is the following:
Imsi user id
Latitude1, longitude1, date1
Latitude2, longitude2, date2
….
Imsi user id
Latitude1, longitude1, date1
Latitude2, longitude2, date2
….
Flatten_chains.py - a script that generate a flatten format from output_chains.txt. Now each row represents only the first and the last point of a user trajectory.
Output: stop_points.txt



Build_chains_osrm.ipynb - a jupyter notebook to generate all driving times using osrm. To run this make sure you download and run OSRM and download Israel maps. The script uses the file stations reduced 1.5km within data range.txt (list of all gas stations)
Outputs: 
Driving durations between all gas stations: gas_stations_durations.txt
Driving durations between all the first points in all the trajectories to all gas stations: orig_gas_stations_durations.txt
Driving durations between all the last points in all the trajectories to all gas stations: gas_stations_dest_durations.txt
Driving durations between all first and last points of the trajectories: origin_dest_durations.txt
main_create_trajs.py - code that maps trajectories (start and end points) to chains of stop points. This script is part of the project spatial-crowdsourcing.
Output: chains_paths_5min_5perc.txt. The file includes imsi, details about the original trajectory (start_long_2_dec,start_lat_2_dec,end_long_2_dec,end_lat_2_dec, orig_time - the original time of the trajectory) and the chain of mapped stop points (stations) and the time it takes to drive from the origin point to destination point when going through all the stop points according to OSRM (total_time)

Paths_transitions.txt - the most important file. Each row contains details about a segment in a chain (two consecutive stop points in a chain): imsi, origin stop point, destination stop point, origin time, destination time, driving duration.



main.py (part of the project  spatial-crowdsourcing) - script to create the routing network and calculate shortest paths.
Outputs: 8 files (one for each participation rate) with details about shortest paths between pairs of stop points at rounded hours. The structures of the files:
Orig_station - origin gas station id
Dest_station - destination gas station id
Time - delivery time
Duration - shortest path duration (with intermediate stop points - multi-hop)
Hops - number of hops (in multi-hop)
Min_hops - minimum number of hops required for delivery (not necessarily the shortest delivery)
Duration_from_time - delivery duration (one-hop)
Wait_in_stations - total wait time in stop points


Data analysis: coverage duration and probability.ipynb. Uses gas_stations_durations.txt 

Network_capacity.py, network_capacity_no_hops.py - scripts (part of the project  spatial-crowdsourcing) to calculate the maximum flow networks and their success rate (for multi-hop and one-hop architectures respectively). Uses the file gas station demands.txt. 
Data analysis: success rates by councils heatmaps.ipynb. Uses success_rate_by_councils_1_packages_1_perc-demand_2.csv and no_hops_success_rate_by_councils_1_packages_1_perc-demand_2.csv
All excel graphs


 


