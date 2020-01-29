from decimal import *
import csv
import osrm
getcontext().prec = 4




try:
    source = '/Users/ayeletarditi/Downloads/output_chains.txt'
    f = open(source, 'rt')
    reader = csv.reader(f, delimiter='\t')
    start_latlong = 0
    end_lat_long = 0
    start_flag = 0
    flag = 0
    output_file = open('stop_points.txt', 'w')
    output_file.write('\t'.join(['imsi', 'start_date', 'start_lat', 'start_long', 'end_date', 'end_lat', 'end_long']))
    output_file.write('\n')
    for s in reader:
        if s[0].startswith('2013-'):
            if start_flag == 1:
                start_date = s[0]
                flag = 1
                start_latlong = (s[1] ,s[2])
                # start_latlong = (float(s[1]) , float(s[2]) )
                start_flag = 0
            end_lat_long = (s[1] ,s[2])
            end_date = s[0]
            # end_lat_long = (float(s[1]) , float(s[2]) )

        else:
            start_flag = 1
            imsi = s[0].strip()
            if flag == 1:
                output_file.write \
                    ('\t'.join([imsi ,start_date, start_latlong[0], start_latlong[1], end_date, end_lat_long[0], \
                                               end_lat_long[1]]))
                output_file.write('\n')
                flag = 0


finally:
    f.close()


