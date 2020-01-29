#!/usr/bin/python
import csv
import os

# read input file


chain_id = 0;
try:
    source = '/home/ayelet/Trajectories_processing/output_chains/'
    output_file = open('/home/ayelet/Trajectories_processing/output_chains.txt', 'w')
    output_file.write("lat, long, date\n")
    for subdir, dirs, files in os.walk(source):
    	for f in files:
            print(f)
            if f != '_SUCCESS' :
                #fullpath = os.path.join(source, f)
                fullpath = subdir + os.sep + f
                f = open(fullpath, 'rt')
                reader = csv.reader(f, delimiter='(')
                count = 0
                for str in reader:
                    count += 1
                    imsi = str[1].replace("', [", "").replace("u'", "")
                    output_file.write(imsi + "\n")
                    for s in str:
                        if s.startswith("'"):
                            arr = s.split(",")
                            if len(arr) > 4:
                                output_file.write(
                                        arr[0].replace("'", "").strip() + "\t" + arr[1].replace("'",
                                                                                                        "").strip() + "\t" +
                                                arr[2].replace("'", "").strip() + "\t" + arr[3].replace("'",
                                                                                                        "").strip() + "\t" +
                                                arr[4].replace("'", "").replace(")", "").replace("]",
                                                                                                 "").strip() + "\n")
                            else:
                                output_file.write(
                                                arr[0].replace("'", "").strip() + "\t" + arr[1].replace("'",
                                                                                                        "").strip() + "\t" +
                                                arr[2].replace("'", "").replace(")", "").replace("]",
                                                                                                 "").strip() + "\n")
                f.close()

finally:
    output_file.close()
