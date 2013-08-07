# mike mommsen mikemommsen@gmail.com
# script to load gtfs data into a spatialite database
# will not work exactly perfectly because schemas change city to city
# but wont take much tweaking
# to use do something like: python load_gtfs_to_sql.py myspatialite.db gtfsfolder
# the db should be made and have tables made with the transit schema sql file also availible

import os
import sys
import csv
from pyspatialite import dbapi2 as sqlite3

# this is the dictionary of fields and types
# this will cause the errors so edit as you see fit to make work
schemadict = {'agency': {
  'agency_id': str,
  'agency_name': str,
  'agency_url': str,
  'agency_timezone': str,
  'agency_lang': str
  },
  'stops': {
  'stop_id': str,
  'stop_code': str,
  'stop_name': str,
  'stop_desc': str,
  'stop_lat': str,
  'stop_lon': float,
  'zone_id': str,
  'stop_url': str,
  'location_type': str,
  'parent_station':str
  },
  'routes': {
  'route_id': str,
  'agency_id': str,
  'route_short_name': str,
  'route_long_name': str,
  'route_desc': str,
  'route_type': int,
  'route_url': str
  },
  'trips': {
  'route_id': str,
  'service_id': str,
  'trip_id': str,
  'trip_headsign': str,
  'block_id': str,
  'shape_id': str,
  'wheelchair_accessible': str
  },
  'stop_times': {
  'trip_id': str,
  'arrival_time': str,
  'departure_time': str,
  'stop_id': str,
  'stop_sequence': int,
  'pickup_type': str,
  'drop_off_type': str,
  'stop_headsign': str,
  'shape_dist_traveled': bool
  },
  'calendar': {
  'service_id': str,
  'monday': int,
  'tuesday': int,
  'wednesday': int,
  'thursday': int,
  'friday': int,
  'saturday': int,
  'sunday': int,
  'start_date': int,
  'end_date': int
},
  'calendar_dates': {
  'service_id': str,
  'exception_date': int,
  'exception_type': int
  },
  'shapes': {
  'shape_id': str,
  'shape_pt_lat': float,
  'shape_pt_lon': float,
  'shape_pt_sequence': int
  },
  'transfers': {
  'from_stop_id': str,
  'to_stop_id': str,
  'transfer_type': str,
  'min_transfer_time': int
}}

def processCsv(infile, schemadict, outdb):
    """function that takes a file, schemadict and outdb
    loops through row by row, matches with the schema
    and throws each row out to the outdb"""
    # let the user know that the script is running
    print infile
    # take the name of the file
    tablename = infile.split('.')[0]
    # grab pertinent part of schemadict
    schema = schemadict[tablename]
    f = open(infile)
    # start of the dictreader, which is a great little option for csvs
    reader = csv.DictReader(f)
    # open a connection and create cursor to access the database
    conn = sqlite3.connect(outdb)
    cur = conn.cursor()
    # find the intersection of csv fieldnames and the schema
    headers = [x for x in reader.fieldnames if x in schema.keys()]
    # i really have no experience with how to do this, i know it is wrong
    # but i am just making a string with the right amount of question marks
    questionmarks = '?,' * len(headers)
    # create a base string that has everything but the values
    string = "insert or replace into {0} {2} values({1})".format(tablename, questionmarks[:-1], tuple(headers))
    # loop through each row of the infile
    for r in reader:
        # process each element of the row through the schema
        # so strs stay as strings, and ints get converted to integers
        vals = [schema[k](r[k]) for k in reader.fieldnames if k in schema]
        # execute
        cur.execute(string, vals)
    # commit and close
    conn.commit()
    conn.close()    

def main():
    # take the args
    outdb = sys.argv[1]
    indir = sys.argv[2]
    # go the directoary
    os.chdir(indir)
    files = os.listdir('.')
    # loop through the files
    for x in files:
        # throw to the process csv function
        processCsv(x, schemadict, outdb)
    print 'done'

if __name__ == '__main__':
    main()
