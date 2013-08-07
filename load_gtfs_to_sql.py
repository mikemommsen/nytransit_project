import os
import sys
import csv
from pyspatialite import dbapi2 as sqlite3


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
    print infile
    tablename = infile.split('.')[0]
    schema = schemadict[tablename]
    f = open(infile)
    reader = csv.DictReader(f)
    conn = sqlite3.connect(outdb)
    cur = conn.cursor()
    headers = [x for x in reader.fieldnames if x in schema.keys()]
    questionmarks = '?,' * len(headers)
    string = "insert or replace into {0} {2} values({1})".format(tablename, questionmarks[:-1], tuple(headers))
    for r in reader:
        vals = [schema[k](r[k]) for k in reader.fieldnames if k in schema] #+ ['MakePoint({1},{0})'.format(r['shape_pt_lat'], r['shape_pt_lon'])]
        #print string, vals
        cur.execute(string, vals)
    conn.commit()
    conn.close()    

def main():
    outdb = sys.argv[1]
    indir = sys.argv[2]
    os.chdir(indir)
    files = os.listdir('.')
    for x in files:
        processCsv(x, schemadict, outdb)
    print 'done'

if __name__ == '__main__':
    main()
