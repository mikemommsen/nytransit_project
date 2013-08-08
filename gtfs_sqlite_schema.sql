begin transaction;

create table agency (
  agency_id text primary key,
  agency_name text,
  agency_url text,
  agency_timezone text,
  agency_lang text
);

create table stops (
  stop_id text primary key,
  stop_code text,
  stop_name text,
  stop_desc text,
  stop_lat float,
  stop_lon float,
  zone_id text,
  stop_url text,
  location_type text,
  parent_station text,
  foreign key (parent_station) references stops(stop_id)
);

/*select addgeometrycolumn("stops", "geometry", 4326, "POINT", 2);
select createspatialindex("stops","geometry");*/

create table routes (
  route_id int primary key,
  agency_id text,
  route_short_name int,
  route_long_name text,
  route_desc text,
  route_type int,
  route_url text,
  foreign key (agency_id) references agency (agency_id)
);

create table transfers (
  from_stop_id text,
  to_stop_id text,
  transfer_type text,
  min_transfer_time int,
  foreign key (from_stop_id) references stops(stop_id),
  foreign key (to_stop_id) references stops(stop_id)
  );

create table trips (
  route_id text,
  service_id text,
  trip_id text,
  trip_headsign text,
  direction_id text
  block_id text,
  shape_id text,
  primary key (route_id, service_id, trip_id),
  foreign key (route_id) references routes(route_id),
  foreign key (shape_id) references shapes(shape_id),
  foreign key (trip_id) references trips(trip_id),
  foreign key (service_id) references calender(service_id)
);
/*select addgeometrycolumn("trips", "geometry", 4326, "LINESTRING", 2);
select createspatialindex("trips","geometry");*/
create table stop_times (
  trip_id text,
  arrival_time text,
  departure_time text,
  stop_id int,
  stop_sequence int,
  pickup_type text,
  drop_off_type text,
  stop_headsign text,
  shape_dist_traveled int,
  primary key (trip_id, stop_sequence),
  foreign key (trip_id) references trips(trip_id),
  foreign key (stop_id) references stops(stop_id)
);

create table calendar (
  service_id text primary key,
  monday int,
  tuesday int,
  wednesday int,
  thursday int,
  friday int,
  saturday int,
  sunday int,
  start_date date,
  end_date date
);

create table calendar_dates (
  service_id text,
  exception_date date,
  exception_type int,
  foreign key (service_id) references calender(service_id)
);

create table shapes (
  shape_id int,
  shape_pt_lat float,
  shape_pt_lon float,
  shape_pt_sequence int,
  primary key (shape_id, shape_pt_sequence)
);
/*select addgeometrycolumn("shapes", "geometry", 4326, "POINT", "XY");
select createspatialindex("shapes", "geometry");*/

commit;
