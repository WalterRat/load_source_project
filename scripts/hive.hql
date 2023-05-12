CREATE DATABASE IF NOT EXISTS ods;
USE ods;

CREATE TABLE IF NOT EXISTS daily_air_pollution_t (
	create_date date,
	country string,
	city string,
	value decimal(10,2))
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/daily_air_pollution_t';

CREATE TABLE IF NOT EXISTS weather_event_t(EventId string,Type string,Severity string,StartTime timestamp,EndTime timestamp,Precipitation decimal(10,2),TimeZone string,AirportCode string,LocationLat decimal(10,2),LocationLng decimal(10,2),City string,County string,State string,ZipCode string)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/weather_event_t';

CREATE TABLE IF NOT EXISTS historical_weather_t (carrier_code string, flight_number string, origin_airport string, destination_airport string, create_date date, scheduled_elapsed_time int, tail_number string, departure_delay int, arrival_delay int, delay_carrier int, delay_weather int, delay_national_aviation_system int, delay_security int, delay_late_aircarft_arrival int, cancelled_code string, year int, month int, day int, weekday int, scheduled_departure_dt timestamp, scheduled_arrival_dt timestamp, actual_departure_dt timestamp, actual_arrival_dt timestamp,station_x string, hourly_dry_build_temperature_x decimal(10,2), hourly_precipitation_x decimal(10,2), hourly_station_presure_x decimal(10,2), hourly_visibility_x decimal(10,2), hourly_wind_speed_x decimal(10,2), station_y decimal(10,2), hourly_dry_build_temperature_y decimal(10,2), hourly_precipitation_y decimal(10,2), hourly_station_presure_y decimal(10,2), hourly_visibility_y decimal(10,2), hourly_wind_speed_y decimal(10,2) )
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/historical_weather_t';

CREATE TABLE IF NOT EXISTS airport_rank_t (month int, day_of_week int, dep_del15 int, dep_time_blk string, distance_group int, segment_number int, concurrent_flights int, number_of_seats int, carrier_name string, airport_flights_month int, airline_flights_month int, airline_airport_flights_month int, avg_monthly_pass_airport int, avg_monthly_pass_airline int, flt_attendants_per_pass decimal(10,2), ground_serv_per_pass decimal(10,2), plane_age int, departing_airport string, latitude decimal(10,2), longitude decimal(10,2), previous_airport string, prcp decimal(10,2), snow decimal(10,2), snwd decimal(10,2), tmax decimal(10,2), awnd decimal(10,2))
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/airport_rank_t';

CREATE TABLE IF NOT EXISTS flights_t (flight_date date, airline string, origin string, dest string, cancelled boolean, diverted boolean, crs_dep_time int, dep_time decimal(10,2), dep_delay_minutes decimal(10,2), dep_delay decimal(10,2), arr_time decimal(10,2), arr_delay_minutes decimal(10,2), air_time decimal(10,2), crs_elapsed_time decimal(10,2), actual_elapsed_time decimal(10,2), distance decimal(10,2), year int, quarter int, month int, day_of_month int, day_of_week int, marketing_airline_network string, operated_or_branded_code_share_partners string, dot_id_marketing_airline int, iata_code_marketing_airline string, flight_number_marketing_airline string, operating_airline string, dot_id_operating_airline int, iata_code_operating_airline string, tail_number string, flight_number_operating_airline string, origin_airport_id int, origin_airport_seq_id int, origin_city_market_id int, origin_city_name string, origin_state string, origin_state_fips int, origin_state_name string, origin_wac int, dest_airport_id int, dest_airport_seq_id int, dest_city_market_id int, dest_city_name string, dest_state string, dest_state_fips int, dest_state_name string, dest_wac int, dep_del15 decimal(10,2), departure_delay_groups decimal(10,2), dep_time_blk string, taxi_out decimal(10,2), wheels_off decimal(10,2), wheels_on decimal(10,2), taxi_in decimal(10,2), crs_arr_time int, arr_delay decimal(10,2), arr_del15 decimal(10,2), arrival_delay_groups decimal(10,2), arr_time_blk string, distance_group int, div_airport_landings int)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/flights_t';

CREATE TABLE IF NOT EXISTS airport_t (airport_name string, airport_iata string)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/airport_t';

CREATE DATABASE IF NOT EXISTS archive;
USE archive;

CREATE TABLE IF NOT EXISTS daily_air_pollution_t( create_date date, country string, city string, value decimal(10,2) )
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/daily_air_pollution_t';

CREATE TABLE IF NOT EXISTS weather_event_t(EventId string, Type string, Severity string, StartTime timestamp, EndTime timestamp, Precipitation decimal(10,2), TimeZone string, AirportCode string, LocationLat decimal(10,2), LocationLng decimal(10,2), City string, County string, State string, ZipCode string)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/weather_event_t';

CREATE TABLE IF NOT EXISTS historical_weather_t (carrier_code string, flight_number string, origin_airport string, destination_airport string, create_date date, scheduled_elapsed_time int, tail_number string, departure_delay int, arrival_delay int, delay_carrier int, delay_weather int, delay_national_aviation_system int, delay_security int, delay_late_aircarft_arrival int, cancelled_code string, year int, month int, day int, weekday int, scheduled_departure_dt timestamp, scheduled_arrival_dt timestamp, actual_departure_dt timestamp, actual_arrival_dt timestamp,station_x string, hourly_dry_build_temperature_x decimal(10,2), hourly_precipitation_x decimal(10,2), hourly_station_presure_x decimal(10,2), hourly_visibility_x decimal(10,2), hourly_wind_speed_x decimal(10,2), station_y decimal(10,2), hourly_dry_build_temperature_y decimal(10,2), hourly_precipitation_y decimal(10,2), hourly_station_presure_y decimal(10,2), hourly_visibility_y decimal(10,2), hourly_wind_speed_y decimal(10,2) )
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/historical_weather_t';

CREATE TABLE IF NOT EXISTS airport_rank_t (month int, day_of_week int, dep_del15 int, dep_time_blk string, distance_group int, segment_number int, concurrent_flights int, number_of_seats int, carrier_name string, airport_flights_month int, airline_flights_month int, airline_airport_flights_month int, avg_monthly_pass_airport int, avg_monthly_pass_airline int, flt_attendants_per_pass decimal(10,2), ground_serv_per_pass decimal(10,2), plane_age int, departing_airport string, latitude decimal(10,2), longitude decimal(10,2), previous_airport string, prcp decimal(10,2), snow decimal(10,2), snwd decimal(10,2), tmax decimal(10,2), awnd decimal(10,2))
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/airport_rank_t';

CREATE TABLE IF NOT EXISTS flights_t (flight_date date, airline string, origin string, dest string, cancelled boolean, diverted boolean, crs_dep_time int, dep_time decimal(10,2), dep_delay_minutes decimal(10,2), dep_delay decimal(10,2), arr_time decimal(10,2), arr_delay_minutes decimal(10,2), air_time decimal(10,2), crs_elapsed_time decimal(10,2), actual_elapsed_time decimal(10,2), distance decimal(10,2), year int, quarter int, month int, day_of_month int, day_of_week int, marketing_airline_network string, operated_or_branded_code_share_partners string, dot_id_marketing_airline int, iata_code_marketing_airline string, flight_number_marketing_airline string, operating_airline string, dot_id_operating_airline int, iata_code_operating_airline string, tail_number string, flight_number_operating_airline string, origin_airport_id int, origin_airport_seq_id int, origin_city_market_id int, origin_city_name string, origin_state string, origin_state_fips int, origin_state_name string, origin_wac int, dest_airport_id int, dest_airport_seq_id int, dest_city_market_id int, dest_city_name string, dest_state string, dest_state_fips int, dest_state_name string, dest_wac int, dep_del15 decimal(10,2), departure_delay_groups decimal(10,2), dep_time_blk string, taxi_out decimal(10,2), wheels_off decimal(10,2), wheels_on decimal(10,2), taxi_in decimal(10,2), crs_arr_time int, arr_delay decimal(10,2), arr_del15 decimal(10,2), arrival_delay_groups decimal(10,2), arr_time_blk string, distance_group int, div_airport_landings int)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/flights_t';

CREATE TABLE IF NOT EXISTS airport_t (airport_name string, airport_iata string)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/archive.db/airport_t';