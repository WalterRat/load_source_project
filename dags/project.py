from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'retries': 1
}


dag = DAG('load_source_project', default_args=default_args, schedule_interval=None)

start_step = BashOperator(
    task_id='start_step',
    bash_command='echo "МЫ РУССКИЕ - С НАМИ БОГ"',
    dag=dag,
)

create_schema = PostgresOperator(
    task_id='create_schema',
    sql="CREATE SCHEMA IF NOT EXISTS dds_stg;"
        "CREATE SCHEMA IF NOT EXISTS dds;"
        "CREATE SCHEMA IF NOT EXISTS dds_view;"
        "CREATE SCHEMA IF NOT EXISTS dds_lgc;"
        "CREATE SCHEMA IF NOT EXISTS dm;"
        "CREATE SCHEMA IF NOT EXISTS idf;"
        "CREATE SCHEMA IF NOT EXISTS etl;",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_dds_stg_daily_air_pollution_t = PostgresOperator(
    task_id='load_dds_stg_daily_air_pollution_t',
    sql="""
    CREATE TABLE IF NOT EXISTS dds_stg.daily_air_pollution_t( create_date date NULL, country text NULL, city text NULL, value numeric NULL);
    CREATE TABLE IF NOT EXISTS dds.daily_air_pollution_t( create_date date NULL, country text NULL, city text NULL, value numeric NULL, "source" text NULL, date_load timestamp NULL);
    TRUNCATE TABLE dds_stg.daily_air_pollution_t;
    TRUNCATE TABLE dds.daily_air_pollution_t;
    copy dds_stg.daily_air_pollution_t (create_date, country, city, value) from '/tmp/datasets/air_quality_index.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_stg_weather_event_t = PostgresOperator(
    task_id='load_dds_stg_weather_event_t',
    sql="""
    CREATE TABLE IF NOT EXISTS dds_stg.weather_event_t(EventId text NULL, Type text NULL, Severity text NULL, StartTime timestamp NULL, EndTime timestamp NULL, Precipitation numeric NULL, TimeZone text NULL, AirportCode text NULL, LocationLat numeric NULL, LocationLng numeric NULL, City text NULL, County text NULL, State text NULL, ZipCode text NULL);
    CREATE TABLE IF NOT EXISTS dds.weather_event_t(EventId text NULL, Type text NULL, Severity text NULL, StartTime timestamp NULL, EndTime timestamp NULL, Precipitation numeric NULL, TimeZone text NULL, AirportCode text NULL, LocationLat numeric NULL, LocationLng numeric NULL, City text NULL, County text NULL, State text NULL, ZipCode text NULL, "source" text NULL, date_load timestamp NULL);
    TRUNCATE TABLE dds_stg.weather_event_t;
    TRUNCATE TABLE dds.weather_event_t;
    copy dds_stg.weather_event_t (EventId,Type,Severity,StartTime,EndTime,Precipitation,TimeZone,AirportCode,LocationLat,LocationLng,City,County,State,ZipCode) from '/tmp/datasets/WeatherEvents_Jan2016-Dec2021.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_stg_historical_weather_t = PostgresOperator(
    task_id='load_dds_stg_historical_weather_t',
    sql="""
    CREATE TABLE IF NOT EXISTS dds_stg.historical_weather_t (carrier_code text NULL, flight_number text NULL, origin_airport text NULL, destination_airport text NULL, date date NULL, scheduled_elapsed_time int8 NULL, tail_number text NULL, departure_delay int8 NULL, arrival_delay int8 NULL, delay_carrier int8 NULL, delay_weather int8 NULL, delay_national_aviation_system int8 NULL, delay_security int8 NULL, delay_late_aircarft_arrival int8 NULL, cancelled_code text NULL, year int8 NULL, month int8 NULL, day int8 NULL, weekday int8 NULL, scheduled_departure_dt timestamp NULL, scheduled_arrival_dt timestamp NULL, actual_departure_dt timestamp NULL, actual_arrival_dt timestamp NULL,station_x text NULL, hourly_dry_build_temperature_x numeric NULL, hourly_precipitation_x numeric NULL, hourly_station_presure_x numeric NULL, hourly_visibility_x numeric NULL, hourly_wind_speed_x numeric NULL, station_y numeric NULL, hourly_dry_build_temperature_y numeric NULL, hourly_precipitation_y numeric NULL, hourly_station_presure_y numeric NULL, hourly_visibility_y numeric NULL, hourly_wind_speed_y numeric NULL);
    CREATE TABLE IF NOT EXISTS dds.historical_weather_t (carrier_code text NULL, flight_number text NULL, origin_airport text NULL, destination_airport text NULL, date date NULL, scheduled_elapsed_time int8 NULL, tail_number text NULL, departure_delay int8 NULL, arrival_delay int8 NULL, delay_carrier int8 NULL, delay_weather int8 NULL, delay_national_aviation_system int8 NULL, delay_security int8 NULL, delay_late_aircarft_arrival int8 NULL, cancelled_code text NULL, year int8 NULL, month int8 NULL, day int8 NULL, weekday int8 NULL, scheduled_departure_dt timestamp NULL, scheduled_arrival_dt timestamp NULL, actual_departure_dt timestamp NULL, actual_arrival_dt timestamp NULL,station_x text NULL, hourly_dry_build_temperature_x numeric NULL, hourly_precipitation_x numeric NULL, hourly_station_presure_x numeric NULL, hourly_visibility_x numeric NULL, hourly_wind_speed_x numeric NULL, station_y numeric NULL, hourly_dry_build_temperature_y numeric NULL, hourly_precipitation_y numeric NULL, hourly_station_presure_y numeric NULL, hourly_visibility_y numeric NULL, hourly_wind_speed_y numeric NULL, "source" text NULL, date_load timestamp NULL);
    TRUNCATE TABLE dds_stg.historical_weather_t;
    TRUNCATE TABLE dds.historical_weather_t;
    COPY dds_stg.historical_weather_t (carrier_code, flight_number, origin_airport, destination_airport, date, scheduled_elapsed_time, tail_number, departure_delay, arrival_delay, delay_carrier, delay_weather, delay_national_aviation_system, delay_security, delay_late_aircarft_arrival, cancelled_code, year, month, day, weekday, scheduled_departure_dt, scheduled_arrival_dt, actual_departure_dt, actual_arrival_dt, station_x, hourly_dry_build_temperature_x, hourly_precipitation_x, hourly_station_presure_x, hourly_visibility_x, hourly_wind_speed_x, station_y, hourly_dry_build_temperature_y, hourly_precipitation_y, hourly_station_presure_y, hourly_visibility_y, hourly_wind_speed_y) FROM '/tmp/datasets/10-2019.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_stg_airport_rank_t = PostgresOperator(
    task_id='load_dds_stg_airport_rank_t',
    sql="""
    CREATE TABLE IF NOT EXISTS dds_stg.airport_rank_t (month int8 NULL, day_of_week int8 NULL, dep_del15 int8 NULL, dep_time_blk text NULL, distance_group int8 NULL, segment_number int8 NULL, concurrent_flights int8 NULL, number_of_seats int8 NULL, carrier_name text NULL, airport_flights_month int8 NULL, airline_flights_month int8 NULL, airline_airport_flights_month int8 NULL, avg_monthly_pass_airport int8 NULL, avg_monthly_pass_airline int8 NULL, flt_attendants_per_pass numeric NULL, ground_serv_per_pass numeric NULL, plane_age int8 NULL, departing_airport text NULL, latitude numeric NULL, longitude numeric NULL, previous_airport text NULL, prcp numeric NULL, snow numeric NULL, snwd numeric NULL, tmax numeric NULL, awnd numeric NULL);
    CREATE TABLE IF NOT EXISTS dds.airport_rank_t (month int8 NULL, day_of_week int8 NULL, dep_del15 int8 NULL, dep_time_blk text NULL, distance_group int8 NULL, segment_number int8 NULL, concurrent_flights int8 NULL, number_of_seats int8 NULL, carrier_name text NULL, airport_flights_month int8 NULL, airline_flights_month int8 NULL, airline_airport_flights_month int8 NULL, avg_monthly_pass_airport int8 NULL, avg_monthly_pass_airline int8 NULL, flt_attendants_per_pass numeric NULL, ground_serv_per_pass numeric NULL, plane_age int8 NULL, departing_airport text NULL, latitude numeric NULL, longitude numeric NULL, previous_airport text NULL, prcp numeric NULL, snow numeric NULL, snwd numeric NULL, tmax numeric NULL, awnd numeric NULL, "source" text NULL, date_load timestamp NULL);
    TRUNCATE TABLE dds_stg.airport_rank_t;
    TRUNCATE TABLE dds.airport_rank_t;
    COPY dds_stg.airport_rank_t (month, day_of_week, dep_del15, dep_time_blk, distance_group, segment_number, concurrent_flights, number_of_seats, carrier_name, airport_flights_month, airline_flights_month, airline_airport_flights_month, avg_monthly_pass_airport, avg_monthly_pass_airline, flt_attendants_per_pass, ground_serv_per_pass, plane_age, departing_airport, latitude, longitude, previous_airport, prcp, snow, snwd, tmax, awnd ) FROM '/tmp/datasets/full_data_flightdelay.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_stg_flghts_t = PostgresOperator(
    task_id='load_dds_stg_flghts_t',
    sql="""
    CREATE TABLE IF NOT EXISTS dds_stg.flights_t (flight_date date NULL, airline text NULL, origin text NULL, dest text NULL, cancelled bool NULL, diverted bool NULL, crs_dep_time int8 NULL, dep_time numeric NULL, dep_delay_minutes numeric NULL, dep_delay numeric NULL, arr_time numeric NULL, arr_delay_minutes numeric NULL, air_time numeric NULL, crs_elapsed_time numeric NULL, actual_elapsed_time numeric NULL, distance numeric NULL, year int8 NULL, quarter int8 NULL, month int8 NULL, day_of_month int8 NULL, day_of_week int8 NULL, marketing_airline_network text NULL, operated_or_branded_code_share_partners text NULL, dot_id_marketing_airline int8 NULL, iata_code_marketing_airline text NULL, flight_number_marketing_airline text NULL, operating_airline text NULL, dot_id_operating_airline int8 NULL, iata_code_operating_airline text NULL, tail_number text NULL, flight_number_operating_airline text NULL, origin_airport_id int8 NULL, origin_airport_seq_id int8 NULL, origin_city_market_id int8 NULL, origin_city_name text NULL, origin_state text NULL, origin_state_fips int8 NULL, origin_state_name text NULL, origin_wac int8 NULL, dest_airport_id int8 NULL, dest_airport_seq_id int8 NULL, dest_city_market_id int8 NULL, dest_city_name text NULL, dest_state text NULL, dest_state_fips int8 NULL, dest_state_name text NULL, dest_wac int8 NULL, dep_del15 numeric NULL, departure_delay_groups numeric NULL, dep_time_blk text NULL, taxi_out numeric NULL, wheels_off numeric NULL, wheels_on numeric NULL, taxi_in numeric NULL, crs_arr_time int8 NULL, arr_delay numeric NULL, arr_del15 numeric NULL, arrival_delay_groups numeric NULL, arr_time_blk text NULL, distance_group int8 NULL, div_airport_landings int8 NULL);
    CREATE TABLE IF NOT EXISTS dds.flights_t ( flight_date date NULL, airline text NULL, origin text NULL, dest text NULL, cancelled bool NULL, diverted bool NULL, crs_dep_time int8 NULL, dep_time numeric NULL, dep_delay_minutes numeric NULL, dep_delay numeric NULL, arr_time numeric NULL, arr_delay_minutes numeric NULL, air_time numeric NULL, crs_elapsed_time numeric NULL, actual_elapsed_time numeric NULL, distance numeric NULL, "year" int8 NULL, quarter int8 NULL, "month" int8 NULL, day_of_month int8 NULL, day_of_week int8 NULL, marketing_airline_network text NULL, operated_or_branded_code_share_partners text NULL, dot_id_marketing_airline int8 NULL, iata_code_marketing_airline text NULL, flight_number_marketing_airline text NULL, operating_airline text NULL, dot_id_operating_airline int8 NULL, iata_code_operating_airline text NULL, tail_number text NULL, flight_number_operating_airline text NULL, origin_airport_id int8 NULL, origin_airport_seq_id int8 NULL, origin_city_market_id int8 NULL, origin_city_full_name text NULL, origin_city_short_name text NULL, origin_state text NULL, origin_state_fips int8 NULL, origin_state_name text NULL, origin_wac int8 NULL, dest_airport_id int8 NULL, dest_airport_seq_id int8 NULL, dest_city_market_id int8 NULL, dest_city_name text NULL, dest_state text NULL, dest_state_fips int8 NULL, dest_state_name text NULL, dest_wac int8 NULL, dep_del15 numeric NULL, departure_delay_groups numeric NULL, dep_time_blk text NULL, taxi_out numeric NULL, wheels_off numeric NULL, wheels_on numeric NULL, taxi_in numeric NULL, crs_arr_time int8 NULL, arr_delay numeric NULL, arr_del15 numeric NULL, arrival_delay_groups numeric NULL, arr_time_blk text NULL, distance_group int8 NULL, div_airport_landings int8 NULL, "source" text NULL, date_load timestamp NULL);
    TRUNCATE TABLE dds_stg.flights_t;
    TRUNCATE TABLE dds.flights_t;
    COPY dds_stg.flights_t(flight_date, airline, origin, dest, cancelled, diverted, crs_dep_time, dep_time, dep_delay_minutes, dep_delay, arr_time, arr_delay_minutes, air_time, crs_elapsed_time, actual_elapsed_time, distance, year, quarter, month, day_of_month, day_of_week, marketing_airline_network, operated_or_branded_code_share_partners, dot_id_marketing_airline, iata_code_marketing_airline, flight_number_marketing_airline, operating_airline, dot_id_operating_airline, iata_code_operating_airline, tail_number, flight_number_operating_airline, origin_airport_id, origin_airport_seq_id, origin_city_market_id, origin_city_name, origin_state, origin_state_fips, origin_state_name, origin_wac, dest_airport_id, dest_airport_seq_id, dest_city_market_id, dest_city_name, dest_state, dest_state_fips, dest_state_name, dest_wac, dep_del15, departure_delay_groups, dep_time_blk, taxi_out, wheels_off, wheels_on, taxi_in, crs_arr_time, arr_delay, arr_del15, arrival_delay_groups, arr_time_blk, distance_group, div_airport_landings) FROM '/tmp/datasets/Combined_Flights_2019.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_stg_airport_t = PostgresOperator(
    task_id='load_dds_stg_airport_t',
    sql="""
    CREATE TABLE IF NOT EXISTS dds_stg.airport_t (airport_name text NULL, airport_iata text NULL);
    CREATE TABLE IF NOT EXISTS dds.airport_t (airport_name text NULL, airport_iata text NULL, "source" text NULL, date_load timestamp NULL);
    TRUNCATE TABLE dds_stg.airport_t;
    TRUNCATE TABLE dds.airport_t;
    copy dds_stg.airport_t (airport_name, airport_iata ) from '/tmp/datasets/name_IATA_t.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields = PostgresOperator(
    task_id='add_tech_fields',
    sql="""
    CREATE OR REPLACE FUNCTION etl.add_tech_fields(p_table text, p_source text) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
    v_date_load timestamp := NOW();
BEGIN
    EXECUTE format('ALTER TABLE dds_stg.%s ADD COLUMN source text DEFAULT %L', p_table, p_source);
    EXECUTE format('ALTER TABLE dds_stg.%s ADD COLUMN date_load timestamp DEFAULT %L', p_table, v_date_load);
END; $function$;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields_daily_air_pollution_t = PostgresOperator(
    task_id='add_tech_fields_daily_air_pollution_t',
    sql="""
    SELECT etl.add_tech_fields ('daily_air_pollution_t', 'daily_air_pollution');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields_weather_event_t = PostgresOperator(
    task_id='add_tech_fields_weather_event_t',
    sql="""
    SELECT etl.add_tech_fields ('weather_event_t', 'weather_event');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields_historical_weather_t = PostgresOperator(
    task_id='add_tech_fields_historical_weather_t',
    sql="""
    SELECT etl.add_tech_fields ('historical_weather_t', 'historical_weather');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields_airport_rank_t = PostgresOperator(
    task_id='add_tech_fields_airport_rank_t',
    sql="""
    SELECT etl.add_tech_fields ('airport_rank_t', 'airport_rank');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields_flights_t = PostgresOperator(
    task_id='add_tech_fields_flights_t',
    sql="""
    SELECT etl.add_tech_fields ('flights_t', 'flights');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

add_tech_fields_airport_t = PostgresOperator(
    task_id='add_tech_fields_airport_t',
    sql="""
    SELECT etl.add_tech_fields ('airport_t', 'airport');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_dds_lgc_daily_air_pollution_v = PostgresOperator(
    task_id='load_dds_lgc_daily_air_pollution_v',
    sql="""
    CREATE OR REPLACE VIEW dds_lgc.daily_air_pollution_v AS SELECT create_date, country, city, value, source, date_load FROM dds_stg.daily_air_pollution_t;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_lgc_weather_event_v= PostgresOperator(
    task_id='load_dds_lgc_weather_event_v',
    sql="""
    CREATE OR REPLACE VIEW dds_lgc.weather_event_v AS SELECT EventId, Type, Severity, StartTime, EndTime, Precipitation, TimeZone, AirportCode, LocationLat, LocationLng, City, County, State, ZipCode source, date_load FROM dds_stg.weather_event_t;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_lgc_historical_weather_v = PostgresOperator(
    task_id='load_dds_lgc_historical_weather_v',
    sql="""
    CREATE OR REPLACE VIEW dds_lgc.historical_weather_v AS SELECT historical_weather_t.carrier_code, historical_weather_t.flight_number, historical_weather_t.origin_airport, historical_weather_t.destination_airport, historical_weather_t.date, historical_weather_t.scheduled_elapsed_time, historical_weather_t.tail_number, historical_weather_t.departure_delay, historical_weather_t.arrival_delay, historical_weather_t.delay_carrier, historical_weather_t.delay_weather, historical_weather_t.delay_national_aviation_system, historical_weather_t.delay_security, historical_weather_t.delay_late_aircarft_arrival, historical_weather_t.cancelled_code, historical_weather_t.year, historical_weather_t.month, historical_weather_t.day, historical_weather_t.weekday, historical_weather_t.scheduled_departure_dt, historical_weather_t.scheduled_arrival_dt, historical_weather_t.actual_departure_dt, historical_weather_t.actual_arrival_dt, historical_weather_t.station_x, historical_weather_t.hourly_dry_build_temperature_x, historical_weather_t.hourly_precipitation_x, historical_weather_t.hourly_station_presure_x, historical_weather_t.hourly_visibility_x, historical_weather_t.hourly_wind_speed_x, historical_weather_t.station_y, historical_weather_t.hourly_dry_build_temperature_y, historical_weather_t.hourly_precipitation_y, historical_weather_t.hourly_station_presure_y, historical_weather_t.hourly_visibility_y, historical_weather_t.hourly_wind_speed_y, historical_weather_t.source, historical_weather_t.date_load FROM dds_stg.historical_weather_t;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_lgc_airport_rank_v = PostgresOperator(
    task_id='load_dds_lgc_airport_rank_v',
    sql="""
    CREATE OR REPLACE VIEW dds_lgc.airport_rank_v AS SELECT art.month, art.day_of_week, art.dep_del15, art.dep_time_blk, art.distance_group, art.segment_number, art.concurrent_flights, art.number_of_seats, art.carrier_name, art.airport_flights_month, art.airline_flights_month, art.airline_airport_flights_month, art.avg_monthly_pass_airport, art.avg_monthly_pass_airline, art.flt_attendants_per_pass, art.ground_serv_per_pass, art.plane_age, art.departing_airport, art.latitude, art.longitude, art.previous_airport, art.prcp, art.snow, art.snwd, art.tmax, art.awnd, art.source, art.date_load FROM dds_stg.airport_rank_t art;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_lgc_flights_v = PostgresOperator(
    task_id='load_dds_lgc_flights_v',
    sql="""
    CREATE OR REPLACE VIEW dds_lgc.flights_v AS SELECT flights_t.flight_date, flights_t.airline, flights_t.origin, flights_t.dest, flights_t.cancelled, flights_t.diverted, flights_t.crs_dep_time, flights_t.dep_time, flights_t.dep_delay_minutes, flights_t.dep_delay, flights_t.arr_time, flights_t.arr_delay_minutes, flights_t.air_time, flights_t.crs_elapsed_time, flights_t.actual_elapsed_time, flights_t.distance, flights_t.year, flights_t.quarter, flights_t.month, flights_t.day_of_month, flights_t.day_of_week, flights_t.marketing_airline_network, flights_t.operated_or_branded_code_share_partners, flights_t.dot_id_marketing_airline, flights_t.iata_code_marketing_airline, flights_t.flight_number_marketing_airline, flights_t.operating_airline, flights_t.dot_id_operating_airline, flights_t.iata_code_operating_airline, flights_t.tail_number, flights_t.flight_number_operating_airline, flights_t.origin_airport_id, flights_t.origin_airport_seq_id, flights_t.origin_city_market_id, SUBSTRING(flights_t.origin_city_name FROM 1 FOR POSITION((','::text) IN (flights_t.origin_city_name)) - 1) AS origin_city_full_name, SUBSTRING(flights_t.origin_city_name FROM POSITION((','::text) IN (flights_t.origin_city_name)) + 2) AS origin_city_short_name, flights_t.origin_state, flights_t.origin_state_fips, flights_t.origin_state_name, flights_t.origin_wac, flights_t.dest_airport_id, flights_t.dest_airport_seq_id, flights_t.dest_city_market_id, flights_t.dest_city_name, flights_t.dest_state, flights_t.dest_state_fips, flights_t.dest_state_name, flights_t.dest_wac, flights_t.dep_del15, flights_t.departure_delay_groups, flights_t.dep_time_blk, flights_t.taxi_out, flights_t.wheels_off, flights_t.wheels_on, flights_t.taxi_in, flights_t.crs_arr_time, flights_t.arr_delay, flights_t.arr_del15, flights_t.arrival_delay_groups, flights_t.arr_time_blk, flights_t.distance_group, flights_t.div_airport_landings, flights_t.source, flights_t.date_load FROM dds_stg.flights_t;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_lgc_airport_v = PostgresOperator(
    task_id='load_dds_lgc_airport_v',
    sql="""
    CREATE OR REPLACE VIEW dds_lgc.airport_v AS SELECT at2.airport_name, at2.airport_iata, source, date_load FROM dds_stg.airport_t at2;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_data_to_dds = PostgresOperator(
    task_id='load_data_to_dds',
    sql="""
    CREATE OR REPLACE FUNCTION etl.load_data_to_dds(view_name text, table_name text)
    RETURNS void LANGUAGE plpgsql AS $function$
    BEGIN
      EXECUTE 'INSERT INTO dds.' || table_name || ' SELECT * FROM dds_lgc.' || view_name;
    END;$function$;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_dds_daily_air_pollution_t = PostgresOperator(
    task_id='load_dds_daily_air_pollution_t',
    sql="""
    select etl.load_data_to_dds('daily_air_pollution_v','daily_air_pollution_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_weather_event_t = PostgresOperator(
    task_id='load_dds_weather_event_t',
    sql="""
    select etl.load_data_to_dds('weather_event_v','weather_event_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_historical_weather_t = PostgresOperator(
    task_id='load_dds_historical_weather_t',
    sql="""
    select etl.load_data_to_dds('historical_weather_v','historical_weather_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_airport_rank_t = PostgresOperator(
    task_id='load_dds_airport_rank_t',
    sql="""
    select etl.load_data_to_dds('airport_rank_v','airport_rank_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_flghts_t = PostgresOperator(
    task_id='load_dds_flghts_t',
    sql="""
    select etl.load_data_to_dds('flights_v','flights_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dds_airport_t = PostgresOperator(
    task_id='load_dds_airport_t',
    sql="""
    select etl.load_data_to_dds('airport_v','airport_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_data_to_dm = PostgresOperator(
    task_id='load_data_to_dm',
    sql="""
    CREATE OR REPLACE FUNCTION etl.load_data_to_dm(view_name text, table_name text)
    RETURNS void LANGUAGE plpgsql AS $function$
    BEGIN
      EXECUTE 'INSERT INTO dm.' || table_name || ' SELECT * FROM idf.' || view_name;
    END; $function$;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


count_flight_v = PostgresOperator(
    task_id='count_flight_v',
    sql="""
    CREATE OR REPLACE VIEW dds_view.count_flight_v AS SELECT count(*) AS count, ft.flight_date FROM dds.flights_t ft GROUP BY ft.flight_date;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


disp_analysis_v = PostgresOperator(
    task_id='disp_analysis_v',
    sql="""
    CREATE OR REPLACE VIEW idf.disp_analysis_v
AS SELECT f.flight_date,
    f.dep_delay_minutes,
    f.origin,
    w.Type,
    w.severity,
    c.count,
    h.hourly_wind_speed_x,
        CASE
            WHEN c.count > 22000 THEN 'large'::text
            WHEN c.count > 19000 AND c.count <= 22000 THEN 'medium'::text
            ELSE 'small'::text
        END AS airport_size
   FROM dds.flights_t f
     JOIN dds.weather_event_t w ON f.flight_date = w.StartTime AND f.origin_city_full_name = w.city
     JOIN dds.historical_weather_t h ON h.date = f.flight_date AND f.origin = h.origin_airport
     JOIN dds_view.count_flight_v c ON c.flight_date = f.flight_date;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


dm_flights_air_v = PostgresOperator(
    task_id='dm_flights_air_v',
    sql="""
    CREATE OR REPLACE VIEW idf.dm_flights_air_v
AS SELECT f.flight_date,
    d.create_date,
    count(f.*) AS count_fligths,
    avg(d.value) AS avg_value,
    sum(d.value) AS sum_values
   FROM dds.flights_t f
     JOIN dds.daily_air_pollution_t d ON f.flight_date = d.create_date AND f.origin_city_full_name = d.city
  GROUP BY f.flight_date, d.create_date;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


disp_analysis_t = PostgresOperator(
    task_id='disp_analysis_t',
    sql="""
    CREATE TABLE dm.disp_analysis_t ( flight_date date NULL, dep_delay_minutes numeric NULL, origin text NULL, event_type text NULL, severity text NULL, count int8 NULL, hourly_wind_speed_x numeric NULL, aiport_size text NULL);
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

flights_air_t = PostgresOperator(
    task_id='flights_air_t',
    sql="""
    CREATE TABLE dm.flights_air_t (flight_date date NULL, create_date date NULL, count_fligths int8 NULL, avg_value numeric NULL, sum_value int4 NULL);
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dm_flights_air_t = PostgresOperator(
    task_id='load_dm_flights_air_t',
    sql="""
    select etl.load_data_to_dm('dm_flights_air_v','flights_air_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_dm_disp_analysis_t = PostgresOperator(
    task_id='load_dm_disp_analysis_t',
    sql="""
    select etl.load_data_to_dm('disp_analysis_v','disp_analysis_t')
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


start_step >> create_schema >> load_dds_stg_daily_air_pollution_t >> add_tech_fields >> add_tech_fields_daily_air_pollution_t >> load_dds_lgc_daily_air_pollution_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> disp_analysis_t >> load_dm_disp_analysis_t
start_step >> create_schema >> load_dds_stg_daily_air_pollution_t >> add_tech_fields >> add_tech_fields_daily_air_pollution_t >> load_dds_lgc_daily_air_pollution_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> flights_air_t >> load_dm_flights_air_t

start_step >> create_schema >> load_dds_stg_weather_event_t >> add_tech_fields >> add_tech_fields_weather_event_t >> load_dds_lgc_weather_event_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> disp_analysis_t >> load_dm_disp_analysis_t
start_step >> create_schema >> load_dds_stg_weather_event_t >> add_tech_fields >> add_tech_fields_weather_event_t >> load_dds_lgc_weather_event_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> flights_air_t >> load_dm_flights_air_t

start_step >> create_schema >> load_dds_stg_historical_weather_t >> add_tech_fields >> add_tech_fields_historical_weather_t >> load_dds_lgc_historical_weather_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> disp_analysis_t >> load_dm_disp_analysis_t
start_step >> create_schema >> load_dds_stg_historical_weather_t >> add_tech_fields >> add_tech_fields_historical_weather_t >> load_dds_lgc_historical_weather_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> flights_air_t >> load_dm_flights_air_t

start_step >> create_schema >> load_dds_stg_airport_rank_t >> add_tech_fields >> add_tech_fields_airport_rank_t >> load_dds_lgc_airport_rank_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> disp_analysis_t >> load_dm_disp_analysis_t
start_step >> create_schema >> load_dds_stg_airport_rank_t >> add_tech_fields >> add_tech_fields_airport_rank_t >> load_dds_lgc_airport_rank_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> flights_air_t >> load_dm_flights_air_t

start_step >> create_schema >> load_dds_stg_flghts_t >> add_tech_fields >> add_tech_fields_flights_t >> load_dds_lgc_flights_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> disp_analysis_t >> load_dm_disp_analysis_t
start_step >> create_schema >> load_dds_stg_flghts_t >> add_tech_fields >> add_tech_fields_flights_t >> load_dds_lgc_flights_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> flights_air_t >> load_dm_flights_air_t

start_step >> create_schema >> load_dds_stg_airport_t >> add_tech_fields >> add_tech_fields_airport_t >> load_dds_lgc_airport_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> disp_analysis_t >> load_dm_disp_analysis_t
start_step >> create_schema >> load_dds_stg_airport_t >> add_tech_fields >> add_tech_fields_airport_t >> load_dds_lgc_airport_v >> load_data_to_dds >> [load_dds_daily_air_pollution_t, load_dds_weather_event_t, load_dds_historical_weather_t, load_dds_airport_rank_t, load_dds_flghts_t, load_dds_airport_t] >> count_flight_v >> [disp_analysis_v, dm_flights_air_v] >> load_data_to_dm >> flights_air_t >> load_dm_flights_air_t

