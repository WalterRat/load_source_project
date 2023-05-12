#!/bin/bash

hdfs dfs -copyFromLocal /tmp/datasets/air_quality_index.csv /user/hive/warehouse/ods.db/daily_air_pollution_t
echo 'Finished daily_air_pollution_t'
hdfs dfs -copyFromLocal /tmp/datasets/WeatherEvents_Jan2016-Dec2021.csv /user/hive/warehouse/ods.db/weather_event_t
echo 'Finished weather_event_t'
hdfs dfs -copyFromLocal /tmp/datasets/10-2019.csv /user/hive/warehouse/ods.db/historical_weather_t
echo 'Finished historical_weather_t'
hdfs dfs -copyFromLocal /tmp/datasets/full_data_flightdelay.csv /user/hive/warehouse/ods.db/airport_rank_t
echo 'Finished airport_rank_t'
hdfs dfs -copyFromLocal /tmp/datasets/Combined_Flights_2019.csv /user/hive/warehouse/ods.db/flights_t
echo 'Finished flights_t'
hdfs dfs -copyFromLocal /tmp/datasets/name_IATA_t.csv /user/hive/warehouse/ods.db/airport_t
echo 'Finished airport_t'