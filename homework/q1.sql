SELECT
  min(trip_pickup_date_time)a, 
  max(trip_dropoff_date_time) d
FROM "taxi_trips"
