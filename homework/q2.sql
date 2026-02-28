SELECT 
    count(*) FILTER (WHERE payment_type = 'Credit') / count(*)::DOUBLE * 100
FROM "taxi_trips"
--26.66