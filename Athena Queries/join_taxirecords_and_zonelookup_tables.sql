CREATE OR REPLACE VIEW nyc_taxi_trip_matrix AS
SELECT
    replace(pl.Zone, '"', '') AS pickup_zone,
    replace(dl.Zone, '"', '') AS dropoff_zone,
    COUNT(*) AS trip_count
FROM
    "processed_nyc_taxi_2025-07-24" t
LEFT JOIN nyc_taxi_zone_lookup pl ON t.pulocationid = pl.LocationID
LEFT JOIN nyc_taxi_zone_lookup dl ON t.dolocationid = dl.LocationID
GROUP BY
    pl.Zone, dl.Zone;
