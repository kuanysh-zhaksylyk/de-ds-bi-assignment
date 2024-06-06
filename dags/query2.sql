INSERT INTO year_avg_table_etl (year, station_name_district, item_name, avg_val)
SELECT
    TO_CHAR(DATE_TRUNC('year', mi.measurement_date), 'YYYY') AS year,
    msi.station_name_district,
    mii.item_name,
    AVG(mi.average_value) AS avg_val
FROM
    measurement_info mi
    LEFT JOIN measurement_station_info msi ON mi.station_code = msi.station_code
    LEFT JOIN measurement_item_info mii ON mi.item_code = mii.item_code
GROUP BY
    TO_CHAR(DATE_TRUNC('year', mi.measurement_date), 'YYYY'),
    msi.station_name_district,
    mii.item_name;
