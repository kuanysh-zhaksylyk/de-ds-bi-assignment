INSERT INTO color_level_table_etl (dt, station_name_district, item_name, avg_val, lvl)
SELECT 
    subquery.dt, 
    subquery.station_name_district, 
    subquery.item_name, 
    subquery.avg_val, 
    CASE 
        WHEN subquery.avg_val <= mii.good_blue THEN 'Blue'
        WHEN subquery.avg_val <= mii.normal_green AND subquery.avg_val > mii.good_blue THEN 'Green'
        WHEN subquery.avg_val <= mii.bad_yellow AND subquery.avg_val > mii.normal_green THEN 'Yellow'
        WHEN subquery.avg_val > mii.bad_yellow THEN 'Red'
        ELSE NULL 
    END AS lvl
FROM (
    SELECT 
        DATE(mi.measurement_date) AS dt, 
        msi.station_name_district, 
        mii.item_name, 
        AVG(mi.average_value) AS avg_val
    FROM 
        measurement_info mi
        LEFT JOIN measurement_station_info msi ON mi.station_code = msi.station_code
        LEFT JOIN measurement_item_info mii ON mi.item_code = mii.item_code
    GROUP BY 
        DATE(mi.measurement_date), 
        msi.station_name_district, 
        mii.item_name
) subquery
LEFT JOIN 
    measurement_item_info mii ON subquery.item_name = mii.item_name;