-- Create table to store information about measurements, including date, station, item, average value, and instrument status.
CREATE TABLE measurement_info (
    measurement_date TIMESTAMP,
    station_code INT,
    item_code INT,
    average_value NUMERIC,
    instrument_status INT
);

-- Create table to store information about measurement items, including code, name, unit of measurement, and color thresholds.
CREATE TABLE measurement_item_info (
    item_code INT,
    item_name TEXT,
    unit_of_measurement TEXT,
    good_blue NUMERIC,
    normal_green NUMERIC,
    bad_yellow NUMERIC,
    very_bad_red NUMERIC
);

-- Create table to store information about measurement stations, including code, district name, address, latitude, and longitude.
CREATE TABLE measurement_station_info (
    station_code INT,
    station_name_district VARCHAR(50),
    address TEXT,
    latitude NUMERIC,
    longitude NUMERIC
);

-- Create table to summarize measurement data including date, station, location, and various pollutant levels.
CREATE TABLE measurement_summary (
    measurement_date TIMESTAMP,
    station_code INT,
    address TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    SO2 NUMERIC,
    NO2 NUMERIC,
    O3 NUMERIC,
    CO NUMERIC,
    PM10 NUMERIC,
    PM2_5 NUMERIC
);

-- Create table to store maximum NO2 measurements extracted from the summary table.
CREATE TABLE max_no2_measurement_etl (
    station_name_district VARCHAR(255),
    station_code VARCHAR(255),
    address VARCHAR(255),
    latitude NUMERIC,
    longitude NUMERIC,
    no2 NUMERIC,
    measurement_date DATE
);

-- Create table to store yearly averages of measurement data.
CREATE TABLE year_avg_table_etl (
    year VARCHAR(4),
    station_name_district VARCHAR(100),
    item_name VARCHAR(100),
    avg_val NUMERIC
);

-- Create table to assign color levels to measurement data based on thresholds.
CREATE TABLE color_level_table_etl (
    dt DATE,
    station_name_district VARCHAR(100),
    item_name VARCHAR(100),
    avg_val NUMERIC,
    lvl VARCHAR(10)
);

-- Create table to store maximum NO2 measurements.
CREATE TABLE max_no2_measurement AS
SELECT 
    msi.station_name_district,
    ms.station_code,
    ms.address,
    ms.latitude,
    ms.longitude,
    ms.no2,
    ms.measurement_date
FROM 
    measurement_summary ms
JOIN 
    measurement_station_info msi ON ms.station_code = msi.station_code
WHERE 
    ms.NO2 = (SELECT MAX(NO2) FROM measurement_summary);

-- Create table to store yearly averages of measurement data.
CREATE TABLE year_avg_table AS
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

-- Create table to assign color levels to measurement data based on thresholds.
CREATE TABLE color_level_table AS
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
