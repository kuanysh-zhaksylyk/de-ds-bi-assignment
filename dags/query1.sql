INSERT INTO max_no2_measurement_etl (station_name_district, station_code, address, latitude, longitude, no2, measurement_date)
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
