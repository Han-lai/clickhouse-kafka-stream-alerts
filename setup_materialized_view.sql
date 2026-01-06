CREATE TABLE alerts (
    alert_type String,
    deviceid String,
    previous_status Int32,
    current_status Int32,
    temp Float32,
    pressure Float32,
    errcode String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (deviceid, timestamp);

CREATE MATERIALIZED VIEW detect_alerts TO alerts AS
SELECT
    'STATUS_CHANGE' AS alert_type,
    deviceid,
    lagInFrame(device_status) OVER (PARTITION BY deviceid ORDER BY timestamp) AS previous_status,
    device_status AS current_status,
    temp,
    pressure,
    errcode,
    timestamp
FROM device_status
WHERE 
    previous_status IS NOT NULL AND previous_status != device_status
    OR temp < 10 OR temp > 90
    OR pressure < 2 OR pressure > 8
    OR (device_status = 5300 AND errcode != '');
