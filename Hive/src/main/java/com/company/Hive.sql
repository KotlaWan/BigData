CREATE TABLE IF NOT EXISTS Logs( ip string, ip1 string, ip2 string, ip3 string,
ip4 string, ip5 string, ip6 string, ip7 string, ip8 string, byte int)
COMMENT 'Hive details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH  'user/hive/hive_data' OVERWRITE  INTO TABLE Logs;
SELECT * FROM Logs;
CREATE TABLE IF NOT EXISTS Statistic (ip string, sum int, average int);
INSERT INTO Statistic (ip, sum, average)
SELECT ip, SUM(byte) as sum, AVG(byte) as average
FROM Logs
GROUP BY ip;
SELECT * FROM Statistic;