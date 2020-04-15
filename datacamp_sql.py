### SQL NOTES
# return 5 rows
'''SELECT TOP(5) artist
FROM artists;'''

# return top 5% of rows
'''SELECT TOP(5) PERCENT artist
FROM artists;'''

# rename columns
'''SELECT demand_loss_mw AS lost_demand
from grid;'''

## CAN USE 'where'
SELECT *
FROM album
WHERE artist_id IN (1, 3)

## WORKING WITH STRINGS
-- Complete the substring function to begin extracting from the correct character in the description column
SELECT TOP (10)
  description, 
  CHARINDEX('Weather', description) AS start_of_string, 
  LEN ('Weather') AS length_of_string, 
  SUBSTRING(
    description, 
    15, 
    LEN(description)
  ) AS additional_description 
FROM 
  grid
WHERE description LIKE '%Weather%';

# charindex returns position of substring within string
# len returns length of string
# substring subsets string

## GROUPS
# filter groupby objects with HAVING

## INNER JOIN
SELECT
	table_a.columnX,
	table_a.columny,
	table_b.columnz
FROM
	table_a
INNER JOIN table_b on table_b.foreign_key = table_a.primary_key;

## COMBINING SIMILAR TABLES
# same columns, same datasets
SELECT *
FROM album
UNION
SELECT album_id
FROM album
WHERE ...

# UNION returns DISTINCT rows
# UNION ALL returns ALL, including duplicates

## CRUD
CREATE TABLE table_name(
	col1name int,
	col2name varchar(20)
	)

##
INSERT INTO table_name
	(col1name, col2name)
VALUES
	(val1, val2)

##
UPDATE table_name
SET column_name = value,
WHERE ...
..

## Delete
DELETE 
FROM table_name
WHERE ...

## 
TRUNCATE TABLE table_name

## CAN USE DECLARE TO SAVE VARIABLES?
DECLARE @start DATE
DECLARE @stop DATE
DECLARE @affected INT;

SET @start = '2014-01-24'
SET @stop  = '2014-07-02'
SET @affected =  5000 ;

SELECT 
  description,
  nerc_region,
  demand_loss_mw,
  affected_customers
FROM 
  grid
WHERE event_date BETWEEN @start AND @stop
AND affected_customers >= @affected;

## TEMPORARY TABLES
# saving results of a query by creating a temporary
# table that remains in DB until SQL server is restarted
# NAMING is done with #

SELECT  album.title AS album_title,
  artist.name as artist,
  MAX(track.milliseconds / (1000 * 60) % 60 ) AS max_track_length_mins
INTO #maxtracks
FROM album
INNER JOIN artist ON album.artist_id = artist.artist_id
JOIN track ON track.album_id = album.album_id
GROUP BY artist.artist_id, album.title, artist.name,album.album_id
-- Run the final SELECT query to retrieve the results from the temporary table
SELECT album_title, artist, max_track_length_mins
FROM  #maxtracks
ORDER BY max_track_length_mins DESC, artist;