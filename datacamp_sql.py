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

## INTRO TO RELATIONAL DATABASES ##################
#-- Query the right table in information_schema to get columns
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'university_professors' AND table_schema = 'public';

#Query the first five rows of our table
SELECT * 
FROM university_professors 
LIMIT 5;

#-- Add the university_shortname column
ALTER TABLE professors
ADD COLUMN university_shortname text;

#-- Rename the organisation column
ALTER TABLE affiliations
RENAME COLUMN organisation TO organization;

##-- Delete the university_shortname column
ALTER TABLE affiliations
DROP COLUMN university_shortname;

#-- Insert unique professors into the new table
INSERT INTO professors 
SELECT DISTINCT firstname, lastname, university_shortname 
FROM professors;

#-- -- Delete the university_professors table
DROP TABLE university_professors;

## Better data quality with constraints
# Attribute constraints, Key constraints, Referential integrity

#-- Calculate the net amount as amount + fee
SELECT transaction_date, CAST(amount AS integer) + CAST(fee AS integer) AS net_amount 
FROM transactions;

##-- Specify the correct fixed-length character type
ALTER TABLE professors
ALTER COLUMN university_shortname
TYPE varchar(3);

#-- Convert the values in firstname to a max. of 16 characters
ALTER TABLE professors 
ALTER COLUMN firstname 
TYPE varchar(16)
USING SUBSTRING(firstname FROM 1 FOR 16)

#-- Can change not null restrictions
ALTER TABLE professors 
ALTER COLUMN firstname SET NOT NULL;

# when creating tables, can add uniqueness constraint
CREATE TABLE table_name (
 column_name UNIQUE
);

# for existing tables
ALTER TABLE table_name
ADD CONSTRAINT some_name UNIQUE(column_name);

## WHAT IS a key? attribute(s) that identify a record uniquely
# superkey: # of distinct records = # of rows
#-- Rename the organization column to id
ALTER TABLE organizations
RENAME COLUMN organization TO id;

#-- Make id a primary key
ALTER TABLE organizations
ADD CONSTRAINT organization_pk PRIMARY KEY (id);

## AUTO INCREMENTING records
-- Add the new column to the table
ALTER TABLE professors 
ADD COLUMN id serial;

#-- Make id a primary key
ALTER TABLE professors 
ADD CONSTRAINT professors_pkey PRIMARY KEY (id);

##
#-- Count the number of distinct rows with columns make, model
SELECT COUNT(DISTINCT(make, model)) 
FROM cars;

#-- Add the id column
ALTER TABLE cars
ADD COLUMN id varchar(128);

#-- Update id with make + model
UPDATE cars
SET id = CONCAT(make, model);

#=-- Create the table
CREATE TABLE students (
  last_name varchar(128) NOT NULL,
  ssn integer PRIMARY KEY,
  phone_no char(12)
);

## RELATIONSHIPS WITH FOREIGN KEYS
ALTER TABLE a 
ADD CONSTRAINT a_fkey FOREIGN KEY (b_id) REFERENCES b (id);

#-- Rename the university_shortname column
ALTER TABLE professors
RENAME COLUMN university_shortname TO university_id;

#-- Add a foreign key on professors referencing universities
ALTER TABLE professors 
ADD CONSTRAINT professors_fkey FOREIGN KEY (university_id) REFERENCES universities (id);

#-- Create a new foreign key from scratch
ALTER TABLE affiliations
ADD COLUMN professor_id integer REFERENCES professors (id);

#-- Rename the organization column to organization_id
ALTER TABLE affiliations
RENAME organization TO organization_id;

#Here's a way to update columns of a table based on values in another table:
UPDATE table_a
SET column_to_update = table_b.column_to_update_from
FROM table_b
WHERE condition1 AND condition2 AND ...;

#-- Set professor_id to professors.id where firstname, lastname correspond to rows in professors
UPDATE affiliations
SET professor_id = professors.id
FROM professors
WHERE affiliations.firstname = professors.firstname AND affiliations.lastname = professors.lastname;

# Referential integrity: a record referencing another table must refer 
# to an existing record in that table
## = the point of foreign key
CREATE TABLE table_name(
	id integer PRIMARY KEY,
	column_name varchar(64),
	b_id integer REFERENCES b (id) ON DELETE NO ACTION
	); 

#options: NO ACTION -> prevents deletion of records
# ON DELETE CASCADE -> deletes records in both tables

#Note: Altering a key constraint doesn't work with ALTER COLUMN. 
#Instead, you have to delete the key constraint and then add a new 
#one with a different ON DELETE behavior.

## EXAMPLE
#-- Identify the correct constraint name
SELECT constraint_name, table_name, constraint_type
FROM information_schema.table_constraints
WHERE constraint_type = 'FOREIGN KEY';

#-- Drop the right foreign key constraint
ALTER TABLE affiliations
DROP CONSTRAINT affiliations_organization_id_fkey;

#-- Add a new foreign key constraint from affiliations to organizations which cascades deletion
ALTER TABLE affiliations
ADD CONSTRAINT affiliations_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES organizations (id) ON DELETE CASCADE;


##
-- Count the total number of affiliations per university
SELECT COUNT(*), professors.university_id 
FROM professors
JOIN affiliations
ON affiliations.professor_id = professors.id
-- Group by the ids of professors
GROUP BY professors.university_id 
ORDER BY count DESC;


############### BUSINESS ANALYSIS IN SQL ###################
## EXPLORATORY DATA ANALYSIS IN SQL (PostgresSQL)
# Database Client: program to access Database clients
# Entity-relationship diagrams
## column_name_a:column_name_b -> relationship btw column in a and b

# Note: count(column_name) : number of non-NULL values
#-- Select the count of profits_change, 
#-- subtract from total number of rows, and alias as missing
SELECT count(*) - count(profits_change) AS missing
FROM fortune500;

# Foreign key: reference another row in another table or the same table
## with a unique ID, non-NULL values only

## COALESCE function: operates rowise, returns first-non-NULL values
# if col1 = [NULL, NULL, 22, 3]
# if col2 = [10, NULL, NULL, 4]
# then coalesce(col1, col2) = [10, NULL, 22, 3]
# can specify fallback value when NULL: coalesce(col1, col2, 0) = [10, 0, 22, 3]

#-- Select the 3 columns desired
SELECT company.name, tag_type.tag, tag_type.type
  FROM company
  	   -- Join to the tag_company table
       INNER JOIN tag_company 
       ON company.id = tag_company.company_id
       -- Join to the tag_type table
       INNER JOIN tag_type
       ON tag_company.tag = tag_type.tag
  -- Filter to most common type
  WHERE type='cloud';

  #
  SELECT company_original.name, fortune500.title, fortune500.rank
  #-- Start with original company information
  FROM company AS company_original
       #-- Join to another copy of company with parent
       #-- company information
	   LEFT JOIN company AS company_parent
       ON company_original.id = company_parent.id 
       #-- Join to fortune500, only keep rows that match
       INNER JOIN fortune500 
       #-- Use parent ticker if there is one, 
       #-- otherwise original ticker
       ON coalesce(company_original.ticker, 
                   company_parent.ticker) = 
             fortune500.ticker
 #-- For clarity, order by rank
 ORDER BY rank; 

 ### Casting with CAST()
 # when you cast a column as a different type, the data is converted to
 # the new type only for the current query

 #showing distribution of values
SELECT revenues_change::integer, count(*)
  FROM fortune500
GROUP BY revenues_change::integer
 ORDER BY revenues_change;

 ## SUMMARY STATISTICS
 # VARIANCE: var_pop(), var_samp(), stddev_samp(), ...

# -- Select average revenue per employee by sector
SELECT sector,
       avg(revenues/employees::numeric) AS avg_rev_employee
  FROM fortune500
 GROUP BY sector
#-- Use the column alias to order the results
 ORDER BY avg_rev_employee;

# -- Select sector and summary measures of fortune500 profits
SELECT sector,
       min(profits),
       avg(profits),
       max(profits),
       stddev(profits)
  FROM fortune500
 GROUP BY sector
 ORDER BY avg(profits);

 ## SUBQUERY
SELECT stddev(maxval),
	   -- min
       min(maxval),
       -- max
       max(maxval),
       -- avg
       avg(maxval)
  FROM (SELECT max(question_count) AS maxval
          FROM stackoverflow
         GROUP BY tag) AS max_results; #-- alias for subquery

 ## TRUNCATE: SELECT trunc(42.1256, 2) replaces last 2 numbers by 0 = 42.12
 # SELECT trunc(12564.12, -2) = 12500 ## CHECK

# SELECT generate_series(1, 10, 2) -> 1, 3, 5, 7, 9

## CREATINGS BINS
# -- create bins
WITH bins AS (
	SELECT generate_series(30, 60, 5) AS lower,
		   generate_series(35, 65, 5) AS upper
		   ),
	# -- subset data to tag of interest
	ebs AS (
		SELECT unanswered_count
		FROM stackoverflow
		WHERE tag = 'amazon-ebs')
#-- count values in each bin
SELECT lower, upper, count(unanswered_count)
# left join keeps all bins
FROM bins
	LEFT JOIN ebs
		ON unanswered_count >= lower
		AND unanswered_count < upper
GROUP BY lower, upper
ORDER BY lower;

## ANOTHER ONE
#-- Bins created in Step 2
WITH bins AS (
      SELECT generate_series(2200, 3050, 50) AS lower,
             generate_series(2250, 3100, 50) AS upper),
     -- Subset stackoverflow to just tag dropbox (Step 1)
     dropbox AS (
      SELECT question_count 
        FROM stackoverflow
       WHERE tag='dropbox') 
#-- Select columns for result
#-- What column are you counting to summarize?
SELECT lower, upper, count(question_count) 
  FROM bins  #-- Created above
       #-- Join to dropbox (created above), 
       #-- keeping all rows from the bins table in the join
       LEFT JOIN dropbox
       #-- Compare question_count to lower and upper
         ON question_count >= lower 
        AND question_count < upper
 #-- Group by lower and upper to count values in each bin
 GROUP BY lower, upper
 #-- Order by lower to put bins in order
 ORDER BY lower;

## PERCENTILE FUNCTION
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY column_name)
FROM table;

### CREATING TEMPORARY TABLE ###########
CREATE TEMP TABLE top_companies AS 
	SELECT rank, title
	FROM fortune500
	WHERE rank < 10;
# other syntax is SELECT INTO

## can add rows 
INSERT INTO top_companies
SELECT rank, title
	FROM fortune500
	WHERE rank BETWEEN 11 AND 20;

## drop table (will also be )
DROP TABLE top_companies;
#or 
DROP TABLE IF EXISTS top_companies;

### EXERCISE
DROP TABLE IF EXISTS profit80;

CREATE TEMP TABLE profit80 AS
  SELECT sector, 
         percentile_disc(0.8) WITHIN GROUP (ORDER BY profits) AS pct80
    FROM fortune500 
   GROUP BY sector;

SELECT fortune500.title, 
       fortune500.sector, 
       fortune500.profits,
       fortune500.profits / profit80.pct80 AS ratio
FROM profit80
LEFT JOIN fortune500
ON fortune500.sector = profit80.sector
WHERE fortune500.profits > profit80.pct80;

## 
#-- Select tag (Remember the table name!) and mindate
SELECT startdates.tag, 
       startdates.mindate, 
       #-- Select question count on the min and max days
	   so_min.question_count AS min_date_question_count,
       so_max.question_count AS max_date_question_count,
       #-- Compute the change in question_count (max- min)
       so_min.question_count - so_max.question_count AS change
  FROM startdates
       #-- Join startdates to stackoverflow with alias so_min
       INNER JOIN stackoverflow AS so_min
          ON startdates.tag = so_min.tag
         AND startdates.mindate = so_min.date
       #-- Join to stackoverflow again with alias so_max
       INNER JOIN stackoverflow AS so_max
          ON startdates.tag = so_max.tag
         AND so_max.date = '2018-09-25';

## CREATING CORRELATIONS
DROP TABLE IF EXISTS correlations;

CREATE TEMP TABLE correlations AS
SELECT 'profits'::varchar AS measure,
       corr(profits, profits) AS profits,
       corr(profits, profits_change) AS profits_change,
       corr(profits, revenues_change) AS revenues_change
  FROM fortune500;

INSERT INTO correlations
SELECT 'profits_change'::varchar AS measure,
       corr(profits_change, profits) AS profits,
       corr(profits_change, profits_change) AS profits_change,
       corr(profits_change, revenues_change) AS revenues_change
  FROM fortune500;

INSERT INTO correlations
SELECT 'revenues_change'::varchar AS measure,
       corr(revenues_change, profits) AS profits,
       corr(revenues_change, profits_change) AS profits_change,
       corr(revenues_change, revenues_change) AS revenues_change
  FROM fortune500;

-- Select each column, rounding the correlations
SELECT measure,
       round(profits::numeric, 2) AS profits,
       round(profits_change::numeric, 2) AS profits_change,
       round(revenues_change::numeric, 2) AS revenues_change
FROM correlations;

### CHARACTER DATA ####
# char(n) -- FIXED length string, w/ trailing spaces added
# varchar(n) -- variable length
# text or varchar -- UNLIMITED length

# categorical: Monday, Tuesday, Saturday
# unstructured text

#-- Find values of zip that appear in at least 100 rows
#-- Also get the count of each value
SELECT zip, count(zip)
  FROM evanston311
 GROUP BY zip
HAVING count(zip) > 100; 

## Case insensitive searches
SELECT *
FROM fruit
WHERE fav_fruit LIKE '%apple%';
# returns apple, ' apple', etc

SELECT *
FROM fruit
WHERE fav_fruit ILIKE '%apple%';
# returns apple, Apple, APPLES, etc... but also pineapple

## TRIMMING SPACES
SELECT trim(' abc ') #trims both ends -> 'abc'
SELECT rtrim(' abc ') #trims right end -> ' abc'
SELECT ltrim(' abc ') #trims left end -> 'abc '

# can also remove specific examples
SELECT trim('Wow!', '!') #!w
SELECT trim('Wow!', '!wW') #o

## SELECTING SUBTRINGS
SELECT left('abcde', 2) #ab
SELECT right('abcde', 2) #de
SELECT substring('abcde' FROM 2 FOR 3) #bcd

# SPLIT 
SELECT split_part('a, bc, de', ',', 2) #bc, delimiter not included in returns

## CONCATENATE
SELECT concat('a', 2, 'cc') #a2cc
SELECT 'a' || 2 || 'cc'; #a2cc
SELECT concat('a', NULL, 'cc') #acc


## EXAMPLE
#-- Select the first 50 chars when length is greater than 50
SELECT CASE WHEN length(description) > 50
            THEN left(description, 50) || '...'
       #-- otherwise just select description
       ELSE description
       END
  FROM evanston311
 #-- limit to descriptions that start with the word I
 WHERE left(description, 2) LIKE '%I %'
 ORDER BY description;

 # CASE WHEN
 # want to split "Description :", "description - " etc

 SELECT CASE WHEN category LIKE '%: %' THEN split_part(category, ': ', 1)
 			 WHEN category LIKE '% - %' THEN split_part(category, ' - ', 1)
 			 ELSE split_part(category, ' | ', 1)
 		END AS major_category,
 		sum(businesses)
 	FROM naics
 	GROUP BY major_category;

 ## ALTERNATIVE
 # STEP 1: create temp table
 CREATE TEMP TABLE recode AS
 	SELECT DISTINCT fav_fruit AS original,
 					fav_fruit AS standardized
 	FROM fruit;

 # STEP 2: update values
 UPDATE recode
 	SET standardized = trim(lower(original));

 UPDATE recode
 	SET ...;

 # STEP 3: join original and recode
 SELECT standardized, count(*)
 FROM fruit
 LEFT JOIN recode
 ON fav_fruit = original
 GROUP BY standardized;

 ##EXAMPLE
DROP TABLE IF EXISTS indicators;

CREATE TEMP TABLE indicators AS
  #-- Select id
  SELECT id, 
         CAST (description LIKE '%@%' AS integer) AS email,
         CAST (description LIKE '%___-___-____%' AS integer) AS phone  #_ matches any character
    #-- What table contains the data? 
    FROM evanston311;

SELECT *
  FROM indicators;


## DATETIMES #########################
SELECT now() - '2020-01-01' #now is TODAY's date, is an interval

# need to CAST datetimes to date for = to work
SELECT count(*) 
FROM evanston311
WHERE date_created::date = '2017-01-31';

# can also cast '' to date then add numbers
SELECT count(*)
  FROM evanston311
 WHERE date_created >= '2017-03-13'
   AND date_created < '2017-03-13'::date + 1;

# -- Add 100 days to the current timestamp
SELECT now() + '100 days'::interval;