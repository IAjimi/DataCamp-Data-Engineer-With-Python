### SQL DATACAMP NOTES ########################################################################3
# return 5 rows
SELECT TOP(5) artist
FROM artists;

# return top 5% of rows
SELECT TOP(5) PERCENT artist
FROM artists;

# rename columns
SELECT demand_loss_mw AS lost_demand
from grid;

## CAN USE 'where' to filter
SELECT *
FROM album
WHERE artist_id IN (1, 3)

### WORKING WITH STRINGS
SELECT TOP (10)
  description, 
  CHARINDEX('Weather', description) AS start_of_string, # position of substring within string
  LEN ('Weather') AS length_of_string, # length of string
  SUBSTRING(                                            # subset string  
    description, 
    15, 
    LEN(description)
  ) AS additional_description 
FROM 
  grid
WHERE description LIKE '%Weather%';

### GROUPS
## filter groupby objects with HAVING

### INNER JOIN
SELECT
	table_a.columnX,
	table_a.columny,
	table_b.columnz
FROM
	table_a
INNER JOIN table_b on table_b.foreign_key = table_a.primary_key;

### COMBINING SIMILAR TABLES
# need same columns
SELECT *
FROM album
UNION
SELECT album_id
FROM album
WHERE ...

# UNION returns DISTINCT rows
# UNION ALL returns ALL, including duplicates

### CRUD
# Create table
CREATE TABLE table_name(
	col1name int,
	col2name varchar(20)
	)

# Insert into table
INSERT INTO table_name
	(col1name, col2name)
VALUES
	(val1, val2)

# Update table
UPDATE table_name
SET column_name = value,
WHERE ...
..

# Delete
DELETE 
FROM table_name
WHERE ...

# another way to delete entries
TRUNCATE TABLE table_name

### Saving variables with DECLARE
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

### TEMPORARY TABLES
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

SELECT sector,
       avg(revenues/employees::numeric) AS avg_rev_employee
  FROM fortune500
 GROUP BY sector
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
       min(maxval),
       max(maxval),
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
     #-- Subset stackoverflow to just tag dropbox (Step 1)
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

## EXTRACTING FIELDS
date_part('field', timestamp)
EXTRACT(FIELD FROM timestamp)

# date trunc?
SELECT date_trunc('month', date) AS month,
		sum(amt) AS Total_Sales
FROM sales
GROUP BY month
ORDER BY month;

## CREATE SERIES OF DATES
SELECT generate_series('2018-01-01', '2018-12-31', '1 month'::interval)

# Finding missing values
WITH hour_series AS(
	SELECT generate_series('2018-04-23 09:00:00', '2018-04-23 14:00:00', '1 hour'::interval) AS hours)

SELECT hours, count(date)
FROM hour_series
LEFT JOIN sales
ON hours = date_trunc('hour', date)
GROUP BY hours
ORDER BY hours;

# EXAMPLE
SELECT day
#-- 1) Subquery to generate all dates
#-- from min to max date_created
  FROM (SELECT generate_series(min(date_created),
                               max(date_created),
                               '1 day')::date AS day
          FROM evanston311) AS all_dates
#-- 4) Select dates (day from above) that are NOT IN the subquery
 WHERE day NOT IN 
       (SELECT date_created::date
          FROM evanston311);


############ DATA DRIVEN DECISION MAKING WITH SQL ###################################3
#-- For each country report the earliest date when an account was created
SELECT country,
	min(date_account_start) AS first_account
FROM customers
GROUP BY country, date_account_start
ORDER BY date_account_start;

SELECT country, -- For each country report the earliest date when an account was created
	MIN(date_account_start) AS first_account
FROM customers
GROUP BY country
ORDER BY first_account;

## SQL NOTE: The average is null only if all values are null.

## NOTE: need to be careful & intentional about the columns returned
# selecting renting_price w/o sum won't work bc the other columns are of length 1 
SELECT 
	sum(m.renting_price), 
	count(*), 
	count(r.customer_id)  
FROM renting AS r
LEFT JOIN movies AS m
ON r.movie_id = m.movie_id;

## NESTED QUERY
# SELECT block in WHERE or HAVING -> created 1st
## SELECT EXAMPLE
# -- step 1: inner query
SELECT DISTINCT customer_id
FROM renting
WHERE rating <= 3

# -- step 2: outer query
SELECT name
FROM customers
WHERE customer_id IN
	( SELECT DISTINCT customer_id
	FROM renting
	WHERE rating <= 3
	)

# HAVING EXAMPLE: earliest account created
# for countries where account was created BEFORE 1st Austrian account
SELECT country, MIN(date_account_start)
FROM customers
GROUP BY country
HAVING MIN(date_account_start) < (
	SELECT MIN(date_account_start)
	FROM customers
	WHERE country = 'Austria'
	)

# PRACTICE
## movies watched more than 5 times
SELECT *
FROM movies
WHERE movie_id IN  
	(SELECT movie_id
	FROM renting
	GROUP BY movie_id
	HAVING COUNT(*) > 5)

## movies with above average ratings
SELECT movie_id,  
       avg(rating)
FROM renting
GROUP BY movie_id
HAVING avg(rating) >         
	(SELECT AVG(rating)
	FROM renting);

## CORRELATED QUERIES
# condition in the where clause of INNER query depends on column in outer query
# nested query is evaluated once for every row, kind of like for loop over rows
## replaces GROUP BY?

# EXAMPLE -- Select customers with less than 5 movie rentals
SELECT *
FROM customers as c
WHERE 5 >
	(SELECT count(*)
	FROM renting as r
	WHERE r.customer_id = c.customer_id);

## EXISTS
# special case of a correlated nested query
# used to check if result is empty, returns T or F
SELECT *
FROM movies as m
WHERE NOT EXISTS
 (
	SELECT *
	FROM renting as r
	WHERE rating IS NOT NULL
	AND r.movie_id = m.movie_id
	)

# Get all Actors in Comedies
SELECT *
FROM actors AS a
WHERE EXISTS
	(SELECT *
	 FROM actsin AS ai
	 LEFT JOIN movies AS m
	 ON m.movie_id = ai.movie_id
	 WHERE m.genre = 'Comedy'
	 AND ai.actor_id = a.actor_id);

## UNION, INTERSECT
# UNION: same columns must be selected for all tables
# INTERSECT: all values that appear in both tables

## EXAMPLES 
#-- Select all actors who are not from the USA and who are also born after 1990
SELECT name, 
       nationality, 
       year_of_birth
FROM actors
WHERE nationality <> 'USA'
INTERSECT 
SELECT name, 
       nationality, 
       year_of_birth
FROM actors
WHERE year_of_birth > 1990;

#-- Select all movies of genre drama with average rating higher than 9
SELECT *
FROM movies
WHERE movie_id IN 
   (SELECT movie_id
    FROM movies
    WHERE genre = 'Drama'
    INTERSECT
    SELECT movie_id
    FROM renting
    GROUP BY movie_id
    HAVING AVG(rating)>9);

## OLAP: On-Line Analytical Processing
### CUBE
 SELECT country,
 		genre,
 		COUNT(*)
 FROM renting_extended
 GROUP BY CUBE (country, genre); #group by each combination of country and genre, plus country and genre alone

 ### ROLLUP
 SELECT country,
 		genre,
 		count(*)
 FROM renting_extended
 GROUP BY ROLLUP (country, genre);
 #aggregation of each combination of country + genre, country only, total
 ## -> GROUP BY CUBE minus one level

### GROUPING SETS
# union over group by statements
SELECT country,
	   genre,
	   count(*)
FROM renting_extended
GROUP BY GROUPING SETS ((country, genre), (country), (genre), ());
# returns group by country & genre, then country only, then genre only, and TOTAL agg (the "()") -> UNION of those results

########### APPLYING SQL TO REAL WORLD PROBLEMS ###########################
#upper, lower -> for strings
#EXTRACT('minute' FROM date_col)
#string_agg() -> concatenate strings in a column
SELECT rating, 
	STRING_AGG(title, ', ') as films
FROM film
GROUP BY rating;
#returns all films per rating CONCATENATED into one string

## Find the right tables
# need to know column names and content
# QUERY specific system tables -> dpeends on db system
## POSTGRES SQL
SELECT *
FROM pg_catalog.pg_tables;

## MY SQL:
SHOW TABLES;

# SEE ALL COLUMNS BY TABLE IN catalog
SELECT table_name,
	   STRING_AGG(column_name, ', ')
FROM information_schema.columns
WHERE table_schema = 'public'
GROUP BY table_name;

# Save it as a VIEW, a virtual table
CREATE VIEW name_of_view AS
( 
	#subquery
	)

## STORING DATA
# create a table using new data
## step 1 - create new table
## step 2 - insert data into table
INSERT into table_name (col1name, col2name)
VALUES (1, 2),
	   (1, 2),
	   (1, 2);

# create a table using existing data -> data is STORED (static), stores the DATA
CREATE TABLE family_films AS
	SELECT film_id, title
	FROM film
	WHERE rating = 'G';

# creating a view using existing data -> QUERY is STORED (dynamic), updates when data is changed
## -> CANNOT change data directly
CREATE VIEW family_films AS
	SELECT film_id, title
	FROM film
	WHERE rating = 'G';

## UPDATE
UPDATE table_name
SET email = lower(email);

UPDATE table_name
SET email = lower(email)
WHERE active = 'True';

UPDATE film
SET rental_rate = rental_rate + 1
WHERE film_id IN 
  (SELECT film_id 
  	FROM actor AS a
   INNER JOIN film_actor AS f
      ON a.actor_id = f.actor_id
   WHERE last_name IN ('WILLIS', 'CHASE', 'WINSLET', 'GUINESS', 'HUDSON')
   );

## DELETE
# Remove a table
DROP TABLE table_name;

# CLEAR table of ALL records
TRUNCATE TABLE table_name; #either this or below
DELETE FROM table_name;

# Clear table of SOME records
DELETE FROM customers
WHERE active = 'False';

## BEST PRACTICE IN WRITING SQL SCRIPTS
# always use AS
# specify join
# clear aliases
# use comments
# syntax highlighting: commands CAPITALiZED
# use snake_case
# IN instead of multiple ORs
# use BETWEEN x and Y instead of <= and >=


### ANALYZING BUSINESS DATA IN SQL ############################
## Common Table Expressions (CTEs): store a query's results in a temporary table
# so it can be referenced later
WITH 

	table_1 AS (
	SELECT ...
	FROM ...),

	table_2 AS (
	SELECT ...
	FROM ...)

## EXAMPLE
# some KPIs: 
## Monthly Active Users (MAU), New Registrations / Month

WITH revenue AS (
  #-- Calculate revenue per eatery
  SELECT meals.eatery,
         sum(orders.order_quantity * meals.meal_price) AS revenue
    FROM meals
    JOIN orders ON meals.meal_id = orders.meal_id
   GROUP BY eatery),

  cost AS (
  #-- Calculate cost per eatery
  SELECT meals.eatery,
         sum(stock.stocked_quantity * meals.meal_cost) AS cost
    FROM meals
    JOIN stock ON meals.meal_id = stock.meal_id
   GROUP BY eatery)

   #-- Calculate profit per eatery
   SELECT revenue.eatery,
          revenue - cost AS profit
     FROM revenue
     JOIN cost ON revenue.eatery = cost.eatery
    ORDER BY profit DESC;

## SAME, GROUPED BY MONTH
WITH revenue AS ( 
	SELECT
		DATE_TRUNC('month', order_date) :: DATE AS delivr_month,
		sum(meals.meal_price * orders.order_quantity) AS revenue
	FROM meals
	JOIN orders ON meals.meal_id = orders.meal_id
	GROUP BY delivr_month),

  cost AS (
 	SELECT
		DATE_TRUNC('month', stocking_date) :: DATE AS delivr_month, #imp: cast to DATE
		sum(meals.meal_cost * stock.stocked_quantity) AS cost
	FROM meals
    JOIN stock ON meals.meal_id = stock.meal_id
	GROUP BY delivr_month)

SELECT
	revenue.delivr_month ,
	revenue.revenue - cost.cost AS profit
FROM revenue
JOIN cost ON revenue.delivr_month = cost.delivr_month
ORDER BY revenue.delivr_month ASC;

### WINDOW FUNCTIONS
## perform an operation across a set of rows related to the current row
# EX: calculate a running total
WITH maus AS(
	SELECT
		DATE_TRUNC('month', order_date)::DATE AS delivr_month,
		COUNT(DISTINCT user_id) AS mau
	FROM orders
	GROUP BY delivr_month
	)

SELECT
	delivr_month,
	mau,
	COALESCE(
		LAG(mau) OVER (ORDER BY delivr_month ASC),
		1
		) AS last_mau
FROM maus
ORDER BY delivr_month ASC
LIMIT 3;

## ANOTHER EX
WITH reg_dates AS (
  SELECT
    user_id,
    MIN(order_date) AS reg_date
  FROM orders
  GROUP BY user_id),

  regs AS (
  SELECT
    DATE_TRUNC('month', reg_date) :: DATE AS delivr_month,
    COUNT(DISTINCT user_id) AS regs
  FROM reg_dates
  GROUP BY delivr_month)

SELECT
  -- Calculate the registrations running total by month
  delivr_month,
  	SUM(regs) OVER (ORDER BY delivr_month ASC) AS regs_rt
FROM regs
-- Order by month in ascending order
ORDER BY delivr_month ASC; 


## RETENTION RATE QUERY
WITH user_activity AS (
	SELECT DISTINCT
	DATE_TRUNC('month', order_date) :: DATE AS delivr_month,
	user_id
	FROM orders
	)

SELECT previous.delivr_month,
	   ROUND(
		COUNT(DISTINCT current.user_id) :: NUMERIC /
		GREATEST(COUNT(DISTINCT previous.user_id), 1), #greatest to avoid dividing by 0
		2) AS retention
FROM user_activity AS previous
LEFT JOIN user_activity AS current
ON previous.user_id = current.user_id
AND previous.delivr_month = (current.delivr_month - INTERVAL '1 month')
GROUP BY previous.delivr_month
ORDER BY previous.delivr_month ASC
LIMIT 3;


## UNIT ECONOMICS -- DISTRIBUTION
## 2 ways to do this
# Method #1
#-- Create a CTE named kpi
WITH kpi AS  (
  SELECT
    user_id,
    SUM(m.meal_price * o.order_quantity) AS revenue
  FROM meals AS m
  JOIN orders AS o ON m.meal_id = o.meal_id
  GROUP BY user_id)

#-- Calculate ARPU
SELECT ROUND(AVG(revenue) :: NUMERIC, 2) AS arpu
FROM kpi;

# Method 2
WITH kpi AS (
  SELECT
    #-- Select the week, revenue, and count of users
    date_trunc('week', o.order_date) :: DATE AS delivr_week,
    sum(o.order_quantity * m.meal_price) AS revenue,
    count(distinct o.user_id) AS users
  FROM meals AS m
  JOIN orders AS o ON m.meal_id = o.meal_id
  GROUP BY delivr_week)

SELECT
  delivr_week,
  #-- Calculate ARPU
  ROUND(
    revenue :: NUMERIC / users,
  2) AS arpu
FROM kpi
ORDER BY delivr_week ASC;


## PLOTTING HISTORGRAMS / CREATING FREQUENCY TABLES
WITH user_revenues AS (
  SELECT
    #-- Select the user ID and revenue
    user_id,
    sum(m.meal_price * o.order_quantity) AS revenue
  FROM meals AS m
  JOIN orders AS o ON m.meal_id = o.meal_id
  GROUP BY user_id)

SELECT
  #-- Return the frequency table of revenues by user
  round(revenue::NUMERIC, -2) AS revenue_100,
  count(user_id) AS users
FROM user_revenues
GROUP BY revenue_100
ORDER BY revenue_100 ASC;


## COULD BUCKET INSTEAD
WITH user_revenues AS (
  SELECT
    #-- Select the user IDs and the revenues they generate
    user_id,
    sum(m.meal_price * o.order_quantity) AS revenue
  FROM meals AS m
  JOIN orders AS o ON m.meal_id = o.meal_id
  GROUP BY user_id)

SELECT
  #-- Fill in the bucketing conditions
  CASE
    WHEN revenue < 150 THEN 'Low-revenue users'
    WHEN revenue BETWEEN 150 AND 300 THEN 'Mid-revenue users'
    ELSE 'High-revenue users'
  END AS revenue_group,
  count(distinct user_id) AS users
FROM user_revenues
GROUP BY revenue_group;

## PERCENTILES
WITH user_revenues AS (
  #-- Select the user IDs and their revenues
  SELECT
    user_id,
    sum(m.meal_price * o.order_quantity) AS revenue
  FROM meals AS m
  JOIN orders AS o ON m.meal_id = o.meal_id
  GROUP BY user_id)

SELECT
  #-- Calculate the first, second, and third quartile
  ROUND(
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY revenue ASC) :: NUMERIC,
  2) AS revenue_p25,
  ROUND(
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY revenue ASC) :: NUMERIC,
  2) AS revenue_p50,
  ROUND(
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY revenue ASC) :: NUMERIC,
  2) AS revenue_p75,
  -- Calculate the average
  ROUND(avg(revenue) :: NUMERIC, 2) AS avg_revenue
FROM user_revenues;


## SURVEY OF USEFUL FUNCTIONS
# DATES
# changing '2018-08-13' -> 'Friday 13, August 2018'
TO_CHAR('2018-08-13', 'FMDay  DD, FMMonth FYY') #check doucmentation

# WINDOW FUNCTIONS
SUM(...) OVER (...)
LAG(...) OVER (...)
RANK() OVER (...) 

# PIVOTING -> CROSSTAB()
## convert long to wide column
CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT * FROM CROSSTAB($$
SELECT
		meal_id,
		DATE_TRUNC('month', order_date) :: DATE AS delivr_month,
		COUNT(DISTINCT order_id) :: INT AS orders
	FROM orders
	WHERE meal_id IN (0, 1)
		AND order_date < '2018-08-01'
	GROUP BY meal_id, delivr_month
	ORDER BY meal_id, delivr_month $$)
	AS ct (meal_id INT,
	"2018-06-01" INT,
	"2018-07-01" INT)
	ORDER BY meal_id ASC;

## ANOTHER ONE
CREATE EXTENSION IF NOT EXISTS tablefunc;

#-- Pivot the previous query by quarter
SELECT * FROM CROSSTAB($$
  WITH eatery_users AS  (
    SELECT
      eatery,
      #-- Format the order date so "2018-06-01" becomes "Q2 2018"
      TO_CHAR(order_date, '"Q"Q YYYY') AS delivr_quarter,
      #-- Count unique users
      COUNT(DISTINCT user_id) AS users
    FROM meals
    JOIN orders ON meals.meal_id = orders.meal_id
    GROUP BY eatery, delivr_quarter
    ORDER BY delivr_quarter, users)

  SELECT
    #-- Select eatery and quarter
    eatery,
    delivr_quarter,
    #-- Rank rows, partition by quarter and order by users
    RANK() OVER
      (PARTITION BY delivr_quarter
       ORDER BY users DESC) :: INT AS users_rank
  FROM eatery_users
  ORDER BY eatery, delivr_quarter;
$$)
#-- Select the columns of the pivoted table
AS  ct (eatery TEXT,
        "Q2 2018" INT,
        "Q3 2018" INT,
        "Q4 2018" INT)
ORDER BY "Q4 2018";


###### REPORTING IN SQL ###################################################
## COMBINE 2 TABLES w/o relationships w/ union

#-- Select sport and events for summer sports
SELECT 
	sport, 
    count(distinct event) AS events
FROM summer_games
GROUP BY sport
UNION
#-- Select sport and events for winter sports
SELECT 
	sport, 
    count(distinct event) AS events
FROM winter_games
GROUP BY sport
#-- Show the most events at the top of the report
ORDER BY events DESC;

## COMPLEX JOINS / UNIONS
# either join countries on summer and winter games sep, then union results

SELECT 
	'summer' AS season, 
    country, 
    count(distinct event) AS events
FROM summer_games AS s
JOIN countries AS c
ON c.id = s.country_id
GROUP BY c.country

UNION

SELECT 
	'winter' AS season, 
    country, 
    count(distinct event) AS events
FROM winter_games AS w
JOIN countries AS c
ON c.id = w.country_id
GROUP BY c.country

ORDER BY events DESC;

# alternative
SELECT 
	season, 
    c.country, 
    count(distinct event) AS events
FROM
    #-- Pull season, country_id, and event for both seasons
    (SELECT 
     	'summer' AS season, 
     	country_id, 
     	event
    FROM summer_games
    UNION
    SELECT 
     	'winter' AS season, 
        country_id, 
     	event
    FROM winter_games) AS subquery
JOIN countries AS c
ON c.id = subquery.country_id
#-- Group by any unaggregated fields
GROUP BY c.country, subquery.season
#-- Order to show most events at the top
ORDER BY events DESC;

## MULTIPLE CASE WHEN
SELECT 
	name,
	CASE 
	WHEN (gender = 'F' AND height > 175) THEN 'Tall Female'
    WHEN (gender = 'M' AND height > 190 )THEN 'Tall Male'
    ELSE 'Other' END AS segment
FROM athletes;

## USING SUBQUERIES TO FILTER DATA
SELECT 
	sum(bronze) AS bronze_medals, 
    sum(silver) AS silver_medals, 
    sum(gold) AS gold_medals
FROM summer_games
WHERE athlete_id IN
    (SELECT id
     FROM athletes
     WHERE age <= 16);


## CONVERTING DATA TYPES
# ERROR: Function avg(...) does not exist -> TYPE error (numeric function on non numeric field)
# ERROR: Operator does not exist -> another TYPE error (2 KEYs as different types)
CAST(variable AS type)

## CLEANING STRINGS
SELECT REPLACE(string, '.') AS new_string
SELECT LEFT(string, 2)
SELECT UPPER(string)
SELECT INITCAP(string) #title case
SELECT LOWER(string)
SELECT TRIM(string) #remove all spaces

## DEALING WITH NULLS
# Fix 1: filtering unlls
# Fix 2: replace with COALESCE()

# Ratio of null rows
SELECT SUM(CASE WHEN country IS NULL THEN 1 ELSE 0) / sum(1.00)
FROM orders;

## WAYS TO FIX DUPLICATION
# Fix 1: remove aggregation that causes the duplication
# Fix 2: add a field to JOIN
# Fix 3: add subquery?

## WINDOW FUNCTION
SUM(value) OVER (PARTITION BY field ORDER BY field)
ROW_NUMBER() OVER (PARTITION BY field ORDER BY field)

# EXAMPLE
SELECT country_id,
	athlete_id,
	SUM(bronze) OVER (PARTITION BY country_id) AS total_bronze #total bronze medals by country
FROM summer_games;

# MAY RUN INTO ISSUES WHEN COMBINED WITH GROUP BY
# NOT
SELECT team_id,
	SUM(points) AS team_points,
	SUM(points) OVER () AS league_points
FROM table
GROUP BY team_id;

# BUT
SELECT team_id,
	SUM(points) AS team_points,
	SUM(SUM(points)) OVER () AS league_points
FROM table
GROUP BY team_id;

## 

#-- Query region, athlete name, and total_golds
SELECT 
	region,
    athlete_name,
    total_golds
FROM
    (SELECT 
		#-- Query region, athlete_name, and total gold medals
        region, 
        name AS athlete_name, 
        SUM(gold) AS total_golds,
        #-- Assign a regional rank to each athlete
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(gold) DESC) AS row_num
    FROM summer_games_clean AS s
    JOIN athletes AS a
    ON a.id = s.athlete_id
    JOIN countries AS c
    ON s.country_id = c.id
    #-- Alias as subquery
    GROUP BY region, athlete_name) AS subquery
#-- Filter for only the top athlete per region
WHERE row_num = 1;

## PERCENT OF TOTAL CALC
SELECT 
	region,
    country,
	SUM(gdp) AS country_gdp,
    SUM(SUM(gdp)) OVER () AS global_gdp,
    SUM(gdp) / SUM(SUM(gdp)) OVER () AS perc_global_gdp
FROM country_stats AS cs
JOIN countries AS c
ON cs.country_id = c.id
WHERE gdp IS NOT NULL
GROUP BY region, country
ORDER BY country_gdp DESC;


##
-- Pull country_gdp by region and country
SELECT 
	region,
    country,
	SUM(gdp) AS country_gdp,
    -- Calculate the global gdp
    SUM(SUM(gdp)) OVER () AS global_gdp,
    -- Calculate percent of global gdp
    SUM(gdp) / SUM(SUM(gdp)) OVER () AS perc_global_gdp,
    -- Calculate percent of gdp relative to its region
    SUM(gdp) / SUM(SUM(gdp)) OVER (PARTITION BY region) AS perc_region_gdp
FROM country_stats AS cs
JOIN countries AS c
ON cs.country_id = c.id
-- Filter out null gdp values
WHERE gdp IS NOT NULL
GROUP BY region, country
-- Show the highest country_gdp at the top
ORDER BY country_gdp DESC;


##
-- Bring in region, country, and gdp_per_million
SELECT 
    region,
    country,
    SUM(gdp) / SUM(pop_in_millions) AS gdp_per_million,
    SUM(SUM(gdp)) OVER () / SUM(SUM(pop_in_millions)) OVER () AS gdp_per_million_total
FROM country_stats_clean AS cs
JOIN countries AS c 
ON cs.country_id = c.id
WHERE year = '2016-01-01' AND gdp IS NOT NULL
GROUP BY region, country
ORDER BY gdp_per_million DESC;

## MORE STUFF
SELECT
	DATE_PART('month', date) AS month,
	country_id,
    SUM(views) AS month_views,
    LAG(SUM(views)) OVER (PARTITION BY country_id ORDER BY DATE_PART('month', date)) AS previous_month_views,
    SUM(views) / LAG(SUM(views)) OVER (PARTITION BY country_id ORDER BY DATE_PART('month', date)) - 1 AS perc_change
FROM web_data
WHERE date <= '2018-05-31'
GROUP BY month, country_id;

## ROLLING AVERAGE
# syntax
AVG(value) OVER (PARTITION BY field ORDER BY field ROWS BETWEEN N PRECEDING AND CURRENT ROW)

# EXAMPLE: 7 day rolling average
SELECT
	date,
	SUM(views) AS daily_views,
	AVG(SUM(views)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg
FROM web_data
GROUP BY date;

# then using to find week on week change
SELECT 
	date,
    weekly_avg,
    LAG(weekly_avg,7) OVER (ORDER BY date) AS weekly_avg_previous,
    (weekly_avg / LAG(weekly_avg,7) OVER (ORDER BY date)) - 1 AS perc_change
FROM
  (SELECT
      date,
      SUM(views) AS daily_views,
      AVG(SUM(views)) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS weekly_avg
  FROM web_data
  GROUP BY date) AS subquery
ORDER BY date DESC;

### DATABASE DESIGN ######################################################
### OLTP & OLAP
## OLTP : application-oriented, simple transactions & frequent updates
## OLAP : complex, agg queries, limited updates

### STAR SCHEMA
## FACT TABLES
# hold record of metric, changes regularly, connects to dimensions via foreign keys

## DIMENSION TABLES
# holds description of attributes, does not change as often

## NORMALIZATION
# divide tables into smaller tables, connect them via relationship
# goal is to reduce redundancy and increase data integrity
# drawback: a lot of joins necessary to get full data back, but quicker
# better on OLTP (operational) databases (write-intensive, helps safer insertion of data)


### DATABASE VIEWS
SELECT * FROM INFORMATION_SCHEMA.views #to see all views

#-- Create a view for reviews with a score above 9
CREATE VIEW high_scores AS
SELECT * FROM REVIEWS
WHERE score > 9;

#-- Count the number of self-released works in high_scores
SELECT COUNT(*) FROM high_scores
INNER JOIN labels ON labels.reviewId = high_scores.reviewId
WHERE label = 'self-released';

### UPDATING / REPLACING VIEWS
## not all views are updatable
# view must be made up of 1 table, doesn't use a window or agg fct
## not all views are insertable

#-- Redefine the artist_title view to have a label column
CREATE OR REPLACE VIEW artist_title AS
SELECT reviews.reviewid, reviews.title, artists.artist, labels.label
FROM reviews
INNER JOIN artists
ON artists.reviewid = reviews.reviewid
INNER JOIN labels
ON labels.reviewid = reviews.reviewid;

SELECT * FROM artist_title;

# CASCADE can get rid of multiple views
DROP VIEW top_15_2017 CASCADE;

# by default, DROP VIEW runs with RESTRICT: returns an error if 
# any objects depend on the view

## Redefining a view
CREATE OR REPLACE view_name AS new_query

### Privileges 
#-- Revoke everyone's update and insert privileges
REVOKE INSERT, UPDATE ON long_reviews FROM PUBLIC; 

#-- Grant the editor update and insert privileges 
GRANT INSERT, UPDATE ON long_reviews TO editor; 

## MATERIALIZED VIEWS
# materialized = 'physical'
# store the query results, not the query
# consumes more storage
# for long running queries, where underlying result doesnt change often

CREATE MATERIALIZED VIEW my_view AS SELECT * FROM table_name;

REFRESH MATERIALIZED VIEW my_view;

## NON-MATERIALIZED VIEWS
# the views we've defined so far
# gets refreshed so always returns up-to-date data
# better for write-intensive databases

### DATABASE ROLES AND ACCESS CONTROL
## Group Role
CREATE ROLE data_analyst;

## User Role with some attributes set
CREATE ROLE alex WITH PASSWORD 'PasswordForIntern' VALID UNTIL '2020-01-01';
GRANT data_analyst TO alex;
REVOKE data_analyst FROM alex;

CREATE ROLE admin CREATEDB;

## Partitioning
# reducing size of large dbs
# e.g., creating new, smaller dbs for each quarter of the year
CREATE TABLE film_partitioned (
  film_id INT,
  title TEXT NOT NULL,
  release_year TEXT
)
PARTITION BY LIST (release_year);

#-- Create the partitions for 2019, 2018, and 2017
CREATE TABLE film_2019
	PARTITION OF film_partitioned FOR VALUES IN ('2019');

CREATE TABLE film_2018
	PARTITION OF film_partitioned FOR VALUES IN ('2018');

CREATE TABLE film_2017
	PARTITION OF film_partitioned FOR VALUES IN ('2017');

#-- Insert the data into film_partitioned
INSERT INTO film_partitioned
SELECT film_id, title, release_year FROM film;

#-- View film_partitioned
SELECT * FROM film_partitioned;

### IMPROVING QUERY PERFORMANCE IN SQL ######################################################
### PROCESSING ORDER
## Error messages come by processing order
# 1. FROM
# 2. WHERE
# 3. SELECT
# 4. ORDER BY

## Generally
# 1. FROM
# 2. ON
# 3. JOIN
# 4. WHERE
# 5. GROUP BY
# 6. HAVING

# 7. SELECT

# 8. DISTINCT
# 9. ORDER BY
# 10. TOP

## It can be more efficient to use filters with agg after?
SELECT PlayerName, 
       Team, 
       Position, 
       AvgRebounds
FROM                          
	(SELECT 
      PlayerName, 
      Team, 
      Position,
     (DRebound+ORebound)/CAST(GamesPlayed AS numeric) AS AvgRebounds
	 FROM PlayerStats) AS tr
WHERE AvgRebounds >= 12;

# INNER JOINS can be more time efficient than correlated SUBQUERIES
# EXCEPT does the opposite of INTERSECT. It is used to check if data, present in one table, is absent in another.

# EXISTS will stop searching when condition is TRUE
# IN collects all results
# ---> IN might be slower

# NOT IN : if the columns in the sub-query being evaluated for a non-match contain NULL values, no results are returned.
#  A workaround to get the query working correctly is to use IS NOT NULL in a WHERE filter condition in the sub-query.

# An inclusive LEFT OUTER JOIN returns all the rows in the left query, whereas an exclusive LEFT OUTER JOIN returns only rows in the left query that are absent 
# in the right query.

SET STATISTICS TIME ON

## Page read statistics
SET STATISTICS IO ON
# all data is stored in 8 kilobyte pages
# one page can store many rows or one value could span multiple pages
# a page can only belong to one table
# SQL server works with pages cached in memory
## STATISTICS TIME logical reads tells you how many kb required

# Logical reads are a measure of the number of the 8-kilobyte pages read from 
# memory to process and return the results of your query. 
# In general, the more pages that need to be read the slower your query will run.


### Index
## applied to table columns
## structure to improve speed of accessing data
## Clustered: like a dictionary, only one per table
## Non-clustered: like textbook with index in the back, can have multiple per table

# This affects the order in pages are stored and therefore query speed 

### Optimizing execution plans
## evalutes multiple execution plans, selects the one optimized for lowest cost
## looking at processor usage, memory usage, data page reads
## can be done with SMSS
## no information about query time however

### Building and Optimizing Triggers in SQL Server ######################################################
### Data Manipulation Language trigger
# INSERT, UPDATE, DELETE
### Data Definition Language
# CREATE, ALTER, or DROP
### Logon triggers
# LOGON
### INSTEAD OF trigger
## stops the original trigger from being executed
## uses replacement statement instead
# e.g., prevent insertions, updates, deletions, object modifications

### Adds actions after trigger event
CREATE TRIGGER ProductsTrigger
ON Products # needs to be attached to a table
AFTER INSERT # trigger behavior type
AS # action executed by the trigger
PRINT ('An insert blabla') 	

### Blocks event
CREATE TRIGGER ProductsTrigger
ON Products # needs to be attached to a table
INSTEAD OF UPDATE # trigger behavior type
AS # action executed by the trigger
PRINT ('An insert blabla') 

### Triggers vs stored procedures
## Triggers
# fired automatically by event
# dont allow parameters or transactions
# cannot return values as outputs

## Stored procedures
# run only when called explicitly
# accept input parameters and transactions
# can return values as outputs

## Computed columns
# cant use columns from other tables

### EXAMPLE
#-- Create a new trigger to keep track of discounts
CREATE TRIGGER CustomerDiscountHistory
ON Discounts
AFTER UPDATE
AS
	#-- Store old and new values into the `DiscountsHistory` table
	INSERT INTO DiscountsHistory (Customer, OldDiscount, NewDiscount, ChangeDate)
	SELECT i.Customer, d.Discount, i.Discount, GETDATE()
	FROM inserted AS i
	INNER JOIN deleted AS d ON i.Customer = d.Customer;

### INSTEAD OF
## performs instead of DML event, which does not run anymore
## used with INSERT, UPDATE, DELETE statements

### EXAMPLE
#-- Create the trigger to log table info
CREATE TRIGGER TrackTableChanges
ON DATABASE
FOR CREATE_TABLE,
	ALTER_TABLE,
	DROP_TABLE
AS
	INSERT INTO TablesChangeLog (EventData, ChangedBy)
    VALUES (EVENTDATA(), USER);

# note: AFTER and FOR will have the same outcome when used in a trigger definition

### EXAMPLE
#-- Add a trigger to disable the removal of tables
CREATE TRIGGER PreventTableDeletion
ON DATABASE
FOR DROP_TABLE
AS
	RAISERROR ('You are not allowed to remove tables from this database.', 16, 1);
    -- Revert the statement that removes the table
    ROLLBACK;

### EXAMPLE
CREATE TRIGGER LogonAudit
ON ALL SERVER WITH EXECUTE AS 'sa'
FOR LOGON

### EXAMPLE
-- Save user details in the audit table
INSERT INTO ServerLogonLog (LoginName, LoginDate, SessionID, SourceIPAddress)
SELECT ORIGINAL_LOGIN(), GETDATE(), @@SPID, client_net_address
-- The user details can be found in SYS.DM_EXEC_CONNECTIONS
FROM SYS.DM_EXEC_CONNECTIONS WHERE session_id = @@SPID;

### EXAMPLE
-- Create a trigger firing when users log on to the server
CREATE TRIGGER LogonAudit
-- Use ALL SERVER to create a server-level trigger
ON ALL SERVER WITH EXECUTE AS 'sa'
-- The trigger should fire after a logon
AFTER LOGON
AS
	-- Save user details in the audit table
	INSERT INTO ServerLogonLog (LoginName, LoginDate, SessionID, SourceIPAddress)
	SELECT ORIGINAL_LOGIN(), GETDATE(), @@SPID, client_net_address
	FROM SYS.DM_EXEC_CONNECTIONS WHERE session_id = @@SPID;

### Known limitations of triggers
## Disadvantage of triggers
# hard to view and detect

## Finding server-level trigger
SELECT * FROM sys.server_triggers;

## Finding database and table triggers
SELECT * FROM sys.triggers;

## Viewing a trigger definition
SELECT definition
FROM sys.sql_modules
WHERE object_id = OBJECT_ID ('PreventOrdersUpdate')

## alternate
SELECT OBJECT_DEFINITION (OBJECT_ID ('PreventOrdersUpdate'))


### EXAMPLE
-- Gather information about database triggers
SELECT name AS TriggerName,
	   parent_class_desc AS TriggerType,
	   create_date AS CreateDate,
	   modify_date AS LastModifiedDate,
	   is_disabled AS Disabled,
	   is_instead_of_trigger AS InsteadOfTrigger,
       -- Get the trigger definition by using a function
	   OBJECT_DEFINITION (object_id)
FROM sys.triggers
UNION ALL
-- Gather information about server triggers
SELECT name AS TriggerName,
	   parent_class_desc AS TriggerType,
	   create_date AS CreateDate,
	   modify_date AS LastModifiedDate,
	   is_disabled AS Disabled,
	   0 AS InsteadOfTrigger,
       -- Get the trigger definition by using a function
	   OBJECT_DEFINITION (object_id)
FROM sys.server_triggers
ORDER BY TriggerName;

### Use cases for AFTER triggers (DML)
## Keeping a history of row changes
CREATE TRIGGER CopyCustomersToHistory
ON Customers
AFTER INSERT, UPDATE
AS 
	INSERT INTO CUstomersHistory (Customer, ContractID, Address, PhoneNo)
	SELECT Customer, ContractID, Address, PhoneNo, GETDATE()
	FROM inserted;

## Table auditing
CREATE TRIGGER OrdersAudit
ON Orders
AFTER INSERT, UPDATE, DELETE
AS
	DECLARE @Insert BIT = 0, @Delete BIT = 0;
	IF EXISTS (SELECT * FROM inserted) SET @Insert = 1;
	IF EXISTS (SELECT * FROM deleted) SET @Delete = 1;
	INSERT INTO [TablesAudit] ([TableName], [EventType], [UserAccount], [EventDate])
	SELECT 'Orders' AS [TableName],
	CASE WHEN @Insert = 1 AND @Delete = 0 THEN 'INSERT'
		 WHEN @Insert = 1 AND @Delete = 1 THEN 'UPDATE'
		 WHEN @Insert = 0 AND @Delete = 1 THEN 'DELETE'
		 END AS [Event]
	,ORIGINAL_LOGIN()
	,GETDATE();

## Notifying users
CREATE TRIGGER NewOrderNotification
ON Orders
AFTER INSERT
AS
	EXECUTE SendNotification 
		@RecipientEmail = 'sales@freshfruit.com',
		@EmailSubject = 'New order placed',
		@EmailBody = 'A new order was just placed.';

## Triggers that prevent changes
CREATE TRIGGER PreventProductChanges
ON Products
INSTEAD OF UPDATE
AS
	RAISERROR ('Updates of products are not permitted. Contact the database administrator if a change is needed.', 
		16, 1);

## with conditional statements
#-- Create a new trigger to confirm stock before ordering
CREATE TRIGGER ConfirmStock
ON Orders
INSTEAD OF INSERT
AS
	IF EXISTS (SELECT *
			   FROM Products AS p
			   INNER JOIN inserted AS i ON i.Product = p.Product
			   WHERE p.Quantity < i.Quantity)
	BEGIN
		RAISERROR ('You cannot place orders when there is no stock for the order''s product.', 16, 1);
	END
	ELSE
	BEGIN
		INSERT INTO Orders (OrderID, Customer, Product, Price, Currency, Quantity, WithDiscount, Discount, OrderDate, TotalAmount, Dispatched)
		SELECT OrderID, Customer, Product, Price, Currency, Quantity, WithDiscount, Discount, OrderDate, TotalAmount, Dispatched FROM Orders;
	END;

### USE CASES FOR DDL TRIGGERS
## Database level
# CREATE_, ALTER_, DROP_ -> TABLE, VIEW, INDEX
# ADD_, DROP_ -> ROLE_MEMBER
# CREATE_, DROP_ -> STATISTICS

## Server Level
# CREATE_, ALTER_, DROP_ -> DATABASE, CREDENTIAL
# GRANT_SERVER, DENY_SERVER, REVOKE_SERVER

## Auditing query history
-- Create a new trigger
CREATE TRIGGER DatabaseAudit
-- Attach the trigger at the database level
ON DATABASE
-- Fire the trigger for all tables/ views events
FOR DDL_TABLE_VIEW_EVENTS
AS
	-- Add details to the specified table
	INSERT INTO DatabaseAudit (EventType, DatabaseName, SchemaName, Object, ObjectType, UserAccount, Query, EventTime)
	SELECT EVENTDATA().value('(/EVENT_INSTANCE/EventType)[1]', 'NVARCHAR(50)') AS EventType
		  ,EVENTDATA().value('(/EVENT_INSTANCE/DatabaseName)[1]', 'NVARCHAR(50)') AS DatabaseName
		  ,EVENTDATA().value('(/EVENT_INSTANCE/SchemaName)[1]', 'NVARCHAR(50)') AS SchemaName
		  ,EVENTDATA().value('(/EVENT_INSTANCE/ObjectName)[1]', 'NVARCHAR(100)') AS Object
		  ,EVENTDATA().value('(/EVENT_INSTANCE/ObjectType)[1]', 'NVARCHAR(50)') AS ObjectType
		  ,EVENTDATA().value('(/EVENT_INSTANCE/LoginName)[1]', 'NVARCHAR(100)') AS UserAccount
		  ,EVENTDATA().value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]', 'NVARCHAR(MAX)') AS Query
		  ,EVENTDATA().value('(/EVENT_INSTANCE/PostTime)[1]', 'DATETIME') AS EventTime;

## other
-- Create a trigger to prevent database deletion
CREATE TRIGGER PreventDatabaseDelete
-- Attach the trigger at the server level
ON ALL SERVER
FOR DROP_DATABASE
AS
   PRINT 'You are not allowed to remove existing databases.';
   ROLLBACK;

 ### DELETING AND ALTERING TRIGGERS
 ## Deleting
 DROP TRIGGER PreventNewDiscounts;

 # if on database
 DROP TRIGGER PreventViewsModifications
 ON DATABASE;

 # if on server
 DROP TRIGGER DisallowLinkedServers
 ON ALL SERVERS;

 ## Disabled
 # if on table
 DISABLE TRIGGER PreventNewDiscounts
 ON Discounts;

 ## Renable
 ENABLE TRIGGER PreventNewDiscounts
 ON Discounts;