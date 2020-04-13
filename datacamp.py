connection_uri = "postgresql://repl:password@localhost:5432/dwh"
db_engine = sqlalchemy.create_engine(connection_uri)

## Loading
def load_to_dwh(recommendations):
    recommendations.to_sql("recommendations", db_engine, if_exists= "replace")


# Setting Up Daily Job
dag = DAG(dag_id="recommendations",
          schedule_interval="0 0 * * *")

task_recommendations = PythonOperator(
    task_id="recommendations_task",
    python_callable=etl,
    op_kwargs={"db_engines":db_engines},
)

# Finishing Up
def recommendations_for_user(user_id, threshold=4.5):
  # Join with the courses table
  query = """
  SELECT title, rating FROM recommendations
    INNER JOIN courses ON courses.course_id = recommendations.course_id
    WHERE user_id=%(user_id)s AND rating>%(threshold)s
    ORDER BY rating DESC
  """
  # Add the threshold parameter
  predictions_df = pd.read_sql(query, db_engine, params = {"user_id": user_id, 
                                                           "threshold": threshold})
  return predictions_df.title.values

# Try the function you created
print(recommendations_for_user(12, 4.65))

## READING IN CSV FILES #####################
# Import pandas as pd
import pandas as pd

# Read the CSV and assign it to the variable data
data = pd.read_csv('vt_tax_data_2016.csv')

# View the first few lines of data
print(data.head())

## Same With Tabs
data = pd.read_csv("vt_tax_data_2016.tsv", sep = '\t')

# Plot the total number of tax returns by income group
counts = data.groupby("agi_stub").N1.sum()
counts.plot.bar()
plt.show()

## Limit Columns
# can use `usecols` to only select certain columns or a function to filter columns
## Limit Rows
# with nrows -- careful, may need to use header = None
## Add Column Names
# with names, a list of column names

## Changing DataTypes 
# using dtype, dictionary of column names and datatypes
tax_data = pd.read_csv('url.csv', 
							dtype = {'zipcode': str},
							na_values = {'zicode': 0}
)

## Customizing Missing Values
# use na_values, with single value, list or dictionary

## Corrupt files?
# using error_bad_lines will let you import the file
tax_data = pd.read_csv('url.csv', 
							dtype = {'zipcode': str},
							na_values = {'zicode': 0},
							error_bad_lines = False,
							warn_bad_lines = True
)

### SPREADSHEETS ####
# use read_excel, same nrows, skiprows, usecols
# Create string of lettered columns to load
col_string = 'AD, AW:BA' ## as a string

# Load data with skiprows and usecols set
survey_responses = pd.read_excel("fcc_survey_headers.xlsx", 
                        skiprows = 2, 
                        usecols = col_string)

# View the names of the columns selected
print(survey_responses.columns)

## Selecting Sheets to Load
#use sheet_name to load other sheets
#specify sheets by name or position number
## note: if number, the name of the resulting DF is the index number
## ANY ARGS PASSED TO READ_EXCEL apply to all sheets

## SHEET NAME = None gives *all* sheets in a dict 
all_responses = pd.DataFrame()

for sheet_name, frame in survey_responses.items():
	frame['Year'] = sheet_name #add a col for year
	all_responses = all_responses.append(frame)

## DEALING WITH DATATIME
pd.read_excel('url.xlsx', read_dates = 'datecol_name')

#if non standard
data['datecol_name'] = pd.to_datetime(data['datecol_name'], 
	format = '%m%d%y')

## GETTING DATA FROM DATABASE ################
from sqlalchemy import create_engine
engine = create_engine('sqlite:///data.db')
query = 'select * from table'
data = pd.read_sql(query, engine)

# Create query to get temperature and precipitation by month
query = """
SELECT month, 
        MAX(tmax), 
        MIN(tmin),
        sum(prcp)
  FROM weather 
 GROUP BY month;
"""

# Get data frame of monthly weather stats
weather_by_month = pd.read_sql(query, engine)

# View weather stats by month
print(weather_by_month)

## MORE COMPLEX
# Query to get water leak calls and daily precipitation
query = """
SELECT hpd311calls.*, weather.prcp
  FROM hpd311calls
  JOIN weather
    ON hpd311calls.created_date = weather.date
  where hpd311calls.complaint_type = 'WATER LEAK';"""

 # Modify query to join tmax and tmin from weather by date
query = """
SELECT hpd311calls.created_date, 
	   COUNT(*), 
       weather.tmax,
       weather.tmin
  FROM hpd311calls 
       JOIN weather
       on hpd311calls.created_date = weather.date
 WHERE hpd311calls.complaint_type = 'HEAT/HOT WATER' 
 GROUP BY hpd311calls.created_date;
 """

# Load query results into the leak_calls data frame
leak_calls = pd.read_sql(query, engine)

# View the data frame
print(leak_calls.head())

### API CALLS & JSON DATA ######################
# Load json_normalize()
from pandas.io.json import json_normalize

# Isolate the JSON data from the API response
data = response.json()

# Flatten business data into a data frame, replace separator
cafes = json_normalize(data["businesses"],
             sep = '_')

# View data
print(cafes.head())

#Notice that by accessing data['businesses'] we're already 
#working one level down the nested structure. data itself 
#could be flattened with json_normalize().

# Load other business attributes and set meta prefix
flat_cafes = json_normalize(data["businesses"],
                            sep="_",
                    		record_path="categories",
                    		meta=['name', 
                                  'alias',  
                                  'rating',
                          		  ['coordinates', 'latitude'], 
                          		  ['coordinates', 'longitude']],
                    		meta_prefix='biz_')





# View the data
print(flat_cafes.head())

#### SOFTWARE ENGINEERING ###########################
## MODULAR CODE
