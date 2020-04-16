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

## CLASSES AND INHERITANCE ######################
## Inheritance in Python
from .parent_class import parent_class

class ChildClass(ParentClass):
	def __init__(self):
		# Call parent's init method
		ParentClass.__init__(self) 

##
class SocialMedia(Document):
    def __init__(self, text):
        Document.__init__(self, text)
        self.hashtag_counts = self._count_hashtags()
        self.mention_counts = self._count_mentions()

    def _count_hashtags(self):
        # Filter attribute so only words starting with '#' remain
        return filter_word_counts(self.word_counts, first_char='#')      

    def _count_mentions(self):
        # Filter attribute so only words starting with '@' remain
        return filter_word_counts(self.word_counts, first_char='@')

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

#### EFFICIENT CODE ###########################
## MODULAR CODE
## MINIMAL MEMORY OVERHEAD, MINIMAL EXECUTION TIME
# Create a range object that goes from 0 to 5
nums = range(0, 6)
print(type(nums))

# Convert nums to a list
nums_list = list(nums)
print(nums_list)

# Create a new list of odd numbers from 1 to 11 by unpacking a range object
nums_list2 = [*range(1,12, 2)]
print(nums_list2)

## ENUMERATE
# Rewrite the for loop to use enumerate
indexed_names = []
for i,name in enumerate(names):
    index_name = (i,name)
    indexed_names.append(index_name) 
print(indexed_names)

# Rewrite the above for loop using list comprehension
indexed_names_comp = [(i,name) for i,name in enumerate(names)]
print(indexed_names_comp)

# Unpack an enumerate object with a starting index of one
indexed_names_unpack = [*enumerate(names, start = 1)]
print(indexed_names_unpack)

## NUMPY arrays, fast and memory-lite alternative to lists
## homogenous
import numpy as np

num = np.array([1, 2, 3])
num.dtype

## regular lists don't support broadcasting
nums ** 2 #doesn't work

## numpy does: vectorized operations

## SYNTAX list
nums2 = ([1, 2, 3],
		 [4, 5, 6])

nums2[0][1] #turns 2
[row[0] for row in nums2] #returns first col

nums2_np = np.array(nums2)
nums2_np[0, 1] #returns 2
nums2_np[:, 0] #returns first col

## ALSO ALLOWS FOR BOOLEAN INDEXING

### RUNTIME #################
#using %timeit (magic commands)

import numpy as np
rand_nums = np.random.rand(1000)
%timeit rand_nums = np.random.rand(1000)
#returns mean & std dev of time

## can set number of runs with -r, and loops with -n
%timeit -r2 -n10 rand_nums = np.random.rand(1000)

#can run on multiple lines of code, if %%timeit is used
#output can be saved with -o

f_time = %timeit -o formal_dict = dict()
l_time = %timeit -o literal_dict = {}

#timing unpacking range vs list comprehension
%timeit [*range(50)]
%timeit [n for n in range(50)]

## LINE PROFILING
# detailed stats on frequency and duration of fct calls
# line-by-line analyses
# package: line_profiler, pip install line_profiler

%load_ext line_profiler
%lprun -f convert_units convert_units(a, b, c) 
#line-by-life, -f says want to profile function

# reports different stats from timeit

## CODE PROFILING: memory
#memory_profiler, mprun -> function must be imported in a file
# place function in sample_funcs.py then
from sample_funcs import convert_units
%load_ext memory_profiler
%mprun -f convert_units convert_units(a, b, c)
# mem usage shows new memory usage
# increment shows impact on total memory used
# need to use enough memory otherwise doesn't show up
# %mprun inspects memory by querying the os -> results may differ based on run


## COMBINING OBJECTS
names = ['a', 'b', 'c']
hps = [1, 2, 3]

combined_ = zip(names, hps)
print(combined_) #tuple of elements from OG list

# Counter object
## can count with a loop or Counter from collections (faster)
from collections import Counter
type_counts = Counter(poke_types)
print(type_counts)

# itertools -> product, permutations, combinations (also faster)
from itertools import combinations
combos_obj = combinations(poke_types, 2) #2 is length of combinations
combos = [*combos_obj]
print(combos)

## SET THEORY
# sets, intersection(), difference(), symmetric_difference(), unions()
# membership testing is faster -> 'a' in set(['a', 'b', 'c'])
# also note that set(['a', 'a', 'b']) only returns unique objects

## REPLACING FOR LOOPS
# For Loop
gen1_gen2_name_lengths_loop = []

for name,gen in zip(poke_names, poke_gens):
    if gen < 3:
        name_length = len(name)
        poke_tuple = (name, name_length)
        gen1_gen2_name_lengths_loop.append(poke_tuple)

# Not a for loop
[(name, len(name)) for name,gen in zip(poke_names, poke_gens) if gen < 3]

## WRITING BETTER LOOPS
#for ix, row in dataframe.iterrows()
# itertuples -> can access attributes with ., NOT []
# itertuples is faster bc iterrows return pandas series
#using apply instead, index 0 for columns, 1 for rows
baseball_df.apply(lambda x: calc_run_diff(x['RS'], x['RD']),
	axis = 1)

baseball_df['W'].values #is a numpy array
# which means they can be broadcast / vectorized
run_diffs_np = baseball_df['RS'].values - baseball_df['RD'].values