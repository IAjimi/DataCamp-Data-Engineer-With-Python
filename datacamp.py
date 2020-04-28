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


### INTRODUCTION TO SHELL ######################################################
# pwd : working directory
# ls : lists files in directory
## ls -R : works recursively, shows every file and directory in the current level, then everything in each sub-directory, and so on.
## ls -F:  prints a / after the name of every directory and a * after the name of every runnable program
# /home/repl : absolute path (starts with /), repl : relative path
# cd: change directory
# ..: go backwards (from home/repl/seasonal to home/repl) 
# .: current directory (no difference between ls and ls .)
# ~ : home directory
# cp: copy (cp original.txt duplicate.txt creates copy of original called duplicate)
## if last parameter of cp is a directory, copies all files to directory

cp seasonal/summer.csv seasonal/summerbackup.bck #copies file 1 as file 2 in path in file 2 name
cp seasonal/summer.csv seasonal/spring.csv backup #copies file 1 and 2 into backup dir

# mv works the same way for moving files -- can also be used to rename FILES
# rm removes files, can add as many files as needed -- CAREFUL: deletes FOR GOOD

# for directories, mv works the same way: mv seasonal by-season remanes dir seasonal to by-season
# not rm! need to use rmdir & ONLY works if the directory is empty
# mkdir creates new directory

## MANIPULATING DATA
# cat: shows the content of files in terminal
# less: shows the content of files by PAGE -> :n to go to next file, :p to go to previus, :q to quit
less seasonal/spring.csv seasonal/summer.csv #shows both files in that order
# head: shows first lines
head -n 3 seasonal/summer.csv #only shows first 3 lines

# man: shows documentation
man head #shows info for head

# cut: select columns from file
cut -f 2-5,8 -d , values.csv #select columns 2 through 5 and columns 8, using comma as the separator
#-f means fields (= columns), -d is delimiter

## NOTE: cut doesn't understand quoted strings

# history: prints history of past commands
## !55: re-runs 55th comman in history
## !head: re-runs most recent used of head

# grep: selects lines based on content
## -v: lines that dont contain match
## -n: show line number
## -c: count of matching lines
grep molar seasonal/autumn.csv #all occurences of molar
grep -v -n molar seasonal/autumn.csv #all lines w/o molar, with line number

# wc: word count
## -c: CHARACTER count
## -w: WORD count
## -l: LINE count
cut -d , -f 2 seasonal/summer.csv | grep -v Tooth

# using REDRECTION to save output in a file
head -n 5 seasonal/summer.csv > top.csv

# pipe:
head -n 5 seasonal/summer.csv | tail -n 3
# instead of
head -n 5 seasonal/summer.csv > top.csv
tail -n 3 top.csv

## this can be extended to MULTIPLE files
cut -d , -f 1 seasonal/winter.csv seasonal/spring.csv seasonal/summer.csv seasonal/autumn.csv
# alternative is using a wildcard
## *: all
cut -d , -f 1 seasonal/* #all files in seasonal
cut -d , -f 1 seasonal/*.csv #all csv files in seasaonl

## ?: matches a single character, 201?.txt matches 2010, 2011, etc
## [...]: matches any character within brackets -> 201[78] matches 2017 and 2018
## {..., ...}: matches any of the patterns within {} -> {*.csv, *.txt} bring back all .csv and .txt files

# sort: order data, by default in ascending alphabetical order
## -n: sort numerically
## -r: reverse order of output
## -b: ignore leading blanks
## -f: be case-insensitive
cut -d , -f 2 seasonal/winter.csv | grep -v Tooth | sort | uniq -c # returns count of occurences of different names
wc -l seasonal/* | grep -v total | sort -n | head -n 1 #returns the file w/ fewest lines

## can play around with order
> output.txt head -n 3 seasonal/winter.csv #saves output of head etc into output

## CTRL + C :stop running program

### ENVIROMMENT VARIABLES
## HOME: user home directory
## PWD: present working directory
## SHELL: which shell is being used (/bin/bash)
## USER: user id
# to get full list, type set in shell

# echo: prints out value of variable
echo USER #prints USER
echo $USER #print value of USER

### SHELL VARIABLE
# basically a local variable

## Setting value
training=seasonal/summer.csv #NO spaces before after =

### LOOPS
## STRUCTURE
for f in ...; do ... f; done

## EXAMPLE
for filetype in gif jpg png; do echo $filetype; done
#returns gif, jpg, png

for filename in seasonal/*.csv; do echo $filename; done

## Can record the names of a set of files
datasets=seasonal/*.csv
for filename in $datasets; do echo $filename; done

## CAREFUL!
files=seasonal/*.csv
for f in files; do echo $f; done #because files, not $files, only prints word files

## MORE COMPLEX EX
for file in seasonal/*.csv; do head -n 2 $file | tail -n 1; done #prints 2nd line of each file

## NOTE: if spaces in file name, need to add quotes
mv 'July 2017.csv' '2017 July data.csv'

## DOING SEVERAL THINGS IN ONE LOOP
#a loop can contain any number of commands, sep by ;
for f in seasonal/*.csv; do echo $f; head -n 2 $f | tail -n 1; done

### SHELL SCRIPT
# convention is to end with .sh to save bash commands
# $@: all of the command-line parameters given to the script

# EXAMPLE
# if unique-lines.sh contains sort $@ | uniq, the line below 
bash unique-lines.sh seasonal/summer.csv #processes one file, seasonal/summer.csv

# alternative: $1 $2, which refer to 1st and 2nd command parameters
cut -d , -f $2 $1
bash column.sh seasonal/autumn.csv 1 
# equivalent to cut -d , -f seasonal/autumn.csv 1

## LOOPS in shell scripts
# Print the first and last data records of each file.
for filename in $@
do
    head -n 2 $filename | tail -n 1
    tail -n 1 $filename
done

### INTRODUCTION TO BASH SCRIPTING ######################################################
### REGEX REMINDERS
grep 'p' fruits.txt
# apple

grep [pc] fruits.txt
# apple, carrot

#!/usr.bash

# SED is the equivalent of REPLACE
cat soccer_scores.csv | sed 's/Cherno/Cherno City/' | sed 's/Arda/Arda United/' > soccer_scores_edited.csv #replaces Arda w Arda United

# Now save and run!

sort | uniq -c #sort then unique -> important bc unique only checks for adjacent rows!

### BASH SCRIPT ANATOMY
# starts with #!/usr/bash

### ARGUMENTS
$@ #all arguments
$# # number of arguments
$1 #first argument

## EXAMPLE SCRIPT
echo $1 

cat hire_data/* | grep "$1" > "$1".csv #$1 in quotes still recognizes $1, casts as string?


## CAREFUL
# single quotes: literal interpretation
# double quotes: literal EXCEPT for $ and backticks ``
# backticks: "a shell within a shell"

# EXAMPLE
rightnow="the date is `date`"
echo $rightnow
# prints out: the date is April 21

#same for
rightnow="the date is $(date)"

### NUMERIC VARIABLES IN BASH
# no 1 + 1 math in BASH, need to use expr
expr 1 + 1
# but does not handle decimals

echo "5 + 7.5" | bc #bc is like expr but does handle decimals
echo "scale=3; 10 / 3" | bc #use scale to specify number of decimals (; separates lines)

## EX
model1=87.65
model2=89.20
echo "the total score is $(echo "$model1 + $model2" | bc)"
echo "the average score is $(echo "$model1 + $model2) / 2" | bc)"

## OTHER EX
# Get first ARGV into variable
temp_f=$1

# Subtract 32
temp_f2=$(echo "scale=2; $temp_f - 32" | bc)

# Multiply by 5/9 and print
temp_c=$(echo "scale=2; $temp_f2 * 5 / 9" | bc)

# Print the temp
echo $temp_c

### BASH ARRAY
# 1. Declare w/o adding elements
declare -a my_first_array

# 2. Create and add elements at the same time
my_first_array=(1 2 3) #NO COMMAS

# array[@]: returns all elements 
# #array[@]: returns length of array
echo ${my_array[@]}
echo ${#my_array[@]}

# can subset with [], 1st element is at position 0
echo ${my_first_array[2]}

# can change array elements with indexing
my_first_array[0]=999

#array[@]:N:M slices subset of array, N is starting index, M is number of elements to return
echo ${my_first_array[@]:3:2}

#append
my_array+-(elements)

#without parenthesis, element gets ADDED to first element of array

## ASSOCIATIVE ARRAYS (Bash 4 onwards)
# like regular array but with key value pairs (dictionary)
# NEED declare syntax
declare -A city_details
city_details=([city_name]="New York" [population]=14000000)
echo ${city_details[city_name]} #use key to index

# in one line
declare -A city_details=([city_name]="New York" [population]=14000000)

# return all keys
echo ${!city_details[@]}

## EXAMPLE
# Create empty associative array
declare -A model_metrics

# Add the key-value pairs
model_metrics[model_accuracy]=98
model_metrics[model_name]='knn'
model_metrics[model_f1]=0.82

## ANOTHER EXAMPLE
# Create variables from the temperature data files
temp_b="$(cat temps/region_B)"
temp_c="$(cat temps/region_C)"

# Create an array with these variables as elements
region_temps=($temp_b $temp_c)

# Call an external program to get average temperature
average_temp=$(echo "scale=2; (${region_temps[0]} + ${region_temps[1]}) / 2" | bc)

# Append to array
region_temps+=($average_temp)

# Print out the whole array
echo ${region_temps[@]}


### IF STATEMENTS
if [ CONDITION ] ; then 
	# SOME CODE
else
	# SOME OTHER CODE
fi 


## EXAMPLE
x="Queen"
if [ $x == "King" ]; then
	echo "$x is a King!"
else
	echo "$x is not a King!"
fi

## arithmetic if statements need (()) structure
x=10
if (($x > 5)); then
	echo "$x is more than 5!"
fi

# other flags
# -eq: equal to, -ne: not equal to
# -lt: less than, -le: less than or equal to
# -gt: greather than, -ge: greater than or equal to
# -e: if file exists, -s: if file exists and has size > 0
# -r: if the file exists and is readable, -w: if it exists and is writable

## OTHER EXAMPLES
# can use command line as condition, without brackets
if grep -q 'Hello' words.txt; then
	echo "Hello is inside!"
fi

## ALTERNATIVE
if $(grep -q 'Hello' words.txt); then
	echo "Hello is inside!"
fi

## ANOTHER
# Extract Accuracy from first ARGV element
accuracy=$(grep Accuracy $1 | sed 's/.* //')

# Conditionally move into good_models folder
if [ $accuracy -ge 90 ]; then
    mv $1 good_models/
fi

# Conditionally move into bad_models folder
if [ $accuracy -le 90 ]; then
    mv $1 bad_models/
fi

### FOR AND WHILE LOOPS IN BASH
## FOR LOOPS
for x in 1 2 3
do 
	echo $x
done

# BRACE EXPRESSION
{START..STOP..INCREMENT} #-> for x in {1..5..2}

# ALT. EXPRESSION
for ((x=2;x<=4;x+=2)) #start at x=2, finish at x<=4, increment by x+=2

# GLOB Expansions: pattern-matching inside loops
for book in books/*
do 
	echo $book
done

# SHELL-WITHIN-A-SHELL within for loop
for book in $(ls books/ | grep -i 'air')
do
	echo $book
done

## WHILE LOOP
x=1

while [ $x -le 3];
do
	echo $x
	((x+=1))
done

### CASE STATEMENTS
CASE 'STRINGVAR' in
	PATTERN1)
	COMMAND;;
	PATTERN2)
	COMMAND2;;
	*)
	DEFAULT COMMAND;;
esac

## EXAMPLE
# Create a CASE statement matching the first ARGV element
case $1 in
  # Match on all weekdays
  Monday|Tuesday|Wednesday|Thursday|Friday)
  echo "It is a Weekday!";;
  # Match on all weekend days
  Saturday|Sunday)
  echo "It is a Weekend!";;
  # Create a default
  *) 
  echo "Not a day!";;
esac

## ANOTHER ONE
# Use a FOR loop for each file in 'model_out/'
for file in model_out/*
do
    # Create a CASE statement for each file's contents
    case $(cat $file) in
      # Match on tree and non-tree models
      *"Random Forest"*|*GBM*|*XGBoost*)
      mv $file tree_models/ ;;
      *KNN*|*Logistic*)
      rm $file ;;
      # Create a default
      *) 
      echo "Unknown model in $file" ;;
    esac
done

### BASIC FUNCTIONS IN BASH
function_name () {
	#function code
	return #something
}

# ALTERNATIVE
function function_name {
	#function code
	return #something
}

## EXAMPLE
# Create function
function upload_to_cloud () {
  # Loop through files with glob expansion
  for file in output_dir/*results*
  do
    # Echo that they are being uploaded
    echo "Uploading $file to cloud"
  done
}

# Call the function
upload_to_cloud

## PASSING ARGUMENTS
function print_filename {
	echo "The first file was $1"

	for file in $@
	do
		echo "This file has name $file"
	done
}

print_filename "LOTR.txt" "mod.txt" "A.py"

## NOTE: ALL variables are GLOBAL by default in bash
# the local keyword can be used to limit scope, as shown below
function print_filename {
	local first_filename=$1
}

## RETURN VALUES
# return is only meant to determine if the function was a success (0) or failure
# it is captured in the global variable $?

# either
# 1. assign to a global variable
# 2. echo what you want back in the last line and capture with shell-within-a-shell

## EXAMPLE
# Create a function 
function return_percentage () {

  # Calculate the percentage using bc
  percent=$(echo "scale=4; $1 / $2" | bc)

  # Return the calculated percentage
  echo $percent
}

# Call the function with 456 and 632 and echo the result
return_test=$(return_percentage 456 632)
echo "456 out of 632 as a percent is $return_test"

## OTHER EXAMPLE
function get_number_wins () {

  # Filter aggregate results by argument
  win_stats=$(cat soccer_scores.csv | cut -d "," -f2 | egrep -v 'Winner'| sort | uniq -c | egrep "$1")

}

# Call the function with specified argument
get_number_wins "Etar"

# Print out the global variable
echo "The aggregated stats are: $win_stats"

## EXAMPLE
# Create a function with a local base variable
function sum_array () {
  local sum=0
  # Loop through, adding to base variable
  for number in "$@"
  do
    sum=$(echo "$sum + $number" | bc)
  done
  # Echo back the result
  echo $sum
  }
# Call function with array
test_array=(14 12 23.5 16 19.34)
total=$(sum_array "${test_array[@]}")
echo "The sum of the test array is $total"

### SCHEDULE SCRIPTS
# driven by crontab, a file which contains cronjobs, which tells crontab what code to run
# and when

crontab -l # see what schedules are running
5 1 * * * bash myscript.sh #run every day at 1:05 am
15,30,45 * * * * #will run every 15th, 30th, 45th on whatever hour specified by 2nd star
*/15 * * * * #runs every 15 minutes

## ADDING A CRONJOB
crontab -e #edit list of cronjobs
30 1 * * * extract_data.sh #create cronjob
#exit editor to save it

crontab -l #check it is there

### DATA PROCESSING IN SHELL ######################################################
#curl: client for Urls

# check installation
man curl

# basic syntax
curl [option flags] [URL]

# -O: save the file with its original name
curl -O https://websitename.com/datafilename.txt

# -o: save and rename
curl -o renamedfile.txt https://websitename.com/datafilename.txt

# -L: allows saving from redirected url

## EXAMPLE: Download and rename the file in the same step
curl -o Spotify201812.zip -L https://assets.datacamp.com/production/repositories/4180/datasets/eb1d6a36fa3039e4e00064797e1a1600d267b135/201812SpotifyData.zip

## EXAMPLE: Download 100 files w/ sequential names
# Download all 100 data files
curl -O https://s3.amazonaws.com/assets.datacamp.com/production/repositories/4180/datasets/files/datafile[001-100].txt

# Print all downloaded files to directory
ls datafile*.

### DOWNLOAD DATA USING WGET
# better than curl at downloading multiple files recursively

# check installation with
which wget

# Option flags
# -b: download in background
# -q: turn off output
# -c: resume broken download
# can link them: 
wget -bqc https://websitename.com/datafilename.txt
# will return: continuing in background, pid 12345

## EXAMPLE
    # Fill in the two option flags 
wget -c -b https://assets.datacamp.com/production/repositories/4180/datasets/eb1d6a36fa3039e4e00064797e1a1600d267b135/201812SpotifyData.zip

# Verify that the Spotify file has been downloaded
ls 

# Preview the log file 
cat wget-log

## MULTIPLE FILE DOWNLOADS
# save a list of file locations in a text file
cat url_list.txt

# can download from them with -i (internal file)
wget -i url_list.txt 

## NOTE: NO option flags added AFTER -i

# set download bandwith limit
wget --limit-rate={rate}k {file_location}

# set mandatory pause time between file downlaods (in seconds)
wget --wait={seconds} {file_location}


## EXAMPLE: curl v. wget
# Use curl, download and rename a single file from URL
curl -o Spotify201812.zip -L https://assets.datacamp.com/production/repositories/4180/datasets/eb1d6a36fa3039e4e00064797e1a1600d267b135/201812SpotifyData.zip

# Unzip, delete, then re-name to Spotify201812.csv
unzip Spotify201812.zip && rm Spotify201812.zip
mv 201812SpotifyData.csv Spotify201812.csv

# View url_list.txt to verify content
cat url_list.txt

# Use Wget, limit the download rate to 2500 KB/s, download all files in url_list.txt
wget --limit-rate=2500k -i url_list.txt

# Take a look at all files downloaded
ls

### csvkit (written in python>)
# INSTALL
pip install csvkit
pip install --upgrade csvkit

## in2csv: convert files to csv
in2csv --help
in2csv -h

# converting files
in2csv SpotifyData.xlsx > SpotifyData.csv

# specify sheet
in2csv -n SpotifyData.xslx #prints out the names of all available sheets
in2csv SpotifyData.xlsx --sheet "Worksheet1_Popularity" > Spotify_Popularity.csv 

## csvlook: preview data 
csvlook Spotify_Popularity.csv 

## csvstat: print descriptive summary stats
csvstat Spotify_Popularity.csv


## EXAMPLE
# Use ls to find the name of the zipped file
ls

# Use Linux's built in unzip tool to unpack the zipped file 
unzip SpotifyData.zip

# Check to confirm name and location of unzipped file
ls

# Convert SpotifyData.xlsx to csv
in2csv SpotifyData.xlsx > SpotifyData.csv

# Print a preview in console using a csvkit suite command 
csvlook SpotifyData.csv 

## FILTERING DATA 
## csvcut: by column by col name or position
# --names or -n : print all column names
csvcut -n Spotify_Popularity.csv

# return 1st column
csvcut -c 1 Spotify_Popularity.csv
csvcut -c "track_id" Spotify_Popularity.csv

# return multiple columns
csvcut -c 1,2 Spotify_Popularity.csv
csvcut -c "track_id","duration_ms" Spotify_Popularity.csv

## csvgrep: filter by row with exact match or regex
# must be paired with
# -m: exact row value
# -r: regex pattern
# -f: path to file

# Find track_id = QW12
csvgrep -c "track_id" -m QW12 Spotify_Popularity.csv
csvgrep -c 1 -m QW12 Spotify_Popularity.csv

### STACKING DATA AND CHAINING COMMANDS
## csvstack: stacking multiple CSVs
# need to check same columns, in same order, with same types!
csvstack Spotify_Rank6.csv Spotify_Rank7.csv > Spotify_AllRanks.csv

# adding flag to keep track of source 
csvstack -g "Rank6","Rank7" \ Spotify_Rank6.csv Spotify_Rank7.csv > Spotify_AllRanks.csv

## chaining command-line commands
# ; links commands together and runs sequentially
csvlook SpotifyData_All.csv; csvstat SpotifyData_All.csv

# && links commands together, but only runs the 2nd command if the 1st succeeds
csvlook SpotifyData_All.csv && csvstat SpotifyData_All.csv

## EXAMPLE
# Take top 15 rows from sorted output and save to new file
csvsort -c 2 Spotify_Popularity.csv | head -n 15 > Spotify_Popularity_Top15.csv

# Preview the new file 
csvlook Spotify_Popularity_Top15.csv

## ANOTHER EXAMPLE
# Convert the Spotify201809 tab into its own csv file 
in2csv Spotify_201809_201810.xlsx --sheet "Spotify201809" > Spotify201809.csv

# Check to confirm name and location of data file
ls

# Preview file preview using a csvkit function
csvlook Spotify201809.csv

# Create a new csv with 2 columns: track_id and popularity
csvcut -c "track_id","popularity" Spotify201809.csv > Spotify201809_subset.csv

## ANOTHER OTHER EXAMPLE
# Convert the Spotify201809 tab into its own csv file 
in2csv Spotify_201809_201810.xlsx --sheet "Spotify201809" > Spotify201809.csv

# Check to confirm name and location of data file
ls

# Preview file preview using a csvkit function
csvlook Spotify201809.csv

# Create a new csv with 2 columns: track_id and popularity
csvcut -c "track_id","popularity" Spotify201809.csv > Spotify201809_subset.csv

# While stacking the 2 files, create a data source column
csvstack -g "Sep2018","Oct2018" Spotify201809_subset.csv Spotify201810_subset.csv > Spotify_all_rankings.csv


### PULLING DATA FROM DATABASES
## sql2csv: pulls data from SQL database, saves to csv file
sql2csv --db "sqlite:///SpotifyDatabase.db" \
		--query "SELECT * FROM Spotify_Popularity" \
		> Spotify_Popularity.csv

# --db: followed by database connection string, depends on SQL db syntacs
# --query: SQL query string
# need > to save output, or will print to console
# -v: verbose: prints detailed trackback

## APPLYING SQL TO LOCAL CSV FILE
# csvsql applies SQL statements to one or more CSV files
# creates an in-memory SQL database that temporarily hosts the file being processed
## -> small to medium files only!
csvsql --query "SELECT * FROM Spotify_music LIMIT 1" \
		data/Spotify_MusicAttributes.csv | csvlook

# JOINS
csvsql --query "SELECT * FROM file_a INNER JOIN file_b..." file_a.csv file_b.csv

## NOTE: can only write the query in ONE line
# files mentioned need to be added after the query in order of appearance in query

## EXAMPLES
# Preview CSV file
ls

# Store SQL query as shell variable
sqlquery="SELECT * FROM Spotify_MusicAttributes ORDER BY duration_ms LIMIT 1"

# Apply SQL query to Spotify_MusicAttributes.csv
csvsql --query "$sqlquery" Spotify_MusicAttributes.csv

### PUSHING DATA BACK TO DATABASE
# csvsql: also supports creating and inserting data

# push local csv to database
csvsql --db "sqlite:///SpotifyDatabase.db" \
	   --insert Spotify_MusicAttributes.csv

csvsql --no-inference --no-constraints \
	   --db "sqlite:///SpotifyDatabase.db" \
	   --insert Spotify_MusicAttributes.csv


### PYTHON ON THE COMMAND LINE
# see python version installed
python --version

# see location of python
which python

# start session
python

# exit session
exit()

# alternative: save python script as .py, execute with
python script.py

# or create by "echo"ing python syntax & instantiating Python file in the same step
echo "print('hello world')" > hello_world.py

### INSTALL PACKAGES ON COMMAND LINE
# upgrade pip
pip install --upgrade pip

# see python packages in current environment
pip list

# install package
pip install scikit-learn

# install specific version of package
pip install scikit-learn==0.19.2

# install multiple packages
pip install scikit-learn statsmodels

# or use a list of packages from txt file, e.g., requirements.txt
pip install -r requirements.txt

### EXAMPLE
# Add scikit-learn to the requirements.txt file
echo "scikit-learn" > requirements.txt

# Preview file content
cat requirements.txt

# Install the required dependencies
pip install -r requirements.txt

### CRONTAB
## time based job scheduler
# see list of jobs
crontab -l 

# add job
# 1. modify crontab with text editor
# 2. echo the schedluer command into crontab
echo "* * * * * python create_model.py" | crontab


### COMMAND LINE AUTOMATION IN PYTHON ######################################################
### IPYTHON
# !: executes shell commands
!df -h #show free disk

## two ways to execute python code in an interpreter
# 1. pass a script
python hello.py

# 2. pass a python program via -c
python -c "import datetime;print(datetime.datetime.utcnow())"

## EXAMPLE: find number of files in directory with .csv extension
dir_contents = !ls test_dir/*.csv
len(dir_contents)

## Capturing shell output with bash magic function
%%bash --output  #captures output

# EX
%%bash --out output
ls -l | awk '{ SUM+=$5} END {print SUM}'
# returns string


### AUTOMATE WITH SLIST
# fields
ls = !ls -l /usr/bin
ls.fields(1,5)[1:4] #selects columns 1, 5

# grep
ls.grep("kill") #only shows matching patterns

# sort
disk_usage = !df -h
disk_usage.sort(5, nums= True)

# methods
var = ls.pop() #popping works on Slists
ls[-4:] #so does slicing

# can turn into list, set, dict
type(ls) #iphone SList
newls = list(ls)
sls = set(ls)
dls = dict(vals=ls) 


### EXECUTE SHELL COMMANDS IN SUBPROCESS
# subprocess.run: shell commands w Python 3.5+
# takes a list of strings
subprocess.run(["ls", "-l"])
subprocess.CompletedProcess #object
print(out.returncode) #see if successful (returns nothing)

# sucessful unix command returns 0
echo $? #how to check if successful

## control flow
good_user_input = "-l"
out = run(["ls", good_user_input])

if out.returncode == 0:
	print("success")
else:
	print("failure")

## EXAMPLE
import subprocess

# Execute Unix command `head` safely as items in a list
with subprocess.Popen(["head", "poem.txt"], stdout=subprocess.PIPE) as head: #subprocess.PIPE captures output
  
  # Print each line of list returned by `stdout.readlines()`
  for line in head.stdout.readlines():
    print(line)
    
# Execute Unix command `wc -w` safely as items in a list
with subprocess.Popen(["wc", "-w", "poem.txt"], stdout=subprocess.PIPE) as word_count:
  
  # Print the string output of standard out of `wc -w`
  print(word_count.stdout.read())

 ## ANOTHER ONE
 import subprocess

# Use subprocess to run the `ps aux` command that lists running processes
with subprocess.Popen(["ps", "aux"], stdout=subprocess.PIPE) as proc:
    process_output = proc.stdout.readlines()
    
# Look through each line in the output and skip it if it contains "python"
for line in process_output:
    if b'python' in line: #need b to turn 'python' stirng into bytes
        continue
    print(line)

### CAPTURE OUTPUT OF SHELL COMMANDS
# in bash, list a directory with ls
ls

# in python output, use Popen to capture output
with Popen(["ls"], stdout=PIPE) as proc:
	out = proc.readlines()
print(out)

# why with? handles closing files/sockets + waits for statement to finish

# communicate: a way to communicate with process
proc = subprocess.Popen(...)
try:
	out, err = pro.communicate(timeout=30) #tries to communicate for 30 secs
except TimeoutExpired:
	proc.kill() #kill the process
	out, error = pro.communicate() #capture std output and std error


# PIPE: connects a standard stream
## standard input is command, returns standard output and error
# stdout: output, can be used either with stdout.read() (returns as STRING) 
# or stdout.readlines() (returns output as ITERATOR)
# shell=FALSE: is default and recommended (shell=True is unsafe)

### EXAMPLE
from subprocess import Popen, PIPE
import json
import pprint

# Use the with context manager to run subprocess.Popen()
with Popen(["pip","list","--format=json"], stdout=PIPE) as proc:
  result = proc.stdout.read()
  
# Convert the JSON payload to a Python dictionary
converted_result = json.loads(result)

# Display the result in the IPython terminal
pprint.pprint(converted_result)

### ANOTHER EXAMPLE
# Start a long running process using subprocess.Popen()
proc = Popen(["sleep", "6"], stdout=PIPE, stderr=PIPE)

# Use subprocess.communicate() to create a timeout 
try:
    output, error = proc.communicate(timeout=5)
    
except TimeoutExpired:

	# Cleanup the process if it takes longer than the timeout
    proc.kill()
    
    # Read standard out and standard error streams and print
    output, error = proc.communicate()
    print(f"Process timed out with output: {output}, error: {error}")


### SENDING INPUT TO PROCESSES
# 2 methods: 
# 1. Popen
proc1 = Popen(["process_one.sh"], stdout=subprocess.PIPE)
Popen(["process_two.sh"], stdin=proc1.stdout)

# 2. run
proc1 = run(["process_one.sh"], stdout=subprocess.PIPE)
run(["process_two.sh"], input=proc1.stdout)

### EXAMPLE
import subprocess

# runs find command to search for files
find = subprocess.Popen(
["find", ".", "-type", "f", "-print"], stdout=subprocess.PIPE)

# runs wc and counts the number of lines
word_count = subprocess.Popen(
["wc", "-l"], stdin=find.stdout, stdout=subprocess.PIPE)

# print the decoded and formatted output
output = word_count.stdout.read()
print(output.decode('utf-8').strip())

### PASSING ARGUMENTS SAFELY TO SHELL COMMANDS
# by default shell=False -> arguments must be passed as list
# shlex: used to sanitize strings (parse UNIX string safely)

## EXAMPLE
danger_string = '/tmp && rm -f /all/my/dirs' #very dangerous command that could be passed as input
shlex.split(danger_string)
directory = shlex.split('/tmp')
cmd = ['ls']
cmd.extend(directory)
run(cmd, shell=True)

## BEST PRACTICE: default to items in a list
import subprocess

#Accepts user input
print("Enter a path to search for directories: \n")
user_input = "."
print(f"directory to process: {user_input}")

#Pass safe user input into subprocess
with subprocess.Popen(["find", user_input, "-type", "d"], stdout=subprocess.PIPE) as find:
    result = find.stdout.readlines()
    
    #Process each line and decode it and strip it
    for line in result:
        formatted_line = line.decode("utf-8").strip()
        print(f"Found Directory: {formatted_line}")

## ANOTHER EXAMPLE
print("Enter a list of directories to calculate storage total: \n")
user_input = "pluto mars jupiter"

# Sanitize the user input
sanitized_user_input = shlex.split(user_input)
print(f"raw_user_input: {user_input} |  sanitized_user_input: {sanitized_user_input}")

# Safely Extend the command with sanitized input
cmd = ["du", "-sh", "--total"]
cmd.extend(sanitized_user_input)
print(f"cmd: {cmd}")

# Print the totals out
disk_total = subprocess.run(cmd, stdout=subprocess.PIPE)
print(disk_total.stdout.decode("utf-8"))


### DEALING WITH FILE SYSTEMS
# tree: shows structure
# os.walk: returns root, dirs, files & a generator -> returns one result at a time
foo = os.walk('/tmp')
type(foo)

# splitting off a file extension
fullpath = "/tmp/somestuff/data.csv"
_, ext = os.path.splitext(fullpath)


## EXAMPLE
matches = []
# Walk the filesystem starting at the test_dir
for root, _, files in os.walk('test_dir'):
    for name in files:
      	# Create the full path to the file
        fullpath = os.path.join(root, name)
        print(f"Processing file: {fullpath}")
        # Split off the extension and discard the rest of the path
        _, ext = os.path.splitext(fullpath)
        # Match the extension pattern .csv
        if ext == ".csv":
            matches.append(fullpath)
            
# Print the matches you find          
print(matches)

## ANOTHER EXAMPLE
# Walk the filesystem starting at the test_dir
for root, _, files in os.walk('cattle'):
    for name in files:
      	
        # Create the full path to the file by using os.path.join()
        fullpath = os.path.join(root, name)
        print(f"Processing file: {fullpath}")
        
        # Rename file
        if "shorthorn" in name:
            p = pathlib.Path(fullpath)
            shortname = name.split("_")[0][0] # You need to split the name by underscore
            new_name = f"{shortname}_longhorn"
            print(f"Renaming file {name} to {new_name}")
            p.rename(new_name)

## THIRD EXAMPLE
# Walk the filesystem starting at the my path
for root, _, files in os.walk('my'):
    for name in files:
      	# Create the full path to the file by using os.path.join()
        fullpath = os.path.join(root, name)
        print(f"Processing file: {fullpath}")
        _, ext = os.path.splitext(fullpath)
        # Match the extension pattern .joblib
        if ext == ".joblib":
            clf = joblib.load(fullpath)
            break

# Predict from pickled model
print(clf.predict(X_digits))


### FIND FILES MATCHING A PATTERN
# Path.glob() finds pattenrs in directories, yields matches and can search recursively

## simple glob patterns
from pathlib import Path
path = Path("data")
list(path.glob("*.csv"))

## recursive glob patterns
list(path.glob("**/*.csv"))

# os.walk: more explicit, can look at directories or files, doesn't return Path object
import os
result = os.walk("/tmp")
next(result)

## fnmatch
if fnmatch.fnmatch(file, '*.csv'):
	log.info(f'Found Match {file}')

# fnmatch translate converts pattern to regex
import fnmatch, re
regex = fnmatch.translate('*.csv')
pattern = re.compile(regex)
print(pattern)

### EXAMPLE
import pathlib
import os

path = pathlib.Path("prod")
matches = sorted(path.glob('*.jar'))
for match in matches:
  print(f"Found rogue .jar file in production: {match}")

### EXAMPLE
import fnmatch

# List of file names to process
files = ["data1.csv", "script.py", "image.png", "data2.csv", "all.py"]

# Function that returns 
def csv_matches(list_of_files):
    """Return matches for csv files"""

    matches = fnmatch.filter(list_of_files, "*.csv")
    return matches

# Call function to find matches
matches = csv_matches(files)
print(f"Found matches: {matches}")

## shutil: high-level file operations
# copy tree
from shutil import copytree, ignore_patterns
copytree(source, destination, ignore=ignore_patterns('*.txt', '*.excel'))

# rm tree
from shutil import rmtree
rmtree(source, destination)

# make_archive
from shutil import make_archive
make_archive("somearchive", "gztar", "inside_tmp_dir")

## tempfile: generates temporary files and directories

### EXAMPLE
# Create a self-destructing temporary file
with tempfile.NamedTemporaryFile() as exploding_file:
  	# This file will be deleted automatically after the with statement block
    print(f"Temp file created: {exploding_file.name}")
    exploding_file.write(b"This message will self-destruct in 5....4...\n")
    
    # Get to the top of the file
    exploding_file.seek(0)

    #Print the message
    print(exploding_file.read())

# Check to sure file self-destructed
if not os.path.exists(exploding_file.name): 
    print(f"self-destruction verified: {exploding_file.name}")


### USING PATHLIB
from pathlib import Path
path = Path("/usr/bin") # create path object
list(path.glob("*"))[0:4] # find all objects at dir root level

# working with PosixPath objects
mypath.cwd() # show current working directory
mypath.exists() # shows whether object exists
mypath.as_posix() # returns full path

# open Makefile from path oject
from pathlib import Path
some_file = Path("Makefile")

with some_file.open() as file_to_read:
	print(file_to_read.readlines()[-1:]) #show last line of object

# create directory
from pathlib import Path
tmp = Path("/tmp/inside_tmp_dir")
tmp.mkdir()

# write text with pathlib
write_path = Path("/tmp/some_file.txt")
write_path.write_text("Wow")
print(write_path.read_text()) # check it wrote

# rename a file
from pathlib import Path
modify_file = Path("/tmp/some_file.txt") #create path object
modify_file.rename("/tmp/some_file_renamed.txt") #rename

### EXAMPLE
import pathlib

# Read the index of social media posts
with open("posts_index.txt") as posts:
  for post in posts.readlines():
    
    # Create a pathlib object
    path = pathlib.Path(post.strip())
    
    # Check if the social media post still exists on disk
    if path.exists():
      print(f"Found active post: {post}")
    else:
      print(f"Post is missing: {post}")

### USING FUNCTIONS FOR AUTOMATION
# functions are units of work

from functools import wraps
import time

def instrument(f): #prints time a function took to run
	@wraps(f) # allows you to preserve func name and docstring
	def wrap(*args, **kw): #inner function
		ts = time.time()
		result = f(*args, **kw)
		te = time.time()
		print(
			f"function: {f.__name__}, args: [{args}, {kw}] took: {te-ts} sec"
			)
		return result
	return wrap #outer function returns inner function back

### OTHER EXAMPLE
# create decorator
def debug(f):
	@wraps(f)
	def wrap(*args, **kw):
		result = f(*args, **kw)
		print(f"function name: {f.__name__}, args: [{args}], kwargs: [{kw}]")
		return result
	return wrap
  
# apply decorator
@debug
def mult(x, y=10):
	return x*y
print(mult(5, y=5))

## UNDERSTAND SCRIPT INPUT
# sys.argv captures input to script
import sys

def hello(user_input):
	print(f"From a user: {user_input}")

if __name__ == "__main__":
	arg1 = sys.argv[1] #get 2nd user_input, passes it to function
	hello(arg1)

# run from command line
python hello_argv.py something

### EXAMPLE
import subprocess

# Write a file
with open("input.txt", "w") as input_file:
  input_file.write("Reverse this string\n")
  input_file.write("Reverse this too!")

# runs python script that reverse strings in a file line by line
run_script = subprocess.Popen(
    ["/usr/bin/python3", "reverseit.py", "input.txt"], stdout=subprocess.PIPE)

# print out the script output
for line in run_script.stdout.readlines():
  print(line)


### INTRODUCTION TO CLICK
# arbitrary nesting of commands, automatic help page generation, lazy loading of subcommands at runtime
import click

@click.command() #allows function to be run as command
@click.option()
df func(): pass

## SIMPLE CLICK EXAMPLE
import click

@click.command() 
@click.option('--phrase', prompt='enter a phrase', help='')
df tokenize(phrase): 
	"""tokenize phrase"""

	click.echo(f"tokenized phrase: {phrase.split()}")

if __name__ == '__main__':
	tokenize()

# then, run from terminal
python hello_click.py

# get help
? python hello_click.py --help

## Mapping functions to subcommand
import click

@click.group()
def cli(): 
	pass

@cli.command()
def one():
	click.echo('One-1')

@cli.command
def two():
	click.echo('Two-2')

if __name__ == '__main__':
	cli()

# using click subcommands
python click_Functions.py
python click_functions.py one

## CLICK UTILITIES
# can generate colored output, generate paginated output, clear the screen
# wait for key press, launch editors, write files

# write with click
with click.open_file(filename, 'w') as f:
	f.write('jazz flute')

# echo
click.echo('Hello!') 

# test click
import click
from click.testing import CliRunner

@click.command()
@click.argument('phrase')
def echo_phrase(phrase):
	click.echo('You said: %s' % phrase)

runner = CliRunner()
result = runner.invoke(echo_phrase, ['have data'])
assert result.output == 'You said: have data'

### INTRODUCTION TO AWS BOTO ######################################################
import boto3
s3 = boto3.client('s3',
				  region_name = 'us-east-1',
				  aws_access_key_id=AWS_KEY_ID,
				  aws_secret_access_key=AWS_SECRET)

# List buckets
response = s3.list_buckets()

# Get Buckets Dictionary
buckets = response['Buckets']
print(buckets)

# Create bucket
bucket = s3.create_bucket(Bucket='gid-requests')

# Delete bucket
response = s3.delete_bucket('gid-requests')

# Uploading files to bucket
s3.upload_file(
	Filename = 'gid_requests_2019_01.csv',
	Bucket ='gid-requests',
	Key = 'gid_requests_2019_01.csv' #name of file in the bucket
	)

# Listing objects in a bucket
response = s3.list_objects(
	Bucket = 'gid-requests',
	MaxKeys=2, #limit number of objects returned, otherwise return up to 1000 objects
	Prefix='gid_requests_2019_' #limits return to prefix
	)

# Getting object metadata
response = s3.head_object(
	Bucket='gid-requests',
	Key='gid_requests_2019_01.csv'
	)

# Downloading files
s3.download_file(
	Filename='gid_requests_downed.csv',
	Bucket='gid-requests',
	Key='gid_requests_2018_12_30.csv'
	)

# Deleting objects
s3.delete_object(
	Bucket='gid-requests',
	Key='gid_requests_2018_12_30.csv'
	)

### EXAMPLE
# List only objects that start with '2018/final_'
response = s3.list_objects(Bucket='gid-staging', 
                           Prefix='2018/final_')

# Iterate over the objects
if 'Contents' in response:
  for obj in response['Contents']:
      # Delete the object
      s3.delete_object(Bucket='gid-staging', Key=obj['Key'])

# Print the keys of remaining objects in the bucket
response = s3.list_objects(Bucket='gid-staging')

for obj in response['Contents']:
  	print(obj['Key'])


### KEEPING OBJECTS SECURE
# can change ACL AFTER uploading file
s3.put_object_acl(
	Bucket='gid-requests',
	Key='potholes.csv',
	ACL='public-read' #or ACL='private'
	)

# or WHILE uploading file
s3.upload_file(
	Filename = 'gid_requests_2019_01.csv',
	Bucket ='gid-requests',
	Key = 'gid_requests_2019_01.csv', #name of file in the bucket
	ExtraArgs={'ACL':'public-read'}
	)

### Accessing public objects
# s3 object url template
https://{buckets}.s3.amazonaws.com/{key}

# URL for key='2019/potholes.csv'
https://gid-requests.s3.amazonaws.com/2019/potholes.csv

# generate object URL string
url = 'https://{}.s3.amazonaws.com/{}'.format("gid-requests", "2019/potholes.csv")
df = pd.read_csv(url)

### Accessing private objects
# download file with s3.download_file then read from disk
# or use get_object
obj = s3.get_object(Bucket='gid-requests', Key='2019/potholes.csv')
print(obj) #returns StreamingBody response
pd.read_csv(obj['Body']) #can be read by pd.read_csv

# OR use pre-signed urls, expire after certain timeframe
s3.generate_presigned_url(
	ClientMethod='get_object',
	ExpiresIn=3600,
	Params={'Bucket': 'gid-requests',
			'Key': 'potholes.csv'}
	)

### SHARING FILES THROUGH A WEBSITE
## Creating HTML tables
df.to_html('table_agg.html', #turn DF to html
	render_links = True,  #make links clickable
	columns['A', 'B', 'C'] #limit columns shown
	) 

## Uploading them
s3.upload_file(
	Filename = 'table_agg.html',
	Bucket ='gid-requests',
	Key = 'table.html', #name of file in the bucket
	ExtraArgs={'ACL':'public-read', 'ContentType': 'text/html'}
	)

# other ContentTypes: application/json, image/png, application/pdf, text/csv

## Generating an index page -> also an option 

### SNS Topics (Simple Notification Service)
## Creating an SNS client
sns = boto3.client('sns',
				   region_name='us-east-1',
				   aws_access_key_id=AWS_KEY_ID,
				   aws_secret_access_key=AWS_SECRET
					)

## Creating a topic
response = sns.create_topic(Name='city-alerts')
topic_arn = response['TopicArn']

## Listing Topics
response = sns.list_topics()

## Delete Topics
sns.delete_topic(TopicArn='arn:aws...etcetc')

### MANAGING SUBSCRIPTIONS
# Endpoint: who it is sent to
# Status: Pending, Sent
# Protocol: how it is sent, SMS, email..

## Creating a subscription
response = sns.subscribe(
	TopicArn='arn:aws...etcetc',
	Protocol='SMS',
	Endpoint='+13125551123'
	)

# List all subscriptions
sns.list_subscriptions()['Subscriptions']

# List subscriptions by topic
sns.list_subscriptions_by_topic(
	TopicArn='arn:aws...etcetc'
	)

# Delete subscription
sns.unsubscribe(
	SubscriptionArn='arn:aws...etcetc'
	)

### SENDING MESSAGES
# Publishing to a topic
response = sns.publish(
	TopicArn = 'arn:aws...etcetc',
	Message = 'Body text',
	Subject = 'Subject Line'
	)

# Sending a single SMS (not part of a subscription!)
response = sns.publish(
	PhoneNumber = '+13125551123',
	Message = 'Body text'
	) 

### REKOGNIZING PATTERNS 
## need to initialize the s3 client, upload a file
## then initialize the rekognition client
rekog = boto3.client('rekognition',
				   region_name='us-east-1',
				   aws_access_key_id=AWS_KEY_ID,
				   aws_secret_access_key=AWS_SECRET
					)

## then call 'detect_labels'
response = rekog.detect_labels(
	Image={'S3Object': {
				'Bucket': 'datacamp-img',
				'Name': 'report.jpg'
			}
		},
	MaxLabels = 10, # max number of labels that are returned
	MinConfidence = 95 # min confidence for labels that are returned
	)

# can also detect text
response = rekog.detect_text(
	Image={'S3Object': {
				'Bucket': 'datacamp-img',
				'Name': 'report.jpg'
			}
		}
	)

### EXAMPLE: counting cats
# Create an empty counter variable
cats_count = 0
# Iterate over the labels in the response
for label in response['Labels']:
    # Find the cat label, look over the detected instances
    if label['Name'] == 'Cat':
        for instance in label['Instances']:
            # Only count instances with confidence > 85
            if (instance['Confidence'] > 85):
                cats_count += 1
# Print count of cats
print(cats_count)

### EXAMPLE: finding words
# Create empty list of words
words = []
# Iterate over the TextDetections in the response dictionary
for text_detection in response['TextDetections']:
  	# If TextDetection type is WORD, append it to words list
    if text_detection['Type'] == 'WORD':
        # Append the detected text
        words.append(text_detection['DetectedText'])
# Print out the words list
print(words)

### TRANSLATING TEXT
# Initialize client
translate = boto3.client('translate',
				   region_name='us-east-1',
				   aws_access_key_id=AWS_KEY_ID,
				   aws_secret_access_key=AWS_SECRET
					)

# Translate text
response = translate.translate_text(
	Text='Hello how are you',
	SourceLanguageCode='auto',
	TargetLanguageCode='es'
	)

### DETECTING LANGUAGE
# Initialize client
comprehend = boto3.client('comprehend',
				   region_name='us-east-1',
				   aws_access_key_id=AWS_KEY_ID,
				   aws_secret_access_key=AWS_SECRET
					)

# Detect dominant language
response = comprehend.detect_dominant_language(
	Text='Hello how are you'
	)

# Detect text sentiment
response = comprehend.detect_sentiment(
	Text='I love you',
	LanguageCode= 'en'
	)['Sentiment']

## EXAMPLE
# Get topic ARN for scooter notifications
topic_arn = sns.create_topic(Name='scooter_notifications')['TopicArn']

for index, row in scooter_requests.iterrows():
    # Check if notification should be sent
    if (row['sentiment'] == 'NEGATIVE') & (row['img_scooter'] == 1):
        # Construct a message to publish to the scooter team.
        message = "Please remove scooter at {}, {}. Description: {}".format(
            row['long'], row['lat'], row['public_description'])

        # Publish the message to the topic!
        sns.publish(TopicArn = topic_arn,
                    Message = message, 
                    Subject = "Scooter Alert")

### INTRODUCTION TO DATABASE DESIGN ######################################################
# skipped

### INTRODUCTION TO SCALA ######################################################
# general-purpose programming language for functional programming and strong
# static type system
# can run on Java virtual machine -> pretty much anywhere
# SCAlable LAnguage

## Scala is object-oriented
# every value is an object
# every operation is a method call
## scala is functional
# functions are first-class values
# operations of a program ... ?

## Scala has 2 types of variable
# val: immutable, cant be reassigned
# var: mutable

## Pros & Cons of immuntability
# pros: cant accid change your data, fewer tests to write
# cons: more memory required

## Types
# Int, Float, Boolean, 

### THE SCALA INTERPRETER
# Calculate the difference between 8 and 5
val difference = 8.-(5)

# Print the difference
println(difference)

## // Define immutable variables for clubs 2♣ through 4♣
var twoClubs: Int = 2
var playerA: String = "Alex"

# Change playerA from Marta to Umberto
playerA = "Umberto"

### FUNCTIONS
def bust(hand: Int): Boolean = {
	hand > 21
}

# can remove the boolean
def bust(hand: Int) = {
	hand > 21
}
# function are first-class values: the = sign is a tell -> returns boolean true or false

## EXAMPLE
#// Calculate hand values
var handPlayerA: int = queenDiamonds + threeClubs + aceHearts + fiveSpades
var handPlayerB: int = kingHearts + jackHearts

// Find and print the maximum hand value
println(maxHand(handPlayerA, handPlayerB))

### COLLECTIONS
## mutable or immutable

## ARRAY (mutable)
# parametrize an array
val players = new Array[String](3) #type parameter, length

# initialize elements
val players = Array("Alex", "Chen")

# arrays are mutable BUT needs to be right type!
players(0) = "Alec" 

# can mix and match types by using Any
val players = new Array[Any](3)

## LIST (immutable)
val players = List("Alex", "Chen")

# some list methods
players.drop()
players.mkString(", ")
players.length
players.reverse

## add elements to a list (really, create another list based on old list)
val newPlayers = "Sindhu" :: players 

# alternatively
var players = List("Alex", "Chen")
players = "Sindhu" :: players

# :: prepends, adds new element to the beginning of a list 
# ::: concatenates lists

## empty list: Nil
# common way to initialize new list
val players = "Alex" :: "Chen" :: Nil

### IF-ELSE
# note: && is AND, || is OR
def maxHand(handA: Int, handB: Int): Int = {
	if (handA > handB) handA
	else handB
}

# outside a function
if (handA > handB) println(handA)
else println(handB)

## MULTIPLE IF ELSE
if (bust(handA) & bust(handB)) println(0)
else if (bust(handA)) println(handB)
else if (bust(handB)) println(handA)
else if (handA > handB) println(handA)
else println(handB)

### WHILE
var i = 0
val num = 3

while (i < num){
	println('some text')
	i = i + 1 #could also be i += 1
}

## EXAMPLE
var i = 0
var hands = Array(17, 24, 21)

while (i < hands.length){
	# BODY OF LOOP
}

### IMPERATIVE V FUNCTIONAL STYLE
## IMPERATIVE
var i = 0
var hands = Array(17, 24, 21)

while  (i < hands.length){
	println(bust(hands(i)))
	i += 1
}

## FUNCTIONAL
# map input values to output values rather than change data in place
var hands = Array(17, 24, 21)
hands.foreach(INSERT_FCT_HERE)

## SIDE EFFECTS
# var: mutable, so often comes with side-effects -> imperative

### INTRODUCTION TO PYSPARK ######################################################
# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

# Spark's core data structure is the Resilient Distributed Dataset (RDD)
# RDDs are hard to work with directly -> Spark DataFrame 

# first start SparkSession object from your SparkContext
# SparkContext +- your connection to the cluster
# SparkSession +- your interface with that connection.

# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark
print(my_spark)

# Print the tables in the catalog
print(spark.catalog.listTables())

## Get stuff from tables
query = "FROM flights SELECT * LIMIT 10"
flights10 = spark.sql(query)
flights10.show()

# Convert the results to a pandas DataFrame
pd_counts = flights10.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

## ADD DATA TO SPARK
# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView('temp')

# Examine the tables in the catalog again
print(spark.catalog.listTables())

### READING CSV
file_path = "/usr/local/share/datasets/airports.csv"
airports = spark.read.csv(file_path, header=True)

### ADDING COLUMNS
# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
flights.show()

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time/60)

### FILTERING COLUMNS
# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")

# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)

### ANOTHER EXAMPLE
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier, )

# Define filters
filterA = flights.origin == "SEA"
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

### MORE COMPLEX SELECT
# Can create more complex selects with .alias()
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")

# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()

### MORE FILTERING
# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

# Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

### GROUPING
# Number of flights each plane made
flights.groupBy("tailnum").count().show()

# Average duration of flights from PDX and SEA
flights.groupBy("origin").avg("air_time").show()

### MORE GROUPING
# can add additional sql aggregate functions from pyspark.sql.functions
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()

### JOINS
# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on = 'dest', how = 'leftouter')

# Examine the new DataFrame
print(flights_with_airports.show())

### MACHINE LEARNING PIPELINE
# Transformer:  takes a DataFrame and returns a new DataFram
# Estimator classes: takes a DataFrame, implement a .fit(), returns a model

## CHANGING TYPES
# Spark only hands **numerical** data for modeling -> use .cast()
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# If no obvious order for string, can create 'one-hot vectors'
# 1st create a StringIndexer
carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")

# 2nd encode it with OneHotEncoder
carr_encoder = OneHotEncoder(inputCol="carrier_index", outputCol="carrier_fact")

# Before modelling need to combine all columns into one single column
vec_assembler = VectorAssembler(
	inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], 
	outputCol="features"
	)

## CREATE THE PIPELINE
# Import Pipeline
from pyspark.ml import Pipeline

# Make the pipeline
flights_pipe = Pipeline(stages=[dest_indexer, 
								dest_encoder, 
								carr_indexer, 
								carr_encoder, 
								vec_assembler])

# Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)

# Split the data into training and test sets
training, test = piped_data.randomSplit([.6, .4])

### START TRAINING MODEL
## IMPORT MODEL
# Import LogisticRegression
from pyspark.ml.classification import LogisticRegression

# Create a LogisticRegression Estimator
lr = LogisticRegression()

## IMPORT EVALUATOR
# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName="areaUnderROC")

## IMPORT TUNING
# Import the tuning submodule
import pyspark.ml.tuning as tune

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()

## CREATE CVor
cv = tune.CrossValidator(estimator=lr,
               estimatorParamMaps=grid,
               evaluator=evaluator
               )

## FIT MODEL
# Call lr.fit()
best_lr = lr.fit(training)

# Use the model to predict the test set
test_results = best_lr.transform(test)

# Evaluate the predictions
print(evaluator.evaluate(test_results))

### BIG DATA FUNDAMENTALS WITH PYSPARK ######################################################
## The 3 Vs of Big Data: Volume, Variety, Velocity
## Big Data concepts
# Clustered computing: collection of resources of different machines
# Parallel computing: simultaneous computation
# Distributed computing: collection of nodes that run in parallel
# Batch processing: breaking the job into small pieces and run them on individual machnines

## Spark mode of deployments
# local mode: convenient for testing, debugging
# cluster mode: set of pre-defined machines

### SparkContext: entry point to Spark
sc.version
sc.pythonVer
sc.master

## Functional programming
# map(func, list): takes a function and a list, returns a new list w/ items returned
# filter(func, list): takes a function and a list, returns a new list w/ items that evaluated to True
list(filter(lambda x: (x % 2 != 0), items))

## Creating RDDs
# from existing collection
rdd = sc.parallelize([1, 2, 4])

# from external datasets
rdd2 = sc.textFile('test.txt')

## Controlling partitions
rdd2 = sc.textFile('test.txt', minPartitions = 6)
rdd2.getNumPartitions()

## RDD Transformations
# Note: performed with lazy evaluation
rdd.map(lambda x: x * x)
rdd.filter(lambda x: x > 2)

# flatmap: returns multiple values for each element in original RDD
mylist = ['Hello World', 'How are you']
rdd = sc.parallelize(mylist)
rdd.flatMap(lambda x: x.split(" ")) #returns 'hello' 'world' 'how' 'are' 'you'

# union: returns the union of 2 rdds
rdd.union(rdd2)

## RDD Actions
# collect(): return all the elements in rdd
# take(N): returns an array with the first N elements
# first: returns the first element
# count: returns the number of elements

### Pair RDDs
## special structure for key/value pairs in datasets
# created from key-value tuple
my_tuple = [("a", 1), ("b", 2)]
rdd = sc.parallelize(my_tuple)

# or created from regular rdd
my_list = ['Sam 23', 'Mary 34', 'Peter 25']
regularRDD = sc.parallelize(my_list)
pairRDD_RDD = regularRDD.map(lambda s: (s.split(' ')[0], s.split(' ')[1]))

## transformations
# reduceByKey(): combines values with the same key, runs parallel operations for each key in df
reducebykey_rdd = rdd.reduceByKey(lambda x, y: x + y)

# sortByKey(): returns rdd sorted in ascending or descending order
rdd.sortByKey(ascending = False)

# groupByKey(): groups all values with the same key
airports = [("US", "JFK"),("UK", "LHR"),("FR", "CDG"), ("US", "SFO")]
regularRDD = sc.parallelize(airports)
pairRDD_group = regularRDD.groupByKey().collect()

for cont, air in pairRDD_group:
	print(cont, list(air))

# join(): joins two pair RDD based on keys
RDD1.join(RDD2).collect()

### ADVANCED RDD ACTIONS
## reduce(func) : aggregates elements of regular rdd
x = [1, 3, 4, 5]
rdd = sc.parallelize(x)
rdd.reduce(lambda x, y: x + y) # returns 14

## saveAsTextFile(): saves RDD into text file, with each partition as a sep file
# can use coalesce() to save all as one file

## for pair RDDs
# countByKey(): only for type key value
rdd = sc.parallelize(("a", 1), ("b", 1), ("a", 1))

for key, val in rdd.countByKey().items():
	print(key, val)

# collectAsMap(): returns key-value pairs as a dictionary

### EXAMPLE CODE
# Convert the words in lower case and remove stop words from stop_words
splitRDD_no_stop = splitRDD.filter(lambda x: x.lower() not in stop_words)

# Create a tuple of the word and 1 
splitRDD_no_stop_words = splitRDD_no_stop.map(lambda w: (w, 1))

# Count of the number of occurences of each word
resultRDD = splitRDD_no_stop_words.reduceByKey(lambda x, y: x + y)

# Display the first 10 words and their frequencies
for word in resultRDD.take(10):
	print(word)

# Swap the keys and values 
resultRDD_swap = resultRDD.map(lambda x: (x[1], x[0]))

# Sort the keys in descending order
resultRDD_swap_sort = resultRDD_swap.sortByKey(ascending=False)

# Show the top 10 most frequent words and their frequencies
for word in resultRDD_swap_sort.take(10):
	print("{} has {} counts". format(word[1], word[0]))


### ABSTRACTING DATA WITH DATAFRAMES
# creating DF from rdd
spark.createDataFrame(rdd, schema = ['Model', 'Year', 'Height'])

# from csv / json / txt file
spark.read.csv('people.csv', header = True, inferSchema = True)

## DF Transformations
# orderby()
df.count().orderBy('Age').show(3)

# dropDuplicates()
test_df.select('User_ID', 'Gender', 'Age').dropDuplicates()

# printSchema: prints types of all columns
test_df.printSchema()

# columns: prints all columns
test_df.columns

# describe(): prints summary stats
test_df.describe().show()