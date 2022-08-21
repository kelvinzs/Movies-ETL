jupyter notebook

import json
import pandas as pd
import numpy as np
import re

file_dir = '/Users/kelvinigwe/Desktop/UoT_Data_Analysis/Module8(ETL)/Movies-ETL'

f'{file_dir}filename'

with open(f'{file_dir}/wikipedia-movies.json', mode='r') as file:
    wiki_movies_raw = json.load(file)

len(wiki_movies_raw)

# First 5 records
wiki_movies_raw[:5]


# Last 5 records
wiki_movies_raw[-5:]


# Some records in the middle
wiki_movies_raw[3600:3605]


kaggle_metadata = pd.read_csv('movies_metadata.csv', low_memory=False)
ratings = pd.read_csv('ratings.csv')


kaggle_metadata.head()

kaggle_metadata.tail()


#5 randomly chosen rows from dataset
kaggle_metadata.sample(n=5)



wiki_movies_df = pd.DataFrame(wiki_movies_raw)
wiki_movies_df.head()


#unsuccessful attempt to view all columns 
print(wiki_movies_df.columns)


#to view all columns in a dataframe 
wiki_movies_df.columns.tolist()


wiki_movies_df.info()



#Adding movies with director and imdb link data 
#c#wiki_movies = [movie for movie in wiki_movies_raw
#                if('Director' in movie or 'Directed by' in movie)
#                     and 'imdb_link' in movie]
#len(wiki_movies)



wiki_df = pd.DataFrame(wiki_movies)



wiki_df.columns.tolist()


wiki_df.head()


wiki_movies = [movie for movie in wiki_movies_raw 
                if ('Director' in movie or 'Directed by' in movie )
                    and 'imdb_link' in movie
                    and 'No. of episodes' not in movie]



#SAMple/proof that local variable takes precedence over global variable when function is run, although global verible remains unchanged 
x = 'global value'

def foo():
    x = 'local value'
    print(x)

foo()
print(x)




#syntax to make copy of list or dictionary.
#When passing mutable objects like a dict or list as parameters to a function, the function can change the values inside the object.
#dont run syntax rather attempt(lists here are not initialized)

new_list = list(old_list)
new_dict = dict(old_dict)



wiki_movies_df[wiki_movies_df['Arabic'].notnull()]['url']


sorted(wiki_movies_df.columns.tolist())



#Step 1: Make an empty dict to hold all of the alternative titles.
#Step 2: Loop through a list of all alternative title keys
#Step 2a: Check if the current key exists in the movie object.
#Step 2b: If so, remove the key-value pair and add to the alternative titles dictionary.
#Step 3: After looping through every key, add the alternative titles dict to the movie object.

def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    return movie





#list of cleaned movies with a list comprehension:
clean_movies = [clean_movie(movie) for movie in wiki_movies]



#Set wiki_movies_df to be the DataFrame created from clean_movies, and print out a list of the columns.
wiki_movies_df = pd.DataFrame(clean_movies)
sorted(wiki_movies_df.columns.tolist())



#Adding a function(change_column_name) within a function(clean_movie)
def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie




#rerun our list comprehension to clean wiki_movies and recreate wiki_movies_df
clean_movies = [clean_movie(movie) for movie in wiki_movies]
wiki_movies_df = pd.DataFrame(clean_movies)
sorted(wiki_movies_df.columns.tolist())




#"(tt\d{7})" — The parentheses marks say to look for one group of text.
#The "tt" in the string simply says to match two lowercase Ts.
#The "\d" says to match a numerical digit.
#The "{7}" says to match the last thing (numerical digits) exactly seven times.
#Since regular expressions use backslashes, which Python also uses for special characters, we want to tell Python to treat our regular expression characters as a raw string of text. Therefore, we put an r before the quotes

wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')



#Dropping duplicate records 
print(len(wiki_movies_df))
wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
print(len(wiki_movies_df))
wiki_movies_df.head()



#get the count of null values for each column using a list comprehension
[[column,wiki_movies_df[column].isnull().sum()] for column in wiki_movies_df.columns]



#ist of columns that have less than 90% null values and use those to trim down our dataset.
wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]



wiki_movies_df.info()


wiki_movies_df.dtypes


#so first we'll make a data series that drops missing values in the box office column
box_office = wiki_movies_df['Box office'].dropna()



#Sample of named method for defining functions and anonymous method using lambda in the next cell beneath
def is_not_a_string(x):
    return type(x) != str

box_office[box_office.map(is_not_a_string)]




#Sample of anonymous function 
box_office[box_office.map(lambda x: type(x) != str)]



#From the output, we can see that there are quite a few data points that are stored as lists. There is a join() string method that concatenates list items into one string; however, we can't just type join(some_list) because the join() method belongs to string objects.
box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)


#Observe differnce between index 54 in cell 58 and cell 61. In Cell 58, index 54 was a list, and in cell 61 its a string. 
box_office[54]


#Regex form for box office
form_one = r'\$\d+\.?\d*\s*[mb]illion'


box_office.str.contains(form_one, flags=re.IGNORECASE, na=False).sum()



form_two = r'\$\d{1,3}(?:,\d{3})+'
box_office.str.contains(form_two, flags=re.IGNORECASE, na=False).sum()




#create the two Boolean series with the following code
matches_form_one = box_office.str.contains(form_one, flags=re.IGNORECASE, na=False)
matches_form_two = box_office.str.contains(form_two, flags=re.IGNORECASE, na=False)



#To make our code easier to understand, we'll create two Boolean Series called matches_form_one and matches_form_two, and then select the box office values that don't match either.
# this will throw an error!
box_office[(not matches_form_one) and (not matches_form_two)]




#Read comment above 
box_office[~matches_form_one & ~matches_form_two]




#Some values have spaces in between the dollar sign and the number.
#This is easy to fix. Just add \s* after the dollar signs. The new forms should look like the following:

#Updated Form 2 to capture more values 
form_one = r'\$\s*\d+\.?\d*\s*[mb]illion'
form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'




#3. Some values are given as a range.
#To solve this problem, we'll search for any string that starts with a dollar sign and ends with a hyphen, and then replace it with just a dollar sign using the replace() method
box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
box_office





#Updated form 1
form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'




#The f-string f'{form_one}|{form_two}' will create a regular expression that matches either form_one or form_two, so we just need to put the whole thing in parentheses to create a capture group. Our final string will be f'({form_one}|{form_two})', and the full line of code to extract the data follows:
box_office.str.extract(f'({form_one}|{form_two})')




#Now we need a function to turn the extracted values into a numeric value. We'll call it parse_dollars, and parse_dollars will take in a string and return a floating-point number

##NOTE:THIS IS SKELETON FOR actual code to imrpove understanding

def parse_dollars(s):
    # if s is not a string, return NaN

    # if input is of the form $###.# million

        # remove dollar sign and " million"

        # convert to float and multiply by a million

        # return value

    # if input is of the form $###.# billion

        # remove dollar sign and " billion"

        # convert to float and multiply by a billion

        # return value

    # if input is of the form $###,###,###

        # remove dollar sign and commas

        # convert to float

        # return value

    # otherwise, return NaN






def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan









wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)



wiki_movies_df['box_office']




wiki_movies_df.drop('Box office', axis=1, inplace=True)



#Create a budget variable with the following code:
budget = wiki_movies_df['Budget'].dropna()




#Convert any lists to strings:
budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)




#Then remove any values between a dollar sign and a hyphen (for budgets given in ranges):

budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)




#Use the same pattern matches that you created to parse the box office data, and apply them without modifications to the budget data
matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE, na=False)
matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE, na=False)
budget[~matches_form_one & ~matches_form_two]



budget = budget.str.replace(r'\[\d+\]\s*', '')
budget[~matches_form_one & ~matches_form_two]



#Everything is now ready to parse the budget values.
wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)




#We can also drop the original Budget column.

wiki_movies_df.drop('Budget', axis=1, inplace=True)



#Parse Release date
release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)




#The forms we'll be parsing are:

#1Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
#2Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
#3Full month name, four-digit year (i.e., January 2000)
#4Four-digit year

date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]?\d,\s\d{4}'
date_form_two = r'\d{4}.[01]\d.[0123]\d'
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
date_form_four = r'\d{4}'




#extract the dates with:
wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)



#Parse running time 
#First, make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings:

running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)




#It looks like most of the entries just look like "100 minutes." Let's see how many running times look exactly like that by using string boundaries.

running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE, na=False).sum()



#The above code returns 6,528 entries. Let's get a sense of what the other 366 entries look like.

running_time[running_time.str.contains(r'^\d*\s*minutes$', flags=re.IGNORECASE, na=False) != True]



#Let's make this more general by only marking the beginning of the string, and accepting other abbreviations of "minutes" by only searching up to the letter "m."

running_time.str.contains(r'^\d*\s*m', flags=re.IGNORECASE, na=False).sum()



#it's time to extract values. We only want to extract digits, and we want to allow for both possible patterns. Therefore, we'll add capture groups around the \d instances as well as add an alternating character. 
running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')



#Unfortunately, this new DataFrame is all strings, we'll need to convert them to numeric values. Because we may have captured empty strings, we'll use the to_numeric() method and set the errors argument to 'coerce'.

running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)




#Now we can apply a function that will convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero, and save the output to wiki_movies_df:

wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)



#Finally, we can drop Running time from the dataset with the following code

wiki_movies_df.drop('Running time', axis=1, inplace=True)




##NOTE KAGGLE DATA Cleaning
kaggle_metadata.dtypes



#Checking for bad data
kaggle_metadata['adult'].value_counts()


#INSPECTING Bad Data
kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]



#Remove unwanted data, along corrupt data keeping only the data required
kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')



kaggle_metadata['video'].value_counts()



#converting video column to booloean data type
#This code creates the Boolean column we want. We just need to assign it back to video:
kaggle_metadata['video'] == 'True'



#Assignment done here 
kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'



#conversion for the numeric columns, we can just use the to_numeric() method from Pandas. We'll make sure the errors= argument is set to 'raise', so we'll know if there's any data that can't be converted to numbers.

kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')



#Converting release_date with pandas built in to_datetime() function
kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])



#Inspecting the rating dataset
ratings.info(null_counts=True)


#Converting date time to the appropriate format

pd.to_datetime(ratings['timestamp'], unit='s')



#assigning the output to the timestamp column.

ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')




#syntax to inspect data by plotting histogram and checking measures of central tendency
pd.options.display.float_format = '{:20,.2f}'.format
ratings['rating'].plot(kind='hist')
ratings['rating'].describe()








