# Python program converting
# a series into list
 
# importing pandas module  
import pandas as pd  
   
# importing regex module 
import re 
     
# making data frame  
data = pd.read_csv("D:/Docs/documents/python/csv/various/nba.csv")  
     
# removing null values to avoid errors  
data.dropna(inplace = True)  
   
# storing dtype before operation 
dtype_before = type(data["Salary"]) 
   
# converting to list 
salary_list = data["Salary"].tolist() 
   
# storing dtype after operation 
dtype_after = type(salary_list) 
   
# printing dtype 
print("Data type before converting = {}\nData type after converting = {}"
      .format(dtype_before, dtype_after)) 
   
# displaying list 
print(salary_list)
print(data["Salary"])

#--------------------
#Binary operation methods on series:

#FUNCTION	DESCRIPTION
#add()	Method is used to add series or list like objects with same length to the caller series
#sub()	Method is used to subtract series or list like objects with same length from the caller series
#mul()	Method is used to multiply series or list like objects with same length with the caller series
#div()	Method is used to divide series or list like objects with same length by the caller series
#sum()	Returns the sum of the values for the requested axis
#prod()	Returns the product of the values for the requested axis
#mean()	Returns the mean of the values for the requested axis
#pow()	Method is used to put each element of passed series as exponential power of caller series and returned the results
#abs()	Method is used to get the absolute numeric value of each element in Series/DataFrame
#cov()	Method is used to find covariance of two series
 
#Pandas series method:

#FUNCTION	DESCRIPTION
#Series()	A pandas Series can be created with the Series() constructor method. This constructor method accepts a variety of inputs
#combine_first()	Method is used to combine two series into one
#count()	Returns number of non-NA/null observations in the Series
#size()	Returns the number of elements in the underlying data
#name()	Method allows to give a name to a Series object, i.e. to the column
#is_unique()	Method returns boolean if values in the object are unique
#idxmax()	Method to extract the index positions of the highest values in a Series
#idxmin()	Method to extract the index positions of the lowest values in a Series
#sort_values()	Method is called on a Series to sort the values in ascending or descending order
#sort_index()	Method is called on a pandas Series to sort it by the index instead of its values
#head()	Method is used to return a specified number of rows from the beginning of a Series. The method returns a brand new Series
#tail()	Method is used to return a specified number of rows from the end of a Series. The method returns a brand new Series
#le()	Used to compare every element of Caller series with passed series.It returns True for every element which is Less than or Equal to the element in passed series
#ne()	Used to compare every element of Caller series with passed series. It returns True for every element which is Not Equal to the element in passed series
#ge()	Used to compare every element of Caller series with passed series. It returns True for every element which is Greater than or Equal to the element in passed series
#eq()	Used to compare every element of Caller series with passed series. It returns True for every element which is Equal to the element in passed series
#gt()	Used to compare two series and return Boolean value for every respective element
#lt()	Used to compare two series and return Boolean value for every respective element
#clip()	Used to clip value below and above to passed Least and Max value
#clip_lower()	Used to clip values below a passed least value
#clip_upper()	Used to clip values above a passed maximum value
#astype()	Method is used to change data type of a series
#tolist()	Method is used to convert a series to list
#get()	Method is called on a Series to extract values from a Series. This is alternative syntax to the traditional bracket syntax
#unique()	Pandas unique() is used to see the unique values in a particular column
#nunique()	Pandas nunique() is used to get a count of unique values
#value_counts()	Method to count the number of the times each unique value occurs in a Series
#factorize()	Method helps to get the numeric representation of an array by identifying distinct values
#map()	Method to tie together the values from one object to another
#between()	Pandas between() method is used on series to check which values lie between first and second argument
#apply()	Method is called and feeded a Python function as an argument to use the function on every Series value. This method is helpful for executing custom operations that are not included in pandas or numpy
