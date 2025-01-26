
#Now we apply iterrows() function in order to get a each element of rows.
# importing pandas as pd
import pandas as pd
  
# dictionary of lists
dict = {'name':["aparna", "pankaj", "sudhir", "Geeku"],
        'degree': ["MBA", "BCA", "M.Tech", "MBA"],
        'score':[90, 40, 80, 98]}
 
# creating a dataframe from a dictionary 
df = pd.DataFrame(dict)
 
# iterating over rows using iterrows() function 
for i, j in df.iterrows():
    print(i, j)
    print()

#output
#0 name      aparna
#degree       MBA
#score         90
#Name: 0, dtype: object

#-----------------

#Iterating over Columns
# creating a list of dataframe columns
columns = list(df)
 
for i in columns:
 
    # printing the third element of the column
    print (df[i][2])

#----------------------------------------------------
#DataFrame Methods:

#FUNCTION	DESCRIPTION
#index()	Method returns index (row labels) of the DataFrame
#insert()	Method inserts a column into a DataFrame
#add()	Method returns addition of dataframe and other, element-wise (binary operator add)
#sub()	Method returns subtraction of dataframe and other, element-wise (binary operator sub)
#mul()	Method returns multiplication of dataframe and other, element-wise (binary operator mul)
#div()	Method returns floating division of dataframe and other, element-wise (binary operator truediv)
#unique()	Method extracts the unique values in the dataframe
#nunique()	Method returns count of the unique values in the dataframe
#value_counts()	Method counts the number of times each unique value occurs within the Series
#columns()	Method returns the column labels of the DataFrame
#axes()	Method returns a list representing the axes of the DataFrame
#isnull()	Method creates a Boolean Series for extracting rows with null values
#notnull()	Method creates a Boolean Series for extracting rows with non-null values
#between()	Method extracts rows where a column value falls in between a predefined range
#isin()	Method extracts rows from a DataFrame where a column value exists in a predefined collection
#dtypes()	Method returns a Series with the data type of each column. The result’s index is the original DataFrame’s columns
#astype()	Method converts the data types in a Series
#values()	Method returns a Numpy representation of the DataFrame i.e. only the values in the DataFrame will be returned, the axes labels will be removed
#sort_values()- Set1, Set2	Method sorts a data frame in Ascending or Descending order of passed Column
#sort_index()	Method sorts the values in a DataFrame based on their index positions or labels instead of their values but sometimes a data frame is made out of two or more data frames and hence later index can be changed using this method
#loc[]	Method retrieves rows based on index label
#iloc[]	Method retrieves rows based on index position
#ix[]	Method retrieves DataFrame rows based on either index label or index position. This method combines the best features of the .loc[] and .iloc[] methods
#rename()	Method is called on a DataFrame to change the names of the index labels or column names
#columns()	Method is an alternative attribute to change the coloumn name
#drop()	Method is used to delete rows or columns from a DataFrame
#pop()	Method is used to delete rows or columns from a DataFrame
#sample()	Method pulls out a random sample of rows or columns from a DataFrame
#nsmallest()	Method pulls out the rows with the smallest values in a column
#nlargest()	Method pulls out the rows with the largest values in a column
#shape()	Method returns a tuple representing the dimensionality of the DataFrame
#ndim()	Method returns an ‘int’ representing the number of axes / array dimensions.
#Returns 1 if Series, otherwise returns 2 if DataFrame
#dropna()	Method allows the user to analyze and drop Rows/Columns with Null values in different ways
#fillna()	Method manages and let the user replace NaN values with some value of their own
#rank()	Values in a Series can be ranked in order with this method
#query()	Method is an alternate string-based syntax for extracting a subset from a DataFrame
#copy()	Method creates an independent copy of a pandas object
#duplicated()	Method creates a Boolean Series and uses it to extract rows that have duplicate values
#drop_duplicates()	Method is an alternative option to identifying duplicate rows and removing them through filtering
#set_index()	Method sets the DataFrame index (row labels) using one or more existing columns
#reset_index()	Method resets index of a Data Frame. This method sets a list of integer ranging from 0 to length of data as index
#where()	Method is used to check a Data Frame for one or more condition and return the result accordingly. By default, the rows not satisfying the condition are filled with NaN value    