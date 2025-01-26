#map
# function that apply on the iterable items. 
# It takes two argument first a function and second an iterable item and return an object and we store that object in a list

#Syntax :- map(function, iterable or (list))


#with Regular Function 
def sqr(x):
    c = x*x
    return c
 
lst_of_digit = [1,2,3,4,5]
sqr_lst= list(map(sqr, lst_of_digit))
print(sqr_lst)
#output: [1, 4, 9, 16, 25]

#----------------
#With Lambda Function 
sqr_lst = [1,2,3,4,5]
sqr_lst= list(map(lambda item: item*item, sqr_lst))
print(sqr_lst)
#output:[1, 4, 9, 16, 25]

