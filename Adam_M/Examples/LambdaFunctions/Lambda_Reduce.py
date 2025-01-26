#Reduce
#also same like map() and filter() but its return only a SINGLE VALUE or item, 
# but to use reduce( ) function you have to import first from functools import reduce

#without lambda
from functools import reduce
 
def add(x,y):
    return x+y
 
c = [1,2,3,4,5,6]
total = reduce(add, c)
print("Sum with py functools library: ",total)
#output :   21


#---------------------
#with lambda

total_withL = reduce(lambda x,y: x + y, c)
print("Sum with lambda: ",total_withL)