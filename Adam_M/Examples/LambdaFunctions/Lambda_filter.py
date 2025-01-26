

#filter the data, list

num = [1, 2, 3, 4, 5, 6, 7]
# We want to keep only even (parzyste) numbers, divide by 2
evens = list(filter(lambda x: x % 2 == 0, num))
print("Even numbers with lambda: ",evens)
# Output: [2, 4, 6]

#return even (parzyste) numbers, divide by 2
def is_even(x):         #method
     return x%2==0
lst = list(filter(is_even, num))
print("Even numbers without lambda: ",lst)


#return only numbers greated than 4
greatthan4 = list(filter(lambda x: x > 4, num))
print("Numbers > 4: ",greatthan4)

