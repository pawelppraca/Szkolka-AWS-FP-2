#Lambda functions can be used within list comprehensions to apply a function to each element in a list.

# List of numbers
numbers = [1, 2, 3, 4, 5]
# Using lambda in list comprehension to square each number
squared_numbers = [(lambda x: x ** 2)(x) for x in numbers]
print(squared_numbers)  # Output: [1, 4, 9, 16, 25]



