
#Data Storage and Manipulation
data = [23, 45, 12, 67, 34]
print("Original: ",data)
data.append(89)  # Add an item
print("Add 89: ",data)
data.remove(45)  # Remove an item
print("Remove 45: ",data)

print("--------------------------")
#Implementing Stacks and Queues
stack = []
stack.append('a')  # Push onto stack
stack.append('b')
print("Add a b to stack: ",stack)
print("Remove top element -> a: ",stack.pop())  # Pop from stack -> 'b'

print("--------------------------")
#3. Iteration and Data Processing
numbers = [1, 2, 3, 4, 5]
total = sum(numbers)
print(f"Total: {total}")


#4.Dynamic Arrays
print("--------------------------")
results = []
for i in range(10):
    results.append(i * i)
print("Multiplication of i element: ",results)    

#5. Storing and Processing Strings
print("--------------------------")
sentence = "Python lists are powerful"
print("Sentence: ",sentence)
words = sentence.split()
for word in words:
    print(word.upper())