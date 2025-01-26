#DICTIONARY


#1. Database Record Representation
record = {
    'id': 1,
    'name': 'John Doe',
    'email': 'john.doe@example.com',
    'age': 30
}
print("Database representation: ",record)


print("-----------------")
#2. Counting Frequency of Elements
elements = ['apple', 'banana', 'apple', 'orange', 'banana', 'apple']
print("Elements: ",elements)
frequency = {}                  #set dict
for item in elements:
    frequency[item] = frequency.get(item, 0) + 1
print("Counted: ",frequency)
# Output: {'apple': 3, 'banana': 2, 'orange': 1}


#3. Fast Lookup Tables
print("-----------------")
lookup = {
    'USD': 'United States Dollar',
    'EUR': 'Euro',
    'JPY': 'Japanese Yen'
}
currency_name = lookup.get('USD')  
print("Dict of currencies: ", lookup)
print("Get lookup for USD: ", currency_name)
# Output: 'United States Dollar'

#4. Storing and Accessing JSON Data
print("-----------------")
import json
json_data = '{"name": "Alice", "age": 25, "city": "New York"}'
print("Dict of names: ",json_data)
data = json.loads(json_data)
# Accessing data
name = data['name']+' from '+data['city'] 
print("Return name: ",name)
 # Output: 'Alice from New York'

 #5. Grouping Data by Keys
print("-----------------")
data = [
    {'name': 'Alice', 'department': 'HR'},
    {'name': 'Bob', 'department': 'IT'},
    {'name': 'Charlie', 'department': 'HR'},
    {'name': 'David', 'department': 'IT'}
]
print("Dict of another names: ",data)
grouped = {}
for item in data:
    department = item['department']
    if department not in grouped:
        grouped[department] = []
    grouped[department].append(item['name'])
print("Names in each department: ",grouped)    
# Output: {'HR': ['Alice', 'Charlie'], 'IT': ['Bob', 'David']}