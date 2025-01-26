#sets

#1. Removing Duplicates from a Set
my_list = [1, 2, 2, 3, 4, 4, 5]
print("List: ",my_list)
unique_elements = set(my_list)
print("Removing duplicates: ",unique_elements)  
# Output: {1, 2, 3, 4, 5}


print("-------------------")
#2.Set Operations (Union, Intersection, Difference)
set1 = {1, 2, 3, 4}
set2 = {3, 4, 5, 6}
print("Set1: ",set1)
print("Set2: ",set2)
union = set1.union(set2)          # {1, 2, 3, 4, 5, 6}
intersection = set1.intersection(set2)  # {3, 4}
difference = set1.difference(set2)  # {1, 2}
print("Union: ",union)
print("Intersection: ",intersection)
print("Difference: ",difference)

#3.Membership Testing
print("-------------------")
prohibited_items = {"knife", "gun", "drugs"}
item = "knife"
print("Items: ",prohibited_items)
print("Checking for the existence of an element in a collection...")
if item in prohibited_items:
    print(f"{item} is prohibited.")

#4. Finding Common Elements
print("-------------------")
friends_A = {"Alice", "Bob", "Charlie"}
friends_B = {"Bob", "David", "Eve"}
print("Set 1: ",friends_A)
print("Set 2: ",friends_B)
common_friends = friends_A.intersection(friends_B)
print(common_friends)  # Output: {'Bob'}

#5.Handling Data in Multi-Set Scenarios
dataset1 = {"apple", "banana", "cherry"}
dataset2 = {"banana", "cherry", "date"}
# {'apple', 'banana', 'cherry', 'date'}
print("Dataset1: ",dataset1)
print("Dataset2: ",dataset2)
combined_unique = dataset1.union(dataset2)  
 # {'banana', 'cherry'}
overlap = dataset1.intersection(dataset2)
print("Combined: ",combined_unique)
print("Overlap: ",overlap)