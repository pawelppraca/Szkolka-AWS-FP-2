names = [
    ('Arsh', 25),
    ('Balli', 30),
    ('Cutie', 20),
    ('Ania', 19)
]

#sort by 1st element without lambda
sort_nolambda = sorted(names)
print("Standard py sort funct, no lambda: ", sort_nolambda)

# Sort by age using sorted
sorted_people = sorted(names, key=lambda person: person[1] ) # person[1] tells to sort by 2nd column, if want to sort by name then person[0]
print('Sorted people by sorted: ',sorted_people)
# Output: [('Cutie', 20), ('Arsh', 25), ('Balli', 30)]

#sort by age desc using sorted
sorted_people_desc = sorted(names, key=lambda x: x[1],reverse=True)
print('Sorted people desc: ',sorted_people_desc)
# output: [('Balli', 30), ('Arsh', 25), ('Cutie', 20)]

# Sort by age using sort
names.sort(key=lambda person: person[1])
print("Sorted people desc by sort", names)
# output: [('Cutie', 20), ('Arsh', 25), ('Balli', 30)]

print("------------------------------")
#--------------------------------------
#-- sorting the numbers based on each of the second digits
num_list = [22, 34, 11, 35, 89, 37, 93, 56, 108]
print('Original Number:', num_list) 
# output: [22, 34, 11, 35, 89, 37, 93, 56, 108]


num_list.sort(key=lambda num: num % 10)
print('Lambda Sort - sorted number:', num_list) 
# output: [11, 22, 93, 34, 35, 56, 37, 108, 89]

num_sort = sorted(num_list, key=lambda num: num % 10)
print('Lambda sorted - sorted numbers',num_sort)

#The only difference between the sort() and sorted() method is that sorted() takes a compulsory iterable and sort() does not.
#Python lists have a built-in list.sort() method that modifies the list in-place. 
#There is also a sorted() built-in function that builds a new sorted list from an iterable.

#--------------------------------------

footballers_and_nums = [("Fabregas", 4),("Beckham" ,10),("Yak", 9), ("Lampard", 8), ("Ronaldo", 7), ("Terry", 26), ("Van der Saar", 1), ("Yobo", 2)]
sorted_footballers_and_nums = sorted(footballers_and_nums, key=lambda index : index[1])
print("Original footballers and jersey numbers", footballers_and_nums) 
# Original footballers and jersey numbers [('Fabregas', 4), ('Beckham', 10), ('Yak', 9), ('Lampard', 8), ('Ronaldo', 7), ('Terry', 26), ('Van der Saar', 1), ('Yobo', 2)]
print("Sorted footballers by jersey numbers:", sorted_footballers_and_nums) 
# Sorted footballers by jersey numbers: [('Van der Saar', 1), ('Yobo', 2), ('Fabregas', 4), ('Ronaldo', 7), ('Lampard', 8), ('Yak', 9), ('Beckham', 10), ('Terry', 26)]


