import numpy as np

#script splits into smaller dictionaries based on parameter, you can split it depends on what value is set as divide

data_list = {
    "divide": "2",
    "elements": ["ino", "g", "yes", "np", "ale", "aa" ]

}
result_list = {}

listelements = data_list["elements"]
divV = int(data_list["divide"] )

split = np.array_split(listelements,divV)
for row in split:
    list_str = ",".join(map(str,list(row)))
    table_list_dict = {"elem_list": list_str}
    result_list["final_list"] = table_list_dict

    print(result_list["final_list"])