import pandas as pd

#cript to create copy of pandas data frame

lst = ['Geeks', 'For', 'Geeks', 'is', 'portal', 'for', 'Geeks'] 
  # Calling DataFrame constructor on list 
df_pd = pd.DataFrame(lst) 

df_dp_cp = df_pd.copy()

print("orig-----")
print(df_pd)
print("copy-----")
print(df_dp_cp)
