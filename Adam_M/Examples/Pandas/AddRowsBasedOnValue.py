import pandas as pd

df = pd.DataFrame({ 'X':['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o'], 'Y':[1,2,2,3,4,1,2,2,1,2,1,2,1,2,3] })

decompose = lambda x: [10**i for i,a in enumerate(str(x)[::-1]) for _ in range(int(a))]

df['Y'] = df['Y'].apply(decompose)
out = df.explode('Y')
print(out)

#-output
#    X  Y
#0   a  1
#1   b  1
#1   b  1
#2   c  1
#2   c  1
#3   d  1
#3   d  1
#3   d  1
#4   e  1
#4   e  1
#4   e  1
#4   e  1
#5   f  1
#6   g  1
#6   g  1
#7   h  1
#7   h  1
#8   i  1
#9   j  1
#9   j  1
#10  k  1
#11  l  1
#11  l  1
#12  m  1
#13  n  1
#13  n  1
#14  o  1
#14  o  1
#14  o  1

#https://stackoverflow.com/questions/65853995/pandas-add-rows-based-on-column-value