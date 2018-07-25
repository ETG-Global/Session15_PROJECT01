
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from urllib.request import urlretrieve
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

# Assign url of file: url
url1 = 'https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv'

# Save file locally
urlretrieve(url1, 'jackiekazil.csv')

df = pd.read_csv('jackiekazil.csv', sep=',')
df.head(2)


# In[2]:


# Assign url of file: url
url2 = 'https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv'

# Save file locally
urlretrieve(url2, 'kjam.csv')

df1 = pd.read_csv('kjam.csv', sep=',')
df1.head(2)


# In[3]:


df.info()


# In[4]:


df1.info()


# In[5]:


df.index.values


# In[6]:


df1.index.values


# In[7]:


df.rename(columns = {'Indicator':'Indicator_Id'}).head(2)


# In[8]:


df.rename(columns = {'Indicator':'Indicator_Id'}, inplace=True)


# In[9]:


df.head(2)


# In[10]:


df.rename(columns = {'PUBLISH STATES':'Publication Status', 'WHO region':'WHO Region'}, inplace=True)


# In[11]:


df.head(3)


# In[12]:


df.sort_values('Year').head()


# In[13]:


#df.sort_values('Indicator_Id','Country','Year','WHO Region','Publication Status')
#df.sort_values('Country')
df.sort_values(['Year', 'Country'], ascending=[True, True]).sort_index().head(3)[['Indicator_Id','Country','Year' ,'WHO Region','Publication Status']]


# In[14]:


df.sort_values(['Year', 'Country'], ascending=[True, True]).sort_index().head(3)[['Country']]


# In[15]:


df.head()
cols = ['Country']  + [col for col in df if col != 'Country']
#print(cols)
#df[cols].head()


# In[16]:


df=df[cols]


# In[17]:


df.head()


# In[18]:


var = df.Country.values
var


# In[23]:


df.loc[[11, 24, 37],] #10. Getting the subset rows 11, 24, 37


# In[24]:


x = [5,12,23,56]
df.loc[~df.index.isin(x)] #11. Geting the subset rows excluding 5, 12, 23, and 56


# In[56]:


users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')
users


# In[57]:


sessions


# In[55]:


transactions


# In[46]:


#12.Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)
result1 = pd.merge(transactions,users,on='UserID',how='left')
result1 


# In[58]:


#13. Which transactions have a UserID not in users?
#result2 = pd.merge(transactions,users,left_on='TransactionID',right_on='UserID', how='left')

result2 = transactions[~transactions['UserID'].isin(users['UserID'])]
result2 


# In[60]:


#14.Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)
result3 = pd.merge(transactions,users,on='UserID', how='inner')
result3


# In[61]:


#15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)
result4 = pd.merge(transactions,users,on='UserID', how='outer')
result4


# In[65]:


#16. Determine which sessions occurred on the same day each user registered

result5 = pd.merge(left=users, right=sessions, how='inner', left_on=['UserID', 'Registered'], right_on=['UserID', 'SessionDate'])
result5


# In[67]:


#17. Build a dataset with every possible (UserID, ProductID) pair (cross join)

df1 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df2 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
result6 = pd.merge(df1, df2,on='key')[['UserID', 'ProductID']]
result6.head()


# In[69]:


#18. Determine how much quantity of each product was purchased by each user
df1 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df2 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
user_products = pd.merge(df1, df2,on='key')[['UserID', 'ProductID']]
result7 = pd.merge(user_products, transactions, how='left', on=['UserID', 'ProductID']).groupby(['UserID', 'ProductID']).apply(lambda x: pd.Series(dict(Quantity=x.Quantity.sum()))).reset_index().fillna(0)
result7.head()


# In[71]:


#19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)
result8 = pd.merge(transactions, transactions, on='UserID')
result8.head()


# In[72]:


#20. Join each user to his/her first occuring transaction in the transactions table
result9 = pd.merge(users, transactions.groupby('UserID').first().reset_index(), how='left', on='UserID')
result9


# In[93]:


#21. Test to see if we can drop columns
data=result9
my_columns = list(data.columns)
my_columns
#my_columns[['UserID','User','Gender','Registered','Cancelled','TransactionID','TransactionDate','ProductID','Quantity']]


# In[95]:


result10 = list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns) #set threshold to drop NAs
result10


# In[96]:


missing_info = list(data.columns[data.isnull().any()])
missing_info


# In[98]:


for col in missing_info:
    num_missing = data[data[col].isnull() == True].shape[0]
    print('number missing for column {}:{}'.format(col, num_missing))


# In[101]:


for col in missing_info:
    num_missing = data[data[col].isnull() == True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing)) #count of missing data


# In[102]:


for col in missing_info:
    percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0]
    print('percent missing for column {}: {}'.format(col, percent_missing))

