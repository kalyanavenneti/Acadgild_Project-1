
# coding: utf-8

# In[1]:


#calling the following libraries
import pandas as pd
import numpy as np 
import os


# In[2]:


#Reading and storing in variable df
df = pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
df.head(2)


# In[3]:


df1 =pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
df1.head(2)


# In[ ]:


# Question 1. Get the Metadata from the above files.


# In[4]:


#df.info provides the Metada from the above files
df.info()


# In[5]:


df1.info()


# In[ ]:


# Question 2. Get the row names from the above files.


# In[6]:


index_lst1 =list(df.index)                               # initially obtaining the index in list form
row_names_df = np.array(index_lst1,np.dtype)             # then in this step converting it to np array
row_names_df


# In[54]:


index_lst1 =list(df1.index)
row_names_df1 = np.array(index_lst1,np.dtype)
row_names_df1


# In[ ]:


# Question 3. Change the column name from any of the above file.


# In[24]:


df.columns #this gives the list of all the available columns in dataframe


# In[8]:


#by following method a single column or multiple columns can be renamed
df.columns = ['Indicator_id', 'Publication Status', 'Year', 'WHO Region',
       'World Bank income grp', 'Country', 'Sex', 'Display Value', 'Numeric',
       'Low', 'High', 'Comments']
 
df.head(2)                


# In[ ]:


# Question 4. Change the column name from any of the above file and store the changes made permanently.


# In[9]:


#inplace function helps to make the change in dataframe permanently
df.rename(columns={'Indicator':'Indicator_id'},inplace =True)
df.head(2)  


# In[ ]:


# Question 5. Change the names of multiple columns.


# In[10]:


## in this way either one, or multiple columns names can be renamed
df.columns = ['Indicator_id', 'Publication Status', 'Year', 'WHO Region',
       'World Bank income grp', 'Country', 'Sex', 'Display Value', 'Numeric',
       'Low', 'High', 'Comments']
df.head(2)


# In[ ]:


# Question 6. Arrange values of a particular column in ascending order.


# In[21]:


## sort_values is used to sort the columns 
df.sort_values(by = ['Year'],ascending =[True])


# In[ ]:


# Question 7. Arrange multiple column values in ascending order.


# In[22]:



df.sort_values(by =['Indicator_id','Country','Year'], ascending =[True, True, True]).head(3)


# In[ ]:


# Question 8. Make country as the first column of the dataframe.


# In[14]:


#by following way index can be named by any column name
df.index = df.Country
df.head(2)


# In[ ]:


# Question 9. Get the column array using a variable Expected Output:


# In[24]:


df.values


# In[ ]:


# Question 10. Get the subset rows 11, 24, 37


# In[20]:


#loc function helps to get the particular index data
df.loc[[11,24,37],:]


# In[ ]:


# Question 11. Get the subset rows excluding 5, 12, 23, and 56


# In[28]:


excludedRows = df.index.isin([5,12,23,34,56])
df[~excludedRows]


# In[30]:



users= pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv' )
users


# In[31]:


sessions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv' )


# In[32]:



products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv' )


# In[33]:



transactions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv') 


# In[34]:


users.head()


# In[35]:


sessions.head() 


# In[36]:


transactions.head()


# In[ ]:


# Question 12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)


# In[37]:


#using merge function and merging on UserId as Key
df2 = pd.merge(transactions,users, on = 'UserID', how ='left')
df2


# In[ ]:


# Question 13. Which transactions have a UserID not in users?


# In[40]:


transactions[~transactions['UserID'].isin(users['UserID'])]


# In[ ]:


# Question 14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)


# In[38]:


df3 = pd.merge(transactions,users, on ='UserID')
df3


# In[ ]:


# Question 15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)


# In[39]:


df4 = pd.merge(transactions,users, on ='UserID', how ='outer')
df4


# In[ ]:


# Question 16. Determine which sessions occurred on the same day each user registered


# In[41]:


pd.merge(sessions,users, on='UserID', how='inner') 


# In[42]:


sameDay_User_Reg=pd.merge(sessions,users, on='UserID', how='inner')
sameDay_User_Reg


# In[43]:


sameDay_User_Reg.loc[sameDay_User_Reg['SessionDate'] == sameDay_User_Reg['Registered']]


# In[ ]:


# Question 17. Build a dataset with every possible (UserID, ProductID) pair (cross join)


# In[44]:


possibleDataSet = users.assign(value=1).merge(products.assign(value=1)).drop('value', 1)
possibleDataSet


# In[ ]:


# Question 18. Determine how much quantity of each product was purchased by each user


# In[45]:


transactions.sort_values('Quantity')


# In[ ]:


# Question 19. For each user, get each possible pair of pair transactions (TransactionID1, TransacationID2)


# In[46]:


pd.merge(transactions, transactions, on='UserID')


# In[ ]:


# Question 20. Join each user to his/her first occuring transaction in the transactions table


# In[49]:


data =pd.merge(users, transactions.groupby('UserID').first().reset_index(), how='left', on='UserID')
data


# ##21. Test to see if we can drop columns

# In[50]:


my_columns = list(data.columns) 
my_columns


# In[51]:


list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns)


# In[52]:


missing_info = list(data.columns[data.isnull().any()]) 
missing_info


# In[53]:


for col in missing_info:
    num_missing = data[data[col].isnull() == True].shape[0] 
    print('number missing for column {}: {}'.format(col, num_missing))


# In[55]:


for col in missing_info:
    percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0] 
    print('percent missing for column {}: {}'.format( col, percent_missing))

