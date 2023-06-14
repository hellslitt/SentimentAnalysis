import tweepy
import time
import pandas as pd
import csv

bt= "******************************************"

client = tweepy.Client(bt, wait_on_rate_limit=True)

data=pd.read_csv('C:\\Users\\mamai\\Desktop\\Tesi\\Code\\twitter download\\corona_tweets_1074.csv') #read csv downloaded from IEEE website
data.columns=['ID', 'sentiment']

def merge(data, from_num, to_num):
    """
    - function to retrieve the text that corresponds to some specific 
      tweet IDs
      
    @param    data (pandas dataframe): data retrieved from IEEE.
    @param    from_num (int): index from which start to retrieve the tweets
    @param    to_num (int): index to which start to retrieve the tweets
    @return   merged_data (pandas dataframe): dataframe with three columns, tweet ID, 
              corresponding tweet text, corresponding sentiment score
    """
    data1 = data[from_num:to_num]
    data1.ID = data1.ID.astype(str)

    count = 1 
    text=[]
    id_list=[]

    final_data = {'ID': id_list,
                  'text': text}

    final_df = pd.DataFrame(final_data)

    for i in range(0, len(data1)//100): #we can only retrieve 100 tweets at a time
        id_100 = list(data1['ID'][ 100*count-100 : 100*count]) #list with 100 tweet IDs
        text100 = client.get_tweets(id_100).data  #data related to the 100 tweet IDs, it can happen that some tweets are not retrieved
        for j in range(0,len(text100)):
            text100[j].id = str(text100[j].id)
            id_list.append(text100[j].id)  # list of the tweets IDs that were effectively retrieved
        for j in range(0,len(text100)):
            text.append(text100[j].text)  # list of the corresponding tweet text
        data = {'ID': id_list,       
                'text': text}
        df1 = pd.DataFrame(data) # dataframe with the retrieved 100 tweet IDs with the corresponding text
        final_df = pd.concat([final_df, df1]) # concatenation of all the small dataframes to get the total
        text=[]
        id_list=[]
        count+=1
          
    final_df.reset_index(drop = True, inplace = True)
    merged_data = pd.merge(final_df, data1, on='ID') # merge the total dataframe with the initial to get the corresponding scores
    return merged_data