### Data Processing in R and Python 2022Z
### Homework Assignment no. 2
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
#
# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#


def solution_1(posts):
    
    # Checking the dtype of CreationDate column - if it is not 'datetime', it will be converted.
    if posts.CreationDate.dtype != '<M8[ns]':
        posts['Year'] = posts.CreationDate.astype('datetime64[ns]').dt.strftime('%Y')
    else:
        posts['Year'] = posts.CreationDate.dt.strftime('%Y')
    
    # Make a groupby using 'Year' column
    result = posts.groupby('Year')[['Id']].count()
    # Adjusting the column name like SQL 1 query outcome
    result = result.reset_index().rename(columns={'Id':'TotalNumber'})
    # Dropping the ['Year'] column from the post table
    posts.drop(columns=['Year'], inplace=True) 

    return result

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

def solution_2(users, posts):

    # Creating the dataFrame from posts data where PostypeId column equals 1
    questions = posts[posts.PostTypeId.isin([1])][['OwnerUserId', 'ViewCount']]
    # Joining the questions table to Users data on Id and OwnerUserId columns
    joins = users.merge(questions, how='inner', left_on='Id', right_on='OwnerUserId')
    # Making the groupby on Id column and sorting the data using 'ViewCount' column descending order and limiting the data first 10 rows
    result = joins.groupby(['Id'])[['ViewCount']].sum().sort_values(by='ViewCount', ascending=False).head(10)
    # Renaming the columns as sql_2 query outcome
    result = result.reset_index().rename(columns={'ViewCount':'TotalViews'})
    # Creating the DisplayName column to the result table copying the data from joins table 
    for row_r in result.itertuples():
        for row_j in joins.itertuples():
            if row_r.Id == row_j.Id:
                result.loc[row_r.Index, 'DisplayName'] = joins.loc[row_j.Index, 'DisplayName']
    # Ordering columns as sql_2 outcome
    result = result[['Id', 'DisplayName', 'TotalViews']]

    return result

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

def solution_3(badges):

    # Checking the dtype of Date column - if it is not 'datetime', it will be converted.
    if badges.Date.dtype != '<M8[ns]':
        badges['Year'] = badges.Date.astype('datetime64[ns]').dt.strftime('%Y')
    else:
        badges['Year'] = badges.Date.dt.strftime('%Y')
    # Creating the first inner tables for joining
    badgesnames = badges.groupby(['Name', 'Year'])[['Id']].count()
    badgesnames = badgesnames.reset_index().rename(columns={'Id': 'Count'})
    # Creating the second inner tables for joining
    badgesyearly = badges.groupby(['Year'])[['Id']].count()
    badgesyearly = badgesyearly.reset_index().rename(columns={'Id': 'CountTotal'})
    # Joins the inner tables on year column
    joins = badgesnames.merge(badgesyearly, how='inner', on='Year')
    # Creating the MaxPercentage column
    joins['MaxPercentage'] = (joins.Count / joins.CountTotal)
    # Making goupby on year column to find the max value on year base
    result = joins.groupby(['Year'])[['MaxPercentage']].max()
    # Resetting the index to create a new result dataframe
    result = result.reset_index()
    # Copying the Name information from Joins table to the final result dataframe
    for row_r in result.itertuples():
        for row_j in joins.itertuples():
            if row_r.MaxPercentage == row_j.MaxPercentage:
                result.loc[row_r.Index, 'Name'] = joins.loc[row_j.Index, 'Name']
    # Ordering columns as sql_3 outcome
    result = result[['Year', 'Name', 'MaxPercentage']]

    return result
    
# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

def solution_4(comments, posts, users):
    
    # Creating the inner table from comments table
    cmttotscr = comments.groupby('PostId')[['Score']].sum()
    cmttotscr = cmttotscr.reset_index().rename(columns={'Score': 'CommentsTotalScore'})
    # Joining the inner table with posts data
    joins1 = posts.merge(cmttotscr, how='inner', left_on='Id', right_on='PostId')
    # Filtering the inner joins table whre PostType column equals 1 and selecting the required columns
    postsbestcomments = joins1[joins1.PostTypeId.isin([1])][['OwnerUserId', 'Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore']]
    # Joining the data with users data
    joins2 = postsbestcomments.merge(users, how='inner', left_on='OwnerUserId', right_on='Id')
    # Sorting the data base on CommentsTotal Score column descending order and limiting the first 10 rows
    result = joins2.sort_values(by='CommentsTotalScore', ascending=False).head(10)
    # Selecting the columns as to the sql_4 outcome
    result = result[['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore', 'DisplayName', 'Reputation', 'Location']]
    # Correcting the 'Nan' to None data as like sql_# outcome to be matched with equals method
    import numpy as np
    result.Location = result.Location.replace({np.nan:None})
    # Resetting the index
    result.reset_index(drop=True, inplace=True)

    return result
    
# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#

def solution_5(votes, posts):
    
    # Defining the function to create the VoteDate column using apply function from pandas lib
    def votedate(x):
        if x == '2022':
            return 'after'
        elif x == '2021' or x == '2020' or x == '2019':
            return 'during'
        else:
            return 'before'
    # Cerating the VoteDate column as to defined function votedate with apply function from pandas
    votes['VoteDate'] = votes['CreationDate'].astype('datetime64[ns]').dt.strftime('%Y').apply(votedate)
    # Filtering dataframe as to VoteTypeId containing 3 ,4, 12 
    filtered_votes = votes[votes.VoteTypeId.isin([3, 4, 12])]
    # Making the groupby on PostId and VoteDate to have the number of total count 
    votesdates = filtered_votes.groupby(['PostId', 'VoteDate'])[['Id']].count()
    # Resetting the index to create dataframe for the later use and rename the total count column
    votesdates = votesdates.reset_index().rename(columns={'Id':'Total'})
    # Creating the covid votes number column as to Votedate using the lambda functionality
    votesdates['BeforeCOVIDVotes'] = votesdates.apply(lambda x: x['Total'] if x['VoteDate'] == 'before' else 0, axis=1)
    votesdates['DuringCOVIDVotes'] = votesdates.apply(lambda x: x['Total'] if x['VoteDate'] == 'during' else 0, axis=1)
    votesdates['AfterCOVIDVotes'] = votesdates.apply(lambda x: x['Total'] if x['VoteDate'] == 'after' else 0, axis=1)
    # Having the maximun count for each covidvotes based on PostId column
    max_join = votesdates.groupby('PostId')[['BeforeCOVIDVotes','DuringCOVIDVotes', 'AfterCOVIDVotes']].max()
    # Resetting the index to create a separate dataframe for the result
    max_join.reset_index(inplace=True)
    # Having the total sum count for each PostId
    sum_join = votesdates.groupby('PostId')[['Total']].sum()
    # Resetting the index to create a separate dataframe for the result and rename the total count
    sum_join = sum_join.reset_index().rename(columns={'Total':'Votes'})
    # By joining the max and sum tables to create votesbyage table
    votesbyage = max_join.merge(sum_join,how='inner', on='PostId')
    # Joining the posts table to votesbyage table 
    joins = posts.merge(votesbyage, how='inner', left_on = 'Id', right_on='PostId')
    # Correcting the types of the value to be recognize easliy by our solution
    import numpy as np
    joins['Title'] = joins['Title'].replace({np.nan : None})
    # Filtering the data as not null value on Title column and bigger than 0 on During Covidvotes
    filtered_joins = joins[(~joins.Title.isin([None])) & (joins.DuringCOVIDVotes > 0)]
    # Sorting and limiting the outcome as like sql 5 outcomes
    result = filtered_joins.sort_values(by=['DuringCOVIDVotes', 'Votes'], ascending=False).head(20)[['Title', 'CreationDate', 'PostId', 
    'BeforeCOVIDVotes', 'DuringCOVIDVotes', 'AfterCOVIDVotes', 'Votes']]
    # Resetting the index and renaming the CreationDate as Date
    result = result.reset_index(drop=True).rename(columns={'CreationDate' : 'Date'})
    # Correcting the formating of Date column
    result['Date'] = result['Date'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')

    return result
