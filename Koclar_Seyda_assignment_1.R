### Data Processing in R and Python 2022Z
### Homework Assignment no. 1
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
###
### Report should include:
### * source() of this file at the beggining,
### * reading the data, 
### * attaching the libraries, 
### * execution time measurements (with microbenchmark),
### * and comparing the equivalence of the results,
### * interpretation of queries.

# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#

sqldf_1 <- function(Posts){
  # sqldf package allows us to run sql queries exactly the same by providing them as strings
  result <- sqldf("
    SELECT STRFTIME('%Y', CreationDate) AS Year, COUNT(*) AS TotalNumber
    FROM Posts
    GROUP BY Year
  ")
  
  return(result)
}

base_1 <- function(Posts){
  # convert date format
  Posts$CreationDate <- as.Date(Posts$CreationDate)
  
  # using format func format creation date column with year then put it into table funtion to create frequency table
  # based on Year value and then convert it back to data frame with as.data.frame
  result <- as.data.frame(table(format(Posts$CreationDate, "%Y")))
  
  # rename the columns with colnames
  colnames(result) <- c("Year", "TotalNumber")
  
  return(result)
}

dplyr_1 <- function(Posts){
  # convert date format
  Posts$CreationDate <- as.Date(Posts$CreationDate)
  
  # execute the query
  result <- Posts %>%
    # group the records by Year and format the column including the Year information and name it as Year
    group_by(Year = format(CreationDate, "%Y")) %>%
    # using "summarise" we can collapse the groups into row and name this column as total number
    # .groups = "drop" is added to increase the performance
    summarise(TotalNumber = n(), .groups = "drop")

    # return after converting to data.frame so that the resulting objects will be equivalent.
    return(as.data.frame(result))
}

data.table_1 <- function(Posts){
  # convert date format
  Posts$CreationDate <- as.Date(Posts$CreationDate)
  
  # take the data into another variable to modify it
  result <- setDT(Posts)
 
  # execute the query
  # use .N to count the rows in each group and .() creates new column named Total number, the data is grouped 
  # with using by and again a column Year crated while this grouping is done
  result <- result[, .(TotalNumber = .N), by = .(Year = format(CreationDate, "%Y"))]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#

sqldf_2 <- function(Users, Posts){
  # sqldf package allows us to run sql queries exactly the same by providing them as strings
  result <- sqldf("
    SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
    FROM Users
    JOIN (
      SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
    ) AS Questions
    ON Users.Id = Questions.OwnerUserId
    GROUP BY Id
    ORDER BY TotalViews DESC
    LIMIT 10")
  return(result)
  
}

base_2 <- function(Users, Posts){
  # execute the innermost query and get questions by filtering the Post data based on PostTypeId
  Questions <- Posts[Posts$PostTypeId == 1, c("OwnerUserId", "ViewCount")]
  
  # Merge Users and questions data frames (join them by Id = OwnerUserId) and do not count NA values
  MergedFrame <- merge(Users, Questions, by.x = "Id", by.y = "OwnerUserId", all.x = 0)
  
  # Calculate total views by user with using aggregate() providing formula 
  TotalViews <- aggregate(ViewCount ~ Id + DisplayName, data = MergedFrame, sum)
  
  # Order and select top 10 rows
  result <- TotalViews[order(-TotalViews$ViewCount), ][1:10, ]
  
  # resetting the rownames from 1 to 10 for equivalence
  rownames(result) <- NULL
  
  # renaming the last column as totalviews
  colnames(result)[colnames(result) == "ViewCount"] <- "TotalViews"
  
  return(result)
}

dplyr_2 <- function(Users, Posts){
  # execute the innermost query and get questions by filtering the Post data based on PostTypeId
  Questions <- Posts %>%
    filter(PostTypeId == 1) %>%
    select(OwnerUserId, ViewCount) 

  # do inner_join users with questions on user.id = questions.owneruserid then do group_by and
  # with summarise get the sum of viewcounts as totalviews, after these arrange table based on descending 
  # value of totalviews and with head get the first 10 value
  result <- Users %>%
    inner_join(Questions, by = c("Id" = "OwnerUserId")) %>%
    group_by(Id, DisplayName) %>%
    summarise(TotalViews = sum(ViewCount), .groups = "drop") %>%
    arrange(desc(TotalViews)) %>%
    head(n = 10)
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

data.table_2 <- function(Users, Posts){
  # convert posts and users to data.table format
  posts <- setDT(Posts)
  users <- setDT(Users)
  
  # execute the innermost query and get questions by filtering the Post data based on PostTypeId
  Questions <- posts[PostTypeId == 1, .(OwnerUserId, ViewCount)]
  
  # do inner_join users with questions on user.id = questions.owneruserid with nomatch option as 0 since
  # we dont want values with NA, which are not macthing values hence filled with NA, then
  # get the sum of viewcounts as totalviews, after group by (id, displayname), order it descending based on 
  # value of totalviews and with 1:10 get first 10 records
  result <- users[Questions, on = .(Id = OwnerUserId), nomatch = 0][
                     , .(TotalViews = sum(ViewCount)), 
                     by = .(Id, DisplayName)][
                       order(-TotalViews)][
                         1:10]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#

sqldf_3 <- function(Badges){
  # sqldf package allows us to run sql queries exactly the same by providing them as strings
  result <- sqldf("
  SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
  FROM (
    SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
    FROM (
      SELECT Name, COUNT(*) AS Count, STRFTIME('%Y', Badges.Date) AS Year
      FROM Badges
      GROUP BY Name, Year
    ) AS BadgesNames
    JOIN (
      SELECT COUNT(*) AS CountTotal, STRFTIME('%Y', Badges.Date) AS Year
      FROM Badges
      GROUP BY YEAR
    ) AS BadgesYearly
    ON BadgesNames.Year = BadgesYearly.Year
  )
  GROUP BY Year")
  
  return(result)
}

base_3 <- function(Badges){
  # copy Badges to badges since the changes will effect Badges if we do not do this
  badges <-Badges
  
  # convert the format of date as year
  badges$Year <- format(as.Date(Badges$Date), "%Y")
  
  # with the help of FUN=length calculate the count of badges in each year and store it in Class column since it will not be used
  BadgesCounts <- aggregate(Class ~ Year + Name, data = badges, FUN = length)
  
  # with the help of FUN=sum calculate total badge counts per year and store it in Class column
  TotalBadgesCounts <- aggregate(Class ~ Year, data = BadgesCounts, FUN = sum)
  
  # join badge counts and total badge counts on year and change column name as classtotal
  MergedData <- merge(BadgesCounts, TotalBadgesCounts, by = "Year", suffixes = c("", "Total"))
  
  # calculate maximum percentage for each year and add it as a new column
  MergedData$MaxPercentage <- with(MergedData, (Class *1.0 / ClassTotal))
  
  # find maximum percentage for each year
  MaxPercentRows <- aggregate(MaxPercentage ~ Year, data = MergedData, FUN = max)
  
  # merge them again since aggregate makes column Name disappear
  result <- merge(MaxPercentRows, MergedData, by = c("Year", "MaxPercentage"))
  
  # pick the wanted columns
  result <- result[, c("Year", "Name", "MaxPercentage")]
  
  return(result)
}

dplyr_3 <- function(Badges){
  
  BadgesNames <- Badges %>%
    # tranform date as year
    mutate(Year = format(as.Date(Date), "%Y")) %>%
    # group by first name then year
    group_by(Name, Year) %>%
    # get the count of the names in each year
    summarise(Count = n())
  
  BadgesYearly <- Badges %>%
    # transform date as year
    mutate(Year = format(as.Date(Date), "%Y")) %>%
    #group by year
    group_by(Year) %>%
    # get the count of badges in each year
    summarise(CountTotal = n())

  result <- BadgesNames %>%
    # join the two data on year value
    inner_join(BadgesYearly, by = "Year") %>%
    # group by year 
    group_by(Year) %>%
    # calculate maxpercentage and add as column
    mutate(MaxPercentage = (Count * 1.0) / CountTotal) %>% 
    # filter the row where we have max value among this maxpercentage column
    filter(row_number() == which.max(MaxPercentage)) %>%
    # select 3 columns that we need
    select(Year, Name, MaxPercentage) %>%
    # order by year
    arrange(Year)
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

data.table_3 <- function(Badges){
  # convert badges to data.table format
  badges <- setDT(Badges)
  
  # group badges by Name and Year and define Year as the formatted version of Date while counting the names for each year
  BadgesNames <- badges[, .(Count = .N), by = .(Name, Year = format(as.Date(Date), "%Y"))]
  
  #group badges by Year and define Year as the formatted version of Date while counting the badges for each year
  BadgesYearly <- badges[, .(CountTotal = .N), by = .(Year = format(as.Date(Date), "%Y"))]
  
  result <-  BadgesNames[
    # join badgesnames with badgesyearly on year
    BadgesYearly, on = "Year"
    # calculate max percentage
  ][, MaxPercentage := (Count * 1.0) / CountTotal
    # select year,name and max percentage
  ][, .(Year, Name, MaxPercentage)
    # filter the max of max percentage column for each year
  ][, .SD[which.max(MaxPercentage)], by = Year
    # order by Year
  ][order(Year)]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#

sqldf_4 <- function(Comments, Posts, Users){
  # sqldf package allows us to run sql queries exactly the same by providing them as strings
  # queries broke down into parts because sqldf could not finish processing when it was only a one query
  CmtTotScr <- sqldf("SELECT PostId, SUM(Score) AS CommentsTotalScore
        FROM Comments
        GROUP BY PostId")
  PostsBestComments <- sqldf("SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount, CmtTotScr.CommentsTotalScore
      FROM CmtTotScr
      JOIN Posts ON Posts.Id = CmtTotScr.PostId
      WHERE Posts.PostTypeId=1")
  
  result <- sqldf("SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
    FROM PostsBestComments
    JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
    ORDER BY CommentsTotalScore DESC
    LIMIT 10")
  return(result) 
}

base_4 <- function(Comments, Posts, Users){
  # get the total score by summing up values in score column for each post
  CmtTotScr <- aggregate(Score ~ PostId, data = Comments, FUN = sum)
  # rename the columns as PostID and CommentsTotalScore
  colnames(CmtTotScr) <- c("PostId", "CommentsTotalScore")
  
  # filter Posts with PostTypeId == 1 with subset() and join with CommentsTotalScore
  PostsBestComments <- merge(subset(Posts, PostTypeId == 1), CmtTotScr, by.x = "Id", by.y = "PostId")
  # filter by selecting the wanted columns in the task
  PostsBestComments <- subset(PostsBestComments, select = c(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore))
  
  # join with Users data
  result <- merge(PostsBestComments, Users, by.x = "OwnerUserId", by.y = "Id")
  # select the wanted columns in the task
  result <- subset(result, select = c(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location))
  
  # order by CommentsTotalScore in descending order
  result <- result[order(result$CommentsTotalScore, decreasing = TRUE), ]
  # select top 10
  result <- result[1:10, ]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

dplyr_4 <- function(Comments, Posts, Users){
  
  # execute on comments
  CmtTotScr <- Comments %>%
    # group by postid
    group_by(PostId) %>%
    # get a column by summing score values for each post and named as CommentsTotalScore
    summarise(CommentsTotalScore = sum(Score))
  
  # join cmttotscr with post on PostId = Id and filter posts with posttypeid = 1
  PostsBestComments <- inner_join(CmtTotScr, Posts %>% filter(PostTypeId == 1), by = c("PostId" = "Id")) %>%
    # select the wanted columns
    select(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore)
 
   result <- PostsBestComments %>%
    # join previous table with users on OwnerUserId = Id
    inner_join(Users, by = c("OwnerUserId" = "Id")) %>%
    # select the wanted columns
    select(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location) %>%
    # order by comments total score descending
    arrange(desc(CommentsTotalScore)) %>%
    # get the top 10
    head(10)
   
   # return after converting to data.frame so that the resulting objects will be equivalent.
   return(as.data.frame(result))
}

data.table_4 <- function(Comments, Posts, Users){
  # convert comments to data.table format
  comments <- setDT(Comments)
  
  # convert posts to data.table format
  posts <- setDT(Posts)
  
  # convert users to data.table format
  users <- setDT(Users)
  
  # group comments by postid and get summation of score for each post id stoe it as comments total score
  CmtTotScr <- comments[, .(CommentsTotalScore = sum(Score)), by = .(PostId)]
  
  # join precious result with posts and filter the ones having posttypeid = 1
  PostsBestComments <- CmtTotScr[posts[PostTypeId == 1], on = .(PostId = Id), nomatch = 0
  #select the columns
  ][, .(OwnerUserId, Title, CommentCount, ViewCount, CommentsTotalScore)]
  
  # join previous result with users 
  result <- PostsBestComments[users, on = .(OwnerUserId = Id)
  #select the columns                            
  ][, .(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)
  # order by CommentsTotalScore descending (- sign is for desc)and then select top 10
  ][order(-CommentsTotalScore)][1:10]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#

sqldf_5 <- function(Posts, Votes){
  # sqldf package allows us to run sql queries exactly the same by providing them as strings
  # sql query broked down into part because when it is just one query the system could not finish processing
  VotesDates <- sqldf("SELECT PostId,
        CASE STRFTIME('%Y', CreationDate)
          WHEN '2022' THEN 'after'
          WHEN '2021' THEN 'during'
          WHEN '2020' THEN 'during'
          WHEN '2019' THEN 'during'
          ELSE 'before'
          END VoteDate, COUNT(*) AS Total
        FROM Votes
        WHERE VoteTypeId IN (3, 4, 12)
        GROUP BY PostId, VoteDate")
  
  VotesByAge <- sqldf("SELECT PostId, MAX(CASE WHEN VoteDate = 'before' THEN Total ELSE 0 END) BeforeCOVIDVotes, MAX(CASE WHEN VoteDate = 'during' THEN Total ELSE 0 END) DuringCOVIDVotes, MAX(CASE WHEN VoteDate = 'after' THEN Total ELSE 0 END) AfterCOVIDVotes, SUM(Total) AS Votes
      FROM VotesDates
      GROUP BY VotesDates.PostId")
  
  result <- sqldf("
    SELECT Posts.Title, STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, VotesByAge.*
    FROM Posts
    JOIN VotesByAge ON Posts.Id = VotesByAge.PostId
    WHERE Title NOT IN ('') AND DuringCOVIDVotes > 0
    ORDER BY DuringCOVIDVotes DESC, Votes DESC
    LIMIT 20")
  return(result)
}

base_5 <- function(Posts, Votes){
  # get the subset of votes where VoteTypeId is in (3,4,12)
  VotesDates <- subset(Votes, VoteTypeId %in% c(3, 4, 12))
  
  # convert to Date format
  VotesDates$CreationDate <- as.Date(VotesDates$CreationDate)  
  
  # arrange VoteDate column as after, during, before based on the year value of the CreationDate column
  VotesDates$VoteDate <- ifelse(
    as.numeric(format(VotesDates$CreationDate, "%Y")) == 2022, "after",
    ifelse(as.numeric(format(VotesDates$CreationDate, "%Y")) %in% c(2019, 2020, 2021), "during", "before")
  )
  
  # count the values based on each post store it in VoteTypeId column
  VotesDates <- aggregate(VoteTypeId ~ PostId + VoteDate, data = VotesDates, FUN = length)
  
  # change the votetypeid column to Total
  colnames(VotesDates)[3] <- "Total"
  
  # create unique PostIds vector from PostId column in VotesDates
  postIds <- unique(VotesDates$PostId)
  
  # Initialize empty vectors to store results with length of the number of postids in the postIds vector
  beforeCOVIDVotes <- numeric(length(postIds))
  duringCOVIDVotes <- numeric(length(postIds))
  afterCOVIDVotes <- numeric(length(postIds))
  totalVotes <- numeric(length(postIds))
  
  # Iterate through each PostId and calculate the values
  for (i in seq_along(postIds)) {
    # get the PostId
    postId <- postIds[i]
    # get the specific record belonging to the PostId
    subsetVotes <- VotesDates[VotesDates$PostId == postId, ]
    #calculate the maximum number of votes received before the COVID period for a specific post
    beforeVotesCount <- sum(subsetVotes$Total[subsetVotes$VoteDate == "before"])
    
    # this part is added to prevent calculation mistake because of NA values
    if (beforeVotesCount > 0) {
      maxBeforeVotes <- max(subsetVotes$Total[subsetVotes$VoteDate == "before"])
      beforeCOVIDVotes[i] <- maxBeforeVotes
    } else {
      beforeCOVIDVotes[i] <- 0
    }
    
    #calculate the maximum number of votes received during the COVID period for a specific post
    duringVotesCount <- sum(subsetVotes$Total[subsetVotes$VoteDate == "during"])
    
    # this part is added to prevent calculation mistake because of NA values
    if (duringVotesCount > 0) {
      maxDuringVotes <- max(subsetVotes$Total[subsetVotes$VoteDate == "during"])
      duringCOVIDVotes[i] <- maxDuringVotes
    } else {
      duringCOVIDVotes[i] <- 0
    }
    
    #calculate the maximum number of votes received after the COVID period for a specific post
    afterVotesCount <- sum(subsetVotes$Total[subsetVotes$VoteDate == "after"])
    
    # this part is added to prevent calculation mistake because of NA values
    if (afterVotesCount > 0) {
      maxAfterVotes <- max(subsetVotes$Total[subsetVotes$VoteDate == "after"])
      afterCOVIDVotes[i] <- maxAfterVotes
    } else {
      afterCOVIDVotes[i] <- 0
    }
    # assign totalvotes value as summation of Total in subsetVotes 
    totalVotes[i] <- sum(subsetVotes$Total)
  }
  
  # create a data frame with the results
  VotesByAge <- data.frame(
    PostId = postIds,
    BeforeCOVIDVotes = beforeCOVIDVotes,
    DuringCOVIDVotes = duringCOVIDVotes,
    AfterCOVIDVotes = afterCOVIDVotes,
    Votes = totalVotes
  )
  
  # join posts with votesbyage
  result <- merge(Posts, VotesByAge, by.x = "Id", by.y = "PostId", all.x = TRUE)
  
  # filter the ones with a title and duringcovidvotes value greater than zero
  result <- result[result$Title != "" & result$DuringCOVIDVotes > 0, ]
  # order the values based on duringcovidvotes and votes, both are descending
  result <- result[order(-result$DuringCOVIDVotes, -result$Votes), ]
  
  # create column date as formatted version of CreationDate
  result$Date <- format(as.Date(result$CreationDate), "%Y-%m-%d")
  # select the desired columns
  result <- result[, c("Title", "Date", "Id", "BeforeCOVIDVotes", "DuringCOVIDVotes", "AfterCOVIDVotes", "Votes")]
  # arrange the column names
  colnames(result) <- c("Title", "Date", "PostId", "BeforeCOVIDVotes", "DuringCOVIDVotes", "AfterCOVIDVotes", "Votes")
  
  # get the top 20 records
  result <- result[1:20, ]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
  
}

dplyr_5 <- function(Posts, Votes){
  #execute on votes
  VotesDates <- Votes %>%
    #filter the ones with votetypeid is 3 or 4 or 12
    filter(VoteTypeId %in% c(3, 4, 12)) %>%
    #group by Post Id and VoteDate
    group_by(PostId, VoteDate = case_when(
      #create Votedate make it after if the creation date is 2022
      year(CreationDate) == 2022 ~ "after",
      #make it during if it is one of the values (2019, 2020, 2021)
      year(CreationDate) %in% c(2019, 2020, 2021) ~ "during",
      #otherwise make it before
      TRUE ~ "before"
    )) %>%
    #get the total value with count(n()), drop the grouping(prevents warning message)
    summarise(Total = n(), .groups = "drop")
  
  # execute on VotesDates
  VotesByAge <- VotesDates %>%
    #group by Postid
    group_by(PostId) %>%
    # summarise the data inside it
    summarise(
      #checkif votedate is before then get value of total column otherwise get 0 then get the max value of this vector assign it to the variable
      BeforeCOVIDVotes = max(ifelse(VoteDate == "before", Total, 0)),
      #checkif votedate is during then get value of total column otherwise get 0 then get the max value of this vector assign it to the variable
      DuringCOVIDVotes = max(ifelse(VoteDate == "during", Total, 0)),
      #checkif votedate is after then get value of total column otherwise get 0 then get the max value of this vector assign it to the variable
      AfterCOVIDVotes = max(ifelse(VoteDate == "after", Total, 0)),
      #get all votes count with summing Total values
      Votes = sum(Total)
    )
  
  result <- Posts %>%
    #join posts with votesbyage and keep the PostId column in the resulting table with keep=TRUE
    inner_join(VotesByAge, by = c("Id" = "PostId"), keep=TRUE) %>%
    #filter the ones with title is not empty and duringcovidvotes value is greater than 0
    filter(!Title %in% '', DuringCOVIDVotes > 0) %>%
    #format the CreationDate value as Year- month- date and put it into Date column
    mutate(Date = format(as.Date(CreationDate), "%Y-%m-%d")) %>%
    # order by DuringCOVIDVotesand Votes both descending
    arrange(desc(DuringCOVIDVotes), desc(Votes)) %>%
    # select the wanted columns
    select(Title, Date, all_of(names(VotesByAge))) %>%
    # get the first 20 records
    head(20)
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

data.table_5 <- function(Posts, Votes){
  # convert posts to data.table format  
  posts <- setDT(Posts)
  # convert votes to data.table format
  votes <- setDT(Votes)
  
  # filter the votes where votetypeid is one of the values 3,4 or 12  
  VotesDates <- votes[VoteTypeId %in% c(3, 4, 12), ][, VoteDate := case_when(
    # if the year of creation date is 2022 set votedate after
   year(CreationDate) == 2022 ~ "after",
   # if is in (2019, 2020,2021) set as during
   year(CreationDate) %in% c(2019, 2020, 2021) ~ "during",
   # otherwise set as before
   TRUE ~ "before"
  )][, .(
    #count total vote ie before+after+during
   Total = .N
   #groupby postid anda vote date
  ), by = .(PostId, VoteDate)]
  
  
  VotesByAge <- VotesDates[, .(
  #checkif votedate is before then get value of total column otherwise get 0 then get the max value of this vector assign it to the variable
   BeforeCOVIDVotes = max(as.integer(ifelse(VoteDate == "before", Total, 0))),
   #checkif votedate is during then get value of total column otherwise get 0 then get the max value of this vector assign it to the variable
   DuringCOVIDVotes = max(as.integer(ifelse(VoteDate == "during", Total, 0))),
   #checkif votedate is after then get value of total column otherwise get 0 then get the max value of this vector assign it to the variable
   AfterCOVIDVotes = max(as.integer(ifelse(VoteDate == "after", Total, 0))),
   #get all votes count with summing Total values
   Votes = sum(Total)
   #group by postid, so the operations above will be about each post
  ), by = PostId]
  
  #join posts with votesbyage on Id = PostId
  result <- posts[VotesByAge, on = .(Id = PostId), nomatch = 0][
    #get the records where title is not empty and duringcovidvotes value is greater than 0
    !Title %in% '' & DuringCOVIDVotes > 0,
    #select the columns
   .(Title, Date = as.Date(CreationDate), PostId = Id, BeforeCOVIDVotes, DuringCOVIDVotes, AfterCOVIDVotes, Votes)
   #order by during covidvotes and votes both decending then get the first 20 records
  ][order(-DuringCOVIDVotes, -Votes)][1:20]
  
  # return after converting to data.frame so that the resulting objects will be equivalent.
  return(as.data.frame(result))
}

