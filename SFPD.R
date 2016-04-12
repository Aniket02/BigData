#install.packages("rJava")
#install.packages("RJDBC",dep=TRUE) 
#install.packages("DBI")

library(rJava)
library(RJDBC)
library(DBI)


### setup JAR librarie

.jinit()

for(l in list.files('/usr/lib/hive/lib/')){ .jaddClassPath(paste("/usr/lib/hive/lib/",l,sep=""))}
for(l in list.files('/usr/lib/hadoop/')){ .jaddClassPath(paste("/usr/lib/hadoop/",l,sep=""))}
for(l in list.files('/usr/lib/hadoop/lib/')){ .jaddClassPath(paste("/usr/lib/hadoop/lib/",l,sep=""))}

.jclassPath()

### Setup JDBC driver and connect to Hive from R
drv <- JDBC("org.apache.hive.jdbc.HiveDriver", "/usr/lib/hive/lib/hive-jdbc-0.10.0-cdh4.7.1.jar")
conn <- dbConnect(drv, "jdbc:hive2://localhost:10000/insofe_hivedb", "",  "")

### TASK 1: the crime frequency in each category
task1 <- dbGetQuery(conn, "select s.category, count(*) as nb_incidents from sfpd_incidents s group by s.category")
task1

### TASK 2: the crime frequency as a function of day of week
nbCrimeByDOW <- function(dow) 
{
  sql = paste(c("select count(*) as nb_incidents from sfpd_incidents where day_of_week='",dow,"'"),collapse="")
  result = dbGetQuery(conn, sql)
  return(result)
}

thedow = "Saturday"
task2 = nbCrimeByDOW(thedow)
task2
	
### TASK 3: the crime frequency as a function of hour of the day
nbCrimeByHour <- function(h) 
{
  sql = paste(c("select count(*) as nb_incidents from sfpd_incidents where time like '",h,"%'"),collapse="")
  result = dbGetQuery(conn, sql)
  return(result)
}

thehour = "23"
task3 = nbCrimeByHour(thehour)
task3

#################################################
### TASK 4: Regression
sfpd <- dbGetQuery(conn, "select * from sfpd_incidents")
str(sfpd)
#PdId and Incident# are unique for each entity, and therefore, will be removed
#Decision tree will overfit if they are included
sfpd$PdId <- as.factor(sfpd$PdId)
sfpd1 <- sfpd[, c(-1,-13)]
str(sfpd1)
#Location is removed as it's too specific, X & Y values are binned through rounding
sfpd1 <- sfpd1[, c(-11)]
sfpd2<- sfpd1
sfpd2$X <- round(sfpd2$X, digits = 3)
sfpd2$Y <- round(sfpd2$Y, digits = 3)
sfpd2$X <- as.factor(sfpd2$X)
sfpd2$Y <- as.factor(sfpd2$Y)
str(sfpd2)
#Address is again being too specific, and hence, removed
#sfpd2 <- sfpd2[,c(-5,-6)]
#Time is rounded to the hour
sfpd2$Time <- substring(as.character(sfpd2$Time),1,2)
sfpd2$Time <- as.factor(sfpd2$Time)
#Data split into training and testing datasets
rows=seq(1,nrow(sfpd2),1)
set.seed(123)
trainRows_sfpd2=sample(rows,(70*nrow(sfpd2))/100)
train_sfpd2 = sfpd2[trainRows_sfpd2,]
test_sfpd2 = sfpd2[-trainRows_sfpd2,] 
#install.packages("C50")
library(C50)
#Creating a Decision Tree using the C5.0 algorithm, category being the predicted variable
C50 <- C5.0(Category~.,data=train_sfpd2,rules=T)
# predict the values
preds_C50_train = predict(C50, train_sfpd2, type="class")
preds_C50_test = predict(C50, test_sfpd2, type="class")
confmat_C50_train = table(train_sfpd2$Category, predict(C50, train_sfpd2,
                                                        type="class"))
confmat_C50_test = table(test_sfpd2$Category, predict(C50, test_sfpd2,
                                                      type="class"))
#Accuracy computation
accuracy_C50_train = round((sum(diag(confmat_C50_train))/sum(confmat_C50_train))* 100,2)
accuracy_C50_test = round((sum(diag(confmat_C50_test))/sum(confmat_C50_test))*100,2)
C50
summary(C50)
summary(confmat_C50_train)
#If all the attributes are included, except for Location, PDID, and incident #,
#the training accuracy is 99.85%, and test accuracy is 99.48%. The high accuracy is mainly
#due to the descriptions, which give a clear indication of the category. 

#But Let's say, we want to predict which category an incident belongs to
#only from knowing the X & Y coordinates, DayofWeek, Time(rounded by hour), and PDdistrict it has occured in
#We will remove the address as it's too specific, and exact date will be of no use for future predictions
#We will also remove the descriptions, and resolution, as those factors can only be
#known once police reaches the crime. 
sfpd3 <- sfpd2[,c(-2,-4,-7,-8)]
#Data split into training and testing datasets
rows=seq(1,nrow(sfpd3),1)
set.seed(123)
trainRows_sfpd3=sample(rows,(70*nrow(sfpd3))/100)
train_sfpd3 = sfpd3[trainRows_sfpd3,]
test_sfpd3 = sfpd3[-trainRows_sfpd3,] 
#install.packages("C50")
library(C50)
#Creating a Decision Tree using the C5.0 algorithm, category being the predicted variable
C50_1 <- C5.0(Category~.,data=train_sfpd3,rules=T)
# predict the values
preds_C50_1_train = predict(C50_1, train_sfpd3, type="class")
preds_C50_1_test = predict(C50_1, test_sfpd3, type="class")
confmat_C50_1_train = table(train_sfpd3$Category, predict(C50_1, train_sfpd3,
                                                          type="class"))
confmat_C50_1_test = table(test_sfpd3$Category, predict(C50_1, test_sfpd3,
                                                        type="class"))
#Accuracy computation
accuracy_C50_1_train = round((sum(diag(confmat_C50_1_train))/sum(confmat_C50_1_train))* 100,2)
accuracy_C50_1_test = round((sum(diag(confmat_C50_1_test))/sum(confmat_C50_1_test))*100,2)
summary(C50_1)
summary(confmat_C50_1_train)
#The training accuracy reduced drastically to 39.18%, and testing accuracy to 29.92%, but this model
#helps predict the category just based on police district, weekday, and X,Y coordinates; the information
#one is likely to have first of a future incident.