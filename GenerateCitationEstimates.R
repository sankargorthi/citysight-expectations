# AB: util functions
isInstalled <- function (package) {
  is.element(package, installed.packages()[,1])
}

LoadOrInstallLibraries <- function (packages) {
  for(package in packages) {
    if(!isInstalled(package)) {
      install.packages(package,repos="http://cran.rstudio.com/")
    }
    require(package,character.only=TRUE,quietly=TRUE)
  }
}

GetDBHandle <- function (ct) {
  connector <- paste("driver={SQL Server}", ct$server, ct$db, ct$uid, ct$pwd, sep=";")
  conn <- odbcDriverConnect(connector)

  return(conn)
}

BuildConfig <- function (config, city) {
  ct <- config[[city]]

  server <- paste("server", ct$server, sep="=")
  uid <- paste("uid", ct$username, sep="=")
  pwd <- paste("pwd", ct$password, sep="=")
  db <- paste("database", ct$db, sep="=")

  return(list("server" = server, "uid" = uid, "pwd" = pwd, "db" = db))
}

LoadOrInstallLibraries(c("argparser", "RODBC", "randomForest", "prodlim", "yaml", "devtools", "futile.logger", "data.table", "plyr"))
install_github("ozagordi/weatherData")
library(weatherData)
options(max.print=5)

parser <- arg_parser("Generate Citation Estimates")
parser <- add_argument(parser, "cwd", help="current working directory")
parser <- add_argument(parser, "date", help="date to generate estimates for")
parser <- add_argument(parser, "city", help="label in config.yml for DB credentials")
parser <- add_argument(parser, "targets", help="list of cities to write to")

args <- parse_args(parser, commandArgs(trailingOnly=TRUE))
cwd <- args$cwd
today <- as.Date(args$date, "%Y-%m-%d")
city <- args$city
targets <- strsplit(args$targets, ",")[[1]]


flog.appender(appender.file(paste(cwd, "logs", paste("estimates-", today, ".log", sep=""), sep="\\")), "quiet")
baseConfig <- yaml.load_file(paste(cwd, "config.yml", sep="\\"))
config <- BuildConfig(baseConfig, city)

## Connect to the database
flog.info("Generating estimates for %s", city, name="quiet")
dbhandle <- GetDBHandle(config)

# Query database to get features and combine into one frame
ticketQuery <- paste("SELECT BADGENUMBER, OFFICERNAME, ISSUEDATE, BEATNAME, count(*) AS TICKETCOUNT FROM
    (SELECT I.BADGENUMBER,I.OFFICERNAME,CAST(I.ISSUEDATETIME AS DATE) AS ISSUEDATE, C.GPSBEAT AS BEATNAME
    FROM ISSUANCE I JOIN CORRECTEDBEATS C
    ON I.TICKETNUMBER = C.TICKETNUMBER) A GROUP BY BADGENUMBER, OFFICERNAME, ISSUEDATE, BEATNAME", sep="")
ticketcount <- sqlQuery(dbhandle, ticketQuery)
ticketcount$ISSUEDATE <- as.Date(ticketcount$ISSUEDATE)

featureQuery <- paste("SELECT O.OFFICERID AS BADGENUMBER, O.OFFICERNAME, CAST(O.DATETIME AS DATE) AS DATEBEAT,
    O.RECID AS SESSIONID, O.DATETIME, O.DATETIME2, D.TOTALLENGTH AS SESSIONLENGTH,
    D.PATROLLENGTH, D.SERVICELENGTH, D.OTHERLENGTH FROM OMS_SESSION O JOIN DutyStatusFeats D on O.RECID = D.SESSIONID", sep="")
oms_session_feats <- sqlQuery(dbhandle, featureQuery)
oms_session_feats$DATEBEAT <- as.Date(oms_session_feats$DATEBEAT)

odbcClose(dbhandle)

weather_feats1 <- getSummarizedWeather("DEN", "2014-01-01", end_date = "2014-12-31", station_type = "airportCode", opt_all_columns = TRUE)
weather_feats2 <- getSummarizedWeather("DEN", "2015-01-01", end_date = "2015-12-31", station_type = "airportCode", opt_all_columns = TRUE)
weather_feats3 <- getSummarizedWeather("DEN", "2016-01-01", end_date = today, station_type = "airportCode", opt_all_columns = TRUE)
weather_feats_temp <- rbind(weather_feats1, weather_feats2)
colnames(weather_feats3) <- colnames(weather_feats_temp)
weather_feats <- rbind(weather_feats_temp, weather_feats3)
weather_feats$Date <- as.Date(weather_feats$Date)

ticketcount <- ticketcount[ticketcount$BEATNAME!=0,]
ticketcount <- ticketcount[!grepl(":",ticketcount$BEATNAME),]

combined_feats <- merge(oms_session_feats, weather_feats, by.x = "DATEBEAT", by.y = "Date", all.x=TRUE)

combined_feats_GT <- merge(combined_feats, ticketcount, by.x=c("BADGENUMBER","OFFICERNAME","DATEBEAT"), by.y=c("BADGENUMBER","OFFICERNAME","ISSUEDATE"), all.x=TRUE)

# Prune features and create derived features
combined_feats_GT$BADGENUMBER <- NULL
combined_feats_GT$SESSIONID <- NULL
combined_feats_GT$DATETIME <- NULL
combined_feats_GT$DATETIME2 <- NULL
combined_feats_GT$Max_Dew_PointF <- NULL
combined_feats_GT$Min_DewpointF <- NULL
combined_feats_GT$MeanDew_PointF <- NULL
combined_feats_GT$Max_Humidity <- NULL
combined_feats_GT$Min_Humidity <- NULL
combined_feats_GT$Mean_Humidity <- NULL
combined_feats_GT$Max_Sea_Level_PressureIn <- NULL
combined_feats_GT$Min_Sea_Level_PressureIn <- NULL
combined_feats_GT$Mean_Sea_Level_PressureIn <- NULL
combined_feats_GT$WindDirDegrees <- NULL
combined_feats_GT$MST <- NULL
combined_feats_GT$Min_Wind_SpeedMPH <- NULL
combined_feats_GT$Max_Gust_SpeedMPH <- NULL
combined_feats_GT$Max_Wind_SpeedMPH <- NULL
combined_feats_GT$Max_Gust_SpeedMPH <- NULL
combined_feats_GT$Mean_TemperatureF <- NULL
combined_feats_GT$Mean_VisibilityMiles <- NULL
combined_feats_GT$Max_VisibilityMiles <- NULL
combined_feats_GT$Max_TemperatureF <- NULL
combined_feats_GT$Min_TemperatureF <- NULL
combined_feats_GT$OTHERLENGTH <- NULL
combined_feats_GT$Min_VisibilityMiles <- NULL
combined_feats_GT$Mean_Wind_SpeedMPH <- NULL
combined_feats_GT$CloudCover <- NULL
combined_feats_GT$EAT <- NULL

weather_feats$Max_Dew_PointF <- NULL
weather_feats$Min_DewpointF <- NULL
weather_feats$MeanDew_PointF <- NULL
weather_feats$Max_Humidity <- NULL
weather_feats$Min_Humidity <- NULL
weather_feats$Mean_Humidity <- NULL
weather_feats$Max_Sea_Level_PressureIn <- NULL
weather_feats$Min_Sea_Level_PressureIn <- NULL
weather_feats$Mean_Sea_Level_PressureIn <- NULL
weather_feats$WindDirDegrees <- NULL
weather_feats$MST <- NULL
weather_feats$Min_Wind_SpeedMPH <- NULL
weather_feats$Max_Gust_SpeedMPH <- NULL
weather_feats$Max_Wind_SpeedMPH <- NULL
weather_feats$Max_Gust_SpeedMPH <- NULL
weather_feats$Mean_TemperatureF <- NULL
weather_feats$Mean_VisibilityMiles <- NULL
weather_feats$Max_VisibilityMiles <- NULL

combined_feats_GT$dayOfWeek <- weekdays(combined_feats_GT$DATEBEAT)
combined_feats_GT$monthOfYear <- format(combined_feats_GT$DATEBEAT, "%m")
combined_feats_GT$isWeekend <- factor(ifelse(combined_feats_GT$dayOfWeek=="Sunday" | combined_feats_GT$dayOfWeek=="Saturday",1,0))

# Convert column types to factor/numeric as needed

combined_feats_GT$BEATTYPE <- "----"
combined_feats_GT$BEATTYPE[grep("SAT",combined_feats_GT$BEATNAME, ignore.case = TRUE, value=FALSE)] <- "SAT"
combined_feats_GT$BEATTYPE[grep("SUN",combined_feats_GT$BEATNAME, ignore.case = TRUE, value=FALSE)] <- "SUN"
combined_feats_GT$BEATTYPE[grep("AM",combined_feats_GT$BEATNAME, ignore.case = TRUE, value=FALSE)] <- "AM"
combined_feats_GT$BEATTYPE[grep("PM",combined_feats_GT$BEATNAME, ignore.case = TRUE, value=FALSE)] <- "PM"
combined_feats_GT$BEATTYPE[grep("SS",combined_feats_GT$BEATNAME, ignore.case = TRUE, value=FALSE)] <- "SS"
combined_feats_GT$BEATTYPE[as.numeric(as.character(combined_feats_GT$BEATNAME))<=14] <- "W"
combined_feats_GT$BEATTYPE[as.numeric(as.character(combined_feats_GT$BEATNAME))==15 | as.numeric(as.character(combined_feats_GT$BEATNAME))==16] <- "WLHS"
combined_feats_GT$BEATTYPE[as.numeric(as.character(combined_feats_GT$BEATNAME))>=49 & as.numeric(as.character(combined_feats_GT$BEATNAME))<=73] <- "D"
combined_feats_GT$BEATTYPE[as.numeric(as.character(combined_feats_GT$BEATNAME))==74 | as.numeric(as.character(combined_feats_GT$BEATNAME))==75] <- "DLHS"

combined_feats_GT$DATEBEAT <- as.character(combined_feats_GT$DATEBEAT)
combined_feats_GT$isWeekend <- as.character(combined_feats_GT$isWeekend)

# Remove incomplete cases from both data frames
do.call(data.frame,lapply(combined_feats_GT, function(x) replace(x, is.infinite(x),NA)))
combined_feats_GT <- combined_feats_GT[complete.cases(combined_feats_GT),]
combined_feats_GT <- combined_feats_GT[combined_feats_GT$SESSIONLENGTH<1440,]

# Use sum of all citation counts as count for beat with max values
DT=as.data.table(combined_feats_GT)
combined_feats_group <- (DT[,.SD[which.max(TICKETCOUNT)],by=.(DATEBEAT,OFFICERNAME)])
combined_feats_sum <- aggregate(TICKETCOUNT~DATEBEAT+OFFICERNAME, data=combined_feats_GT, sum, na.rm=TRUE)
colnames(combined_feats_sum) <- c("DATEBEAT","OFFICERNAME","TICKETCOUNTSUM")
combined_feats_temp <- merge(x = combined_feats_sum, y = combined_feats_group)
combined_feats_temp$OFFICERNAME <- NULL
combined_feats_temp$TICKETCOUNT <- NULL
combined_feats_GT <- combined_feats_temp
colnames(combined_feats_GT)[2] <- "TICKETCOUNT"

combined_feats_GT$TICKETCOUNT = as.numeric(combined_feats_GT$TICKETCOUNT)
x <- ddply(combined_feats_GT,~DATEBEAT+BEATNAME,function(x){x[which.max(x$TICKETCOUNT),]})
x <- x[order(x$DATEBEAT),]
x <- (x[x$TICKETCOUNT>7,])
combined_feats_GT <- x

combined_feats_GT <- combined_feats_GT[combined_feats_GT$DATEBEAT != today,]


# Add Estimate Rows
PATROLLENGTH = 480
SERVICELENGTH = 0
OTHERLENGTH = 0
SESSIONLENGTH = 480

allBeats <- c("AM1","AM2","AM3","AM4","PM1","PM2","PM3","PM4","PM5","PM6","PM7","PM8","PM9","PM10",
              "PM11","PM12","PM13","PM14","PM15",as.character(seq(1,16)),as.character(seq(49,75)),"SAT","SUN")
allBeatTypes <- c(rep("AM",4),rep("PM",15),rep("W",16),rep("D",27),"SAT","SUN")

combined_feats_GT[,2] <- as.character(combined_feats_GT[,2])
combined_feats_GT[,3] <- as.character(combined_feats_GT[,3])
combined_feats_GT[,4] <- as.character(combined_feats_GT[,4])
combined_feats_GT[,5] <- as.character(combined_feats_GT[,5])
combined_feats_GT[,6] <- as.character(combined_feats_GT[,6])
combined_feats_GT[,12] <- as.character(combined_feats_GT[,12])



for(i in 1:length(allBeats)){
  newrow <- c(as.character(today), "-1", SESSIONLENGTH, PATROLLENGTH, SERVICELENGTH,
              weather_feats$PrecipitationIn[weather_feats$Date == today],
              weather_feats$Events[weather_feats$Date == today],
              allBeats[i], weekdays(today), format(today, "%m"),
              ifelse(weekdays(today)=="Sunday" | weekdays(today)=="Saturday",1,0), allBeatTypes[i])
  combined_feats_GT <- rbind(combined_feats_GT,newrow)
}

combined_feats_GT$BEATTYPE <- as.factor(combined_feats_GT$BEATTYPE)
combined_feats_GT$PrecipitationIn <- as.numeric(combined_feats_GT$PrecipitationIn)
combined_feats_GT$PATROLLENGTH <- as.numeric(combined_feats_GT$PATROLLENGTH)
combined_feats_GT$SERVICELENGTH <- as.numeric(combined_feats_GT$SERVICELENGTH)
combined_feats_GT$SESSIONLENGTH <- as.numeric(combined_feats_GT$SESSIONLENGTH)
combined_feats_GT$TICKETCOUNT <- as.numeric(combined_feats_GT$TICKETCOUNT)
combined_feats_GT$Events <- as.factor(combined_feats_GT$Events)
combined_feats_GT$dayOfWeek <- as.factor(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.factor(combined_feats_GT$monthOfYear)
combined_feats_GT$BEATNAME <- as.factor(combined_feats_GT$BEATNAME)
combined_feats_GT$DATEBEAT <- as.Date(combined_feats_GT$DATEBEAT)
combined_feats_GT$isWeekend <- as.factor(combined_feats_GT$isWeekend)
#Removing deprecated beats and remove BEATNAME field (too many factors for random forest)
combined_feats_GT <- combined_feats_GT[!(combined_feats_GT$BEATTYPE=="----"),]  


citExpAllDays <- data.frame(citDate=as.Date(character()),
                            citBeat=character(),
                            citExp=numeric(),
                            citReason=character(),
                            stringsAsFactors=FALSE)
combined_feats_GT_test <- combined_feats_GT[combined_feats_GT$DATEBEAT == today,]

print(nrow(combined_feats_GT_test))

print(nrow(combined_feats_GT))
for ( i in 1:nrow(combined_feats_GT_test)){
  print(combined_feats_GT_test$DATEBEAT[i])
  print(today)
  if(combined_feats_GT_test$DATEBEAT[i] == today){
    citDate <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
    citBeat <- as.character(combined_feats_GT_test$BEATNAME[i])
    traintest <- combined_feats_GT[combined_feats_GT$DATEBEAT <= combined_feats_GT_test$DATEBEAT[i] & combined_feats_GT$BEATTYPE == combined_feats_GT_test$BEATTYPE[i],]
    traintest$BEATNAME <- as.factor(as.character(traintest$BEATNAME))
    train <- traintest[traintest$DATEBEAT < combined_feats_GT_test$DATEBEAT[i],]
    testall <- traintest[traintest$DATEBEAT == combined_feats_GT_test$DATEBEAT[i],]
    test <- testall[row.match(combined_feats_GT_test[i,],testall),]
    if(nrow(train) < 50){
      citDate <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
      citBeat <- as.character(combined_feats_GT_test$BEATNAME[i])
      citExp <- -1
      citReason <- "Insufficient Data"
      newrow <-  data.frame(citDate=citDate,citBeat=citBeat,citExp=citExp,citReason=citReason)
      citExpAllDays <- rbind(citExpAllDays, newrow)
    }
    else{
      rf <- randomForest(TICKETCOUNT ~ ., data=train, ntree=20, importance=TRUE)
      citExp <- as.numeric(predict(rf, test))
      citReasonframe <-as.data.frame(cbind(as.data.frame(importance(rf)),rownames(importance(rf))))
      names(citReasonframe) <- c('percentMSE', 'percentNodePurity', 'feature')
      citReason <- paste("Feature", "percentMSE", "percentNodePurity", "ActualValue", sep=":")
      for( j in 1:nrow(citReasonframe)){
        citReason <- paste(citReason,";",citReasonframe$feature[j],":",citReasonframe$percentMSE[j],":"
                           ,citReasonframe$percentNodePurity[j],":",
                           combined_feats_GT_test[i,grep(paste("^",citReasonframe$feature[j],"$",sep=""), names(combined_feats_GT_test))],sep="")
      }
      
      newrow <- data.frame(citDate=citDate,citBeat=citBeat,citExp=citExp,citReason=citReason)
      flog.info(as.character(combined_feats_GT_test$BEATNAME[i]), name="quiet")
      flog.info(citBeat, name="quiet")
      flog.info(citExp, name="quiet")
      citExpAllDays <- rbind(citExpAllDays, newrow)
    }
  }
}

names(citExpAllDays) <- c("DATE","BEAT","EXP","REASON")
citExpAllDaystest <- citExpAllDays
citExpAllDaystest$EXP <- round(citExpAllDaystest$EXP, digits=2)
citExpAllDaystest$DATE <- as.character(citExpAllDaystest$DATE)
citExpAllDaystest$EXP <- as.character(citExpAllDaystest$EXP)
citExpAllDaystest$BEAT <- as.character(citExpAllDaystest$BEAT)
citExpAllDaystest$REASON <- as.character(citExpAllDaystest$REASON)

combined_feats_GT$DATEBEAT <- as.character(combined_feats_GT$DATEBEAT)
combined_feats_GT$BEATNAME <- as.character(combined_feats_GT$BEATNAME)
combined_feats_GT$TICKETCOUNT <- as.character(combined_feats_GT$TICKETCOUNT)
combined_feats_GT$dayOfWeek <- as.character(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.character(combined_feats_GT$monthOfYear)
combined_feats_GT$isWeekend <- as.character(combined_feats_GT$isWeekend)
combined_feats_GT$BEATTYPE <- as.character(combined_feats_GT$BEATTYPE)
  
flog.info("Writing estimates data to table", name="quiet")

queryValues <- list()
for (r in 1:nrow(citExpAllDaystest)) {
  queryValues[[r]] <- paste("('", citExpAllDaystest$DATE[r], "', '",
                  citExpAllDaystest$BEAT[r], "', '",
                  citExpAllDaystest$EXP[r], "', '",
                  citExpAllDaystest$REASON[r], "'",
        ")",
      sep="")
}

write.table(citExpAllDaystest,
    file = paste(cwd, "logs",
        paste("citExpEstimatesToday-",
          today,
          ".csv",
          sep=""), sep="\\"),
    row.names=FALSE,
    col.names=FALSE,
    sep=",",
    quote=FALSE)

insertQueries <- list()
for (q in 1:length(targets)) {
  cfg <- BuildConfig(baseConfig, targets[[q]])
  writeHandle <- GetDBHandle(cfg)

  flog.info("Deleting estimates for yesterday %s", targets[[q]], name="quiet")
  sqlQuery(writeHandle, paste("DELETE FROM CITATIONESTIMATESCONVERTED WHERE DATE='", today, "'", sep=""))

  insertQuery <- paste("INSERT INTO CITATIONESTIMATESCONVERTED VALUES",
      paste(queryValues, collapse=", "),
      sep="")
  sqlQuery(writeHandle, insertQuery)
  flog.info("Wrote CITATIONESTIMATES to %s", targets[[q]], name="quiet")
  odbcClose(writeHandle)
}

flog.info("Done writing estimates for %s", city, "quiet")
