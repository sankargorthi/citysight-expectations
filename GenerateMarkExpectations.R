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

LoadOrInstallLibraries(c("argparser", "RODBC", "randomForest", "prodlim", "yaml", "devtools", "futile.logger"))
install_github("ozagordi/weatherData")
library(weatherData)
options(max.print=5)

parser <- arg_parser("Generate Mark Expectations")
parser <- add_argument(parser, "cwd", help="current working directory")
parser <- add_argument(parser, "date", help="date to generate estimates for")
parser <- add_argument(parser, "city", help="label in config.yml for DB credentials")
parser <- add_argument(parser, "targets", help="list of cities to write to")

args <- parse_args(parser, commandArgs(trailingOnly=TRUE))
cwd <- args$cwd
today <- as.Date(args$date, "%Y-%m-%d")
city <- args$city
targets <- strsplit(args$targets, ",")[[1]]


flog.appender(appender.file(paste(cwd, "logs", paste("mark-estimates-", today, ".log", sep=""), sep="\\")), "quiet")
baseConfig <- yaml.load_file(paste(cwd, "config.yml", sep="\\"))
config <- BuildConfig(baseConfig, city)

## Connect to the database
flog.info("Generating mark expectations for %s", city, name="quiet")
dbhandle <- GetDBHandle(config)

# Query database to get features and combine into one frame
markQuery <- paste("SELECT MARKEDDATE, SESSIONID, BEATNAME, count(*) AS MARKCOUNT FROM
(SELECT CAST(T.MARKEDDATETIME AS DATE) AS MARKEDDATE, T.LICENSEPLATE, T.SESSIONID, C.GPSBEAT AS BEATNAME
FROM TIMING_ACTIVITY T JOIN CORRECTEDMARKS
C ON T.LICENSEPLATE = C.LICENSEPLATE AND T.SESSIONID = C.SESSIONID) A
GROUP BY SESSIONID, MARKEDDATE, BEATNAME", sep="")
markcount <- sqlQuery(dbhandle,markQuery)
markcount$MARKEDDATE <- as.Date(markcount$MARKEDDATE)

featuresQuery <- paste("SELECT O.OFFICERID AS BADGENUMBER, O.OFFICERNAME, CAST(O.DATETIME AS DATE) AS DATEBEAT, O.RECID AS SESSIONID,
O.DATETIME, O.DATETIME2, D.TOTALLENGTH AS SESSIONLENGTH,
D.PATROLLENGTH, D.SERVICELENGTH, D.OTHERLENGTH
FROM OMS_SESSION O JOIN DutyStatusFeats D on O.RECID = D.SESSIONID", sep="")
oms_session_feats <- sqlQuery(dbhandle, featuresQuery)
oms_session_feats$DATEBEAT <- as.Date(oms_session_feats$DATEBEAT)

odbcClose(dbhandle)

weather_feats1 <- getSummarizedWeather("DEN", "2014-01-01", end_date = "2014-12-31",
                                       station_type = "airportCode", opt_all_columns = TRUE)
weather_feats2 <- getSummarizedWeather("DEN", "2015-01-01", end_date = "2015-12-31",
                                       station_type = "airportCode", opt_all_columns = TRUE)
weather_feats3 <- getSummarizedWeather("DEN", "2016-01-01", end_date = today,
                                       station_type = "airportCode", opt_all_columns = TRUE)
weather_feats_temp <- rbind(weather_feats1, weather_feats2)
colnames(weather_feats3) <- colnames(weather_feats_temp)
weather_feats <- rbind(weather_feats_temp, weather_feats3)
weather_feats$Date <- as.Date(weather_feats$Date)

markcount <- markcount[markcount$BEATNAME!=0,]
markcount <- markcount[!grepl(":",markcount$BEATNAME),]

combined_feats <- merge(oms_session_feats, weather_feats, by.x = "DATEBEAT", by.y = "Date", all.x=TRUE)

combined_feats_GT <- merge(combined_feats, markcount, by.x=c("SESSIONID","DATEBEAT"), by.y=c("SESSIONID","MARKEDDATE"), all.x=TRUE)

# Prune features and create derived features
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
combined_feats_GT$BADGENUMBER <- NULL
combined_feats_GT$OFFICERNAME <- NULL
combined_feats_GT$Max_Wind_SpeedMPH <- NULL
combined_feats_GT$Mean_TemperatureF <- NULL
combined_feats_GT$Mean_VisibilityMiles <- NULL
combined_feats_GT$Max_VisibilityMiles <- NULL
combined_feats_GT$Min_VisibilityMiles <- NULL
combined_feats_GT$CloudCover <- NULL
combined_feats_GT$OTHERLENGTH <- NULL
combined_feats_GT$Max_TemperatureF <- NULL
combined_feats_GT$Min_TemperatureF <- NULL
combined_feats_GT$Mean_Wind_SpeedMPH <- NULL
combined_feats_GT$EAT <- NULL

combined_feats_GT$dayOfWeek <- weekdays(combined_feats_GT$DATEBEAT)
combined_feats_GT$monthOfYear <- format(combined_feats_GT$DATEBEAT, "%m")
combined_feats_GT$isWeekend <- factor(ifelse(combined_feats_GT$dayOfWeek=="Sunday" | combined_feats_GT$dayOfWeek=="Saturday",1,0))
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

#Removing deprecated beats and remove BEATNAME field (too many factors for random forest)
combined_feats_GT <- combined_feats_GT[!(combined_feats_GT$BEATTYPE=="----"),]


# Remove incomplete cases from both data frames
do.call(data.frame,lapply(combined_feats_GT, function(x) replace(x, is.infinite(x),NA)))
combined_feats_GT <- combined_feats_GT[complete.cases(combined_feats_GT),]
combined_feats_GT <- combined_feats_GT[combined_feats_GT$SESSIONLENGTH<1440,]

#Set Length variables and create rows for today
PATROLLENGTH = 480
SERVICELENGTH = 0
SESSIONLENGTH = 480

allBeats <- c("AM1","AM2","AM3","AM4","PM1","PM2","PM3","PM4","PM5","PM6","PM7","PM8","PM9","PM10",
              "PM11","PM12","PM13","PM14","PM15",seq(1,16),seq(49,75),"SAT","SUN")
allBeatTypes <- c(rep("AM",4),rep("PM",15),rep("W",14),rep("WLHS",2),rep("D",25),rep("DLHS",2),"SAT","SUN")

#days <- seq(min(as.Date(combined_feats_GT$DATEBEAT)),to = max(as.Date(combined_feats_GT$DATEBEAT)), by='days')
#days <- seq(min(as.Date("2015-01-01")),to = max(as.Date("2015-12-31")), by='days')
days <- seq(min(as.Date(combined_feats_GT$DATEBEAT)),to = max(as.Date(combined_feats_GT$DATEBEAT)), by='days')
#days <- seq(max(as.Date(combined_feats_GT$DATEBEAT)),to = max(as.Date(combined_feats_GT$DATEBEAT)), by='days')


combined_feats_GT <- combined_feats_GT[combined_feats_GT$DATEBEAT!=today,]
combined_feats_GT[] <- lapply(combined_feats_GT, as.character)
for(i in 1:length(allBeats)){
  newrow <- c(as.character(today), SESSIONLENGTH, PATROLLENGTH, SERVICELENGTH,
              #as.numeric(as.vector(combined_feats_GT$PrecipitationIn[combined_feats_GT$DATEBEAT == today])[1]),
              #as.character(as.vector(combined_feats_GT$Events[combined_feats_GT$DATEBEAT == today])[1]),
              #weather_feats$PrecipitationIn[weather_feats$Date == today],
              #weather_feats$Events[weather_feats$Date == today],
              ifelse(length(weather_feats$PrecipitationIn[weather_feats$Date == today]) != 0, weather_feats$PrecipitationIn[weather_feats$Date == today], weather_feats$PrecipitationIn[weather_feats$Date == max(weather_feats$Date[weather_feats$Date <= today])]),
              ifelse(length(weather_feats$Events[weather_feats$Date == today]) != 0, weather_feats$Events[weather_feats$Date == today], weather_feats$Events[weather_feats$Date == max(weather_feats$Date[weather_feats$Date <= today])]),
              as.character(allBeats[i]), "-1", weekdays(today), format(today, "%m"),
              ifelse(weekdays(today)=="Sunday" | weekdays(today)=="Saturday",1,0), allBeatTypes[i])
  # print(newrow)
  # print("-----------------------")
  # print(newrow[7])
  combined_feats_GT <- rbind(combined_feats_GT,newrow)
  
}

combined_feats_GT$BEATTYPE <- as.factor(combined_feats_GT$BEATTYPE)
combined_feats_GT$PrecipitationIn <- as.numeric(combined_feats_GT$PrecipitationIn)
combined_feats_GT$PATROLLENGTH <- as.numeric(combined_feats_GT$PATROLLENGTH)
combined_feats_GT$SERVICELENGTH <- as.numeric(combined_feats_GT$SERVICELENGTH)
combined_feats_GT$SESSIONLENGTH <- as.numeric(combined_feats_GT$SESSIONLENGTH)
combined_feats_GT$MARKCOUNT <- as.numeric(combined_feats_GT$MARKCOUNT)
combined_feats_GT$Events <- as.factor(combined_feats_GT$Events)
combined_feats_GT$dayOfWeek <- as.factor(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.factor(combined_feats_GT$monthOfYear)
combined_feats_GT$BEATNAME <- as.factor(combined_feats_GT$BEATNAME)
combined_feats_GT$DATEBEAT <- as.Date(combined_feats_GT$DATEBEAT)
combined_feats_GT$isWeekend <- as.factor(combined_feats_GT$isWeekend)

markExpAllDays <- data.frame(markDate=as.Date(character()),
                            markBeat=character(),
                            markExp=numeric(),
                            markReason=character(),
                            stringsAsFactors=FALSE)



combined_feats_GT_test <- combined_feats_GT[combined_feats_GT$DATEBEAT == today,]
print(nrow(combined_feats_GT_test))
print(nrow(combined_feats_GT))
#flog.info("Found %d combined rows for marks", nrow(combined_feats_GT_test), name="quiet")

for ( i in 1:nrow(combined_feats_GT_test)){
  print(combined_feats_GT_test$DATEBEAT[i])
  #print(today)
  #flog.info("Generating training data #%d", i, name="quiet")
  #flog.info("Current datebeat: %s", combined_feats_GT_test$DATEBEAT[i], name="quiet")
  if(combined_feats_GT_test$DATEBEAT[i] == today){
    date <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
    beat <- as.character(combined_feats_GT_test$BEATNAME[i])
    traintest <- combined_feats_GT[combined_feats_GT$DATEBEAT <= combined_feats_GT_test$DATEBEAT[i] & combined_feats_GT$BEATTYPE == combined_feats_GT_test$BEATTYPE[i],]
    traintest$BEATNAME <- as.factor(as.character(traintest$BEATNAME))
    train <- traintest[traintest$DATEBEAT < combined_feats_GT_test$DATEBEAT[i],]
    testall <- traintest[traintest$DATEBEAT == combined_feats_GT_test$DATEBEAT[i],]
    test <- testall[row.match(combined_feats_GT_test[i,],testall),]
    if (nrow(train) < 100) {
      #flog.info("Found too little training data", name="quiet")
      date <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
      beat <- as.character(combined_feats_GT_test$BEATNAME[i])
      exp <- -1
      reason <- "Insufficient Data"
      newrow <- data.frame(date=date,beat=beat,exp=exp,reason=reason)
      markExpAllDays <- rbind(markExpAllDays, newrow)
    } else {
      rf <- randomForest(MARKCOUNT ~ ., data=train, ntree=20, importance=TRUE)
      exp <- as.numeric(predict(rf, test))
      print(exp)
      markReasonframe <- as.data.frame(cbind(as.data.frame(importance(rf)),rownames(importance(rf))))
      names(markReasonframe) <- c('percentMSE', 'percentNodePurity', 'feature')
      reason <- paste("Feature", "percentMSE", "percentNodePurity", "ActualValue", sep=":")
      #flog.info("Found enough training data for random forest: %d", nrow(markReasonframe), name="quiet")
      for(j in 1:nrow(markReasonframe)) {
        reason <- paste(reason,";",markReasonframe$feature[j],":",markReasonframe$percentMSE[j],":"
                        ,markReasonframe$percentNodePurity[j],":",
                        combined_feats_GT_test[i,grep(paste("^",markReasonframe$feature[j],"$",sep=""),
                                                 names(combined_feats_GT_test))],sep="")
      }
      
      newrow <- data.frame(date=date,beat=beat,exp=exp,reason=reason)
      markExpAllDays <- rbind(markExpAllDays, newrow)
    }
  }
}

names(markExpAllDays) <- c("DATE","BEAT","EXP","REASON")
markExpAllDaystest <- markExpAllDays
markExpAllDaystest$EXP <- round(markExpAllDaystest$EXP, digits=2)
markExpAllDaystest$DATE <- as.character(markExpAllDaystest$DATE)
markExpAllDaystest$EXP <- as.character(markExpAllDaystest$EXP)
markExpAllDaystest$BEAT <- as.character(markExpAllDaystest$BEAT)
markExpAllDaystest$REASON <- as.character(markExpAllDaystest$REASON)

combined_feats_GT$DATEBEAT <- as.character(combined_feats_GT$DATEBEAT)
combined_feats_GT$BEATNAME <- as.character(combined_feats_GT$BEATNAME)
combined_feats_GT$MARKCOUNT <- as.character(combined_feats_GT$MARKCOUNT)
combined_feats_GT$dayOfWeek <- as.character(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.character(combined_feats_GT$monthOfYear)
combined_feats_GT$isWeekend <- as.character(combined_feats_GT$isWeekend)
combined_feats_GT$BEATTYPE <- as.character(combined_feats_GT$BEATTYPE)

flog.info("Writing mark estimates data to table", name="quiet")

queryValues <- list()
for (r in 1:nrow(markExpAllDaystest)) {
  queryValues[[r]] <- paste("('", markExpAllDaystest$DATE[r], "', '",
                  markExpAllDaystest$BEAT[r], "', '",
                  markExpAllDaystest$EXP[r], "', '",
                  markExpAllDaystest$REASON[r], "'",
        ")",
      sep="")
}

write.table(markExpAllDaystest,
    file = paste(cwd, "logs",
        paste("citMarkEstimatesToday-",
        today,
        ".csv",
        sep=""), sep="\\"
        ),
    row.names=FALSE,
    col.names=FALSE,
    sep=",",
    quote=FALSE)

insertQueries <- list()
for (q in 1:length(targets)) {
  cfg <- BuildConfig(baseConfig, targets[[q]])
  writeHandle <- GetDBHandle(cfg)

  flog.info("Deleting mark estimates for yesterday %s", targets[[q]], name="quiet")
  sqlQuery(writeHandle, paste("DELETE FROM MARKPREDICTIONCONVERTED WHERE DATE='", (today - 1), "'", sep=""))

  insertQuery <- paste("INSERT INTO MARKPREDICTIONCONVERTED VALUES",
      paste(queryValues, collapse=", "),
      sep="")
  sqlQuery(writeHandle, insertQuery)
  flog.info("Wrote MARKPREDICTIONS to %s", targets[[q]], name="quiet")
  odbcClose(writeHandle)
}

flog.info("Done writing mark estimates for %s", city, "quiet")
