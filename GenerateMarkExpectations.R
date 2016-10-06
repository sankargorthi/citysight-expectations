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
  drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver",
      "/opt/citysight-expectations/sqljdbc/enu/sqljdbc4.jar")
  connector <- paste("jdbc:sqlserver://", ct$server, sep="")
  conn <- dbConnect(drv, connector, ct$uid, ct$pwd)

  return(conn)
}

BuildConfig <- function (config, city) {
  ct <- config[[city]]

  server <- ct$server
  uid <- ct$username
  pwd <- ct$password
  db <- ct$db

  return(list("server" = server, "uid" = uid, "pwd" = pwd, "db" = db))
}

LoadOrInstallLibraries(c("argparser", "RJDBC", "randomForest", "prodlim", "yaml", "devtools", "futile.logger"))
install_github("ozagordi/weatherData")
library(weatherData)
options(max.print=5)

parser <- arg_parser("Generate Citation Estimates")
parser <- add_argument(parser, "date", help="date to generate estimates for")
parser <- add_argument(parser, "city", help="label in config.yml for DB credentials")
parser <- add_argument(parser, "targets", help="list of cities to write to")

args <- parse_args(parser, commandArgs(trailingOnly=TRUE))
today <- as.Date(args$date, "%Y-%m-%d") - 1
city <- args$city
targets <- strsplit(args$targets, ",")[[1]]


flog.appender(appender.file(paste("/tmp/mark-estimates-", today, ".log", sep="")), "quiet")
baseConfig <- yaml.load_file("/opt/citysight-expectations/config.yml")
config <- BuildConfig(baseConfig, city)

## Connect to the database
flog.info("Generating mark expectations for %s", city, name="quiet")
dbhandle <- GetDBHandle(config)
tableIdentifier <- paste(config$db, ".dbo.", sep="")
writeTables <- list()
for (t in 1:length(targets)) {
  writeTables[[t]] <- paste(targets[[t]], ".dbo.", sep="")
}

# Query database to get features and combine into one frame
markQuery <- paste("SELECT MARKEDDATE, SESSIONID, BEATNAME, count(*) AS MARKCOUNT FROM
(SELECT CAST(T.MARKEDDATETIME AS DATE) AS MARKEDDATE, T.LICENSEPLATE, T.SESSIONID, C.GPSBEAT AS BEATNAME
FROM ", tableIdentifier, "TIMING_ACTIVITY T JOIN ", tableIdentifier, "CORRECTEDMARKS
C ON T.LICENSEPLATE = C.LICENSEPLATE AND T.SESSIONID = C.SESSIONID) A
GROUP BY SESSIONID, MARKEDDATE, BEATNAME", sep="")
markcount <- dbGetQuery(dbhandle,markQuery)
markcount$MARKEDDATE <- as.Date(markcount$MARKEDDATE)

featuresQuery <- paste("SELECT O.OFFICERID AS BADGENUMBER, O.OFFICERNAME, CAST(O.DATETIME AS DATE) AS DATEBEAT, O.RECID AS SESSIONID,
O.DATETIME, O.DATETIME2, D.TOTALLENGTH AS SESSIONLENGTH,
D.PATROLLENGTH, D.SERVICELENGTH, D.OTHERLENGTH
FROM ", tableIdentifier, "OMS_SESSION O JOIN ", tableIdentifier, "DutyStatusFeats D on O.RECID = D.SESSIONID", sep="")
oms_session_feats <- dbGetQuery(dbhandle, featuresQuery)
oms_session_feats$DATEBEAT <- as.Date(oms_session_feats$DATEBEAT)

weather_feats1 <- getSummarizedWeather("DEN", "2014-01-01", end_date = "2014-12-31", station_type = "airportCode", opt_all_columns = TRUE)
weather_feats2 <- getSummarizedWeather("DEN", "2015-01-01", end_date = "2015-12-31", station_type = "airportCode", opt_all_columns = TRUE)
weather_feats3 <- getSummarizedWeather("DEN", "2016-01-01", end_date = Sys.Date(), station_type = "airportCode", opt_all_columns = TRUE)
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

combined_feats_GT$dayOfWeek <- weekdays(combined_feats_GT$DATEBEAT)
combined_feats_GT$monthOfYear <- format(combined_feats_GT$DATEBEAT, "%m")
combined_feats_GT$isWeekend <- factor(ifelse(combined_feats_GT$dayOfWeek=="Sunday" | combined_feats_GT$dayOfWeek=="Saturday",1,0))

# Convert column types to factor/numeric as needed
combined_feats_GT$Max_TemperatureF <- as.numeric(combined_feats_GT$Max_TemperatureF)
combined_feats_GT$Min_TemperatureF <- as.numeric(combined_feats_GT$Min_TemperatureF)
combined_feats_GT$Mean_TemperatureF <- as.numeric(combined_feats_GT$Mean_TemperatureF)
combined_feats_GT$Max_VisibilityMiles <- as.numeric(combined_feats_GT$Max_VisibilityMiles)
combined_feats_GT$Min_VisibilityMiles <- as.numeric(combined_feats_GT$Min_VisibilityMiles)
combined_feats_GT$Mean_VisibilityMiles <- as.numeric(combined_feats_GT$Mean_VisibilityMiles)
combined_feats_GT$Max_Wind_SpeedMPH <- as.numeric(combined_feats_GT$Max_Wind_SpeedMPH)
combined_feats_GT$Mean_Wind_SpeedMPH <- as.numeric(combined_feats_GT$Mean_Wind_SpeedMPH)

combined_feats_GT$PrecipitationIn <- as.numeric(combined_feats_GT$PrecipitationIn)
combined_feats_GT$PATROLLENGTH <- as.numeric(combined_feats_GT$PATROLLENGTH)
combined_feats_GT$SERVICELENGTH <- as.numeric(combined_feats_GT$SERVICELENGTH)
combined_feats_GT$OTHERLENGTH <- as.numeric(combined_feats_GT$OTHERLENGTH)
combined_feats_GT$SESSIONLENGTH <- as.numeric(combined_feats_GT$SESSIONLENGTH)
combined_feats_GT$CloudCover <- as.numeric(combined_feats_GT$CloudCover)
combined_feats_GT$Events <- as.factor(combined_feats_GT$Events)
combined_feats_GT$dayOfWeek <- as.factor(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.factor(combined_feats_GT$monthOfYear)
combined_feats_GT$BEATNAME <- as.factor(combined_feats_GT$BEATNAME)
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
combined_feats_GT$BEATTYPE <- as.factor(combined_feats_GT$BEATTYPE)

#Removing deprecated beats and remove BEATNAME field (too many factors for random forest)
combined_feats_GT <- combined_feats_GT[!(combined_feats_GT$BEATTYPE=="----"),]

combined_feats_GT$BADGENUMBER <- NULL
combined_feats_GT$OFFICERNAME <- NULL
combined_feats_GT$Max_Wind_SpeedMPH <- NULL
#combined_feats_GT$Max_Gust_SpeedMPH <- NULL
combined_feats_GT$Mean_TemperatureF <- NULL
combined_feats_GT$Mean_VisibilityMiles <- NULL
combined_feats_GT$Max_VisibilityMiles <- NULL
weather_feats$Min_VisibilityMiles[is.na(weather_feats$Min_VisibilityMiles)] <- -1
weather_feats$Max_VisibilityMiles[is.na(weather_feats$Min_VisibilityMiles)] <- -1
weather_feats$Mean_VisibilityMiles[is.na(weather_feats$Min_VisibilityMiles)] <- -1


# Remove incomplete cases from both data frames
do.call(data.frame,lapply(combined_feats_GT, function(x) replace(x, is.infinite(x),NA)))
combined_feats_GT <- combined_feats_GT[complete.cases(combined_feats_GT),]
combined_feats_GT <- combined_feats_GT[combined_feats_GT$SESSIONLENGTH<1440,]


citExpAllDays <- data.frame(date=as.Date(character()),
                            beat=character(),
                            exp=numeric(),
                            reason=character(),
                            stringsAsFactors=FALSE)
combined_feats_GT_test <- combined_feats_GT[combined_feats_GT$DATEBEAT == (Sys.Date() - 1),]
print(nrow(combined_feats_GT_test))
print(nrow(combined_feats_GT))
for ( i in 1:nrow(combined_feats_GT_test)){
  print(combined_feats_GT_test$DATEBEAT[i])
  print((Sys.Date() - 1))
  if(combined_feats_GT_test$DATEBEAT[i] == (Sys.Date() - 1)){
    date <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
    beat <- as.character(combined_feats_GT_test$BEATNAME[i])
    traintest <- combined_feats_GT[combined_feats_GT$DATEBEAT <= combined_feats_GT_test$DATEBEAT[i] & combined_feats_GT$BEATTYPE == combined_feats_GT_test$BEATTYPE[i],]
    traintest$BEATNAME <- as.factor(as.character(traintest$BEATNAME))
    train <- traintest[traintest$DATEBEAT < combined_feats_GT_test$DATEBEAT[i],]
    testall <- traintest[traintest$DATEBEAT == combined_feats_GT_test$DATEBEAT[i],]
    test <- testall[row.match(combined_feats_GT_test[i,],testall),]
    if (nrow(train) < 100) {
      date <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
      beat <- as.character(combined_feats_GT_test$BEATNAME[i])
      exp <- -1
      reason <- "Insufficient Data"
      newrow <- data.frame(date=date,beat=beat,exp=exp,reason=reason)
      citExpAllDays <- rbind(citExpAllDays, newrow)
    } else {
      rf <- randomForest(MARKCOUNT ~ ., data=train, ntree=20, importance=TRUE)
      exp <- as.numeric(predict(rf, test))
      citReasonframe <- as.data.frame(cbind(as.data.frame(importance(rf)),rownames(importance(rf))))
      names(citReasonframe) <- c('percentMSE', 'percentNodePurity', 'feature')
      reason <- "Feature,percentMSE,percentNodePurity,ActualValue"
      for(j in 1:nrow(citReasonframe)) {
        reason <- paste(reason,";",citReasonframe$feature[j],":",citReasonframe$percentMSE[j],":"
            ,citReasonframe$percentNodePurity[j],":",
            combined_feats_GT[i,grep(paste("^",citReasonframe$feature[j],"$",sep=""),
            names(combined_feats_GT))],sep="")
      }

      newrow <- data.frame(date=date,beat=beat,exp=exp,reason=reason)
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
combined_feats_GT$MARKCOUNT <- as.character(combined_feats_GT$MARKCOUNT)
combined_feats_GT$dayOfWeek <- as.character(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.character(combined_feats_GT$monthOfYear)
combined_feats_GT$isWeekend <- as.character(combined_feats_GT$isWeekend)
combined_feats_GT$BEATTYPE <- as.character(combined_feats_GT$BEATTYPE)

flog.info("Writing mark estimates data to table", name="quiet")

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
    file = paste("/tmp/citExpEstimatesToday-",
        today,
        ".csv",
        sep=""),
    row.names=FALSE,
    col.names=FALSE,
    sep=",",
    quote=FALSE)

insertQueries <- list()
for (q in 1:length(writeTables)) {
  cfg <- BuildConfig(baseConfig, targets[[q]])
  writeHandle <- GetDBHandle(cfg)

  flog.info("Deleting mark estimates for yesterday %s", targets[[q]], name="quiet")
  dbSendUpdate(writeHandle, paste("DELETE FROM ", writeTables[[q]], "MARKPREDICTIONCONVERTED WHERE DATE='", today, "'", sep=""))

  insertQuery <- paste("INSERT INTO ", writeTables[[q]], "MARKPREDICTIONCONVERTED VALUES",
      paste(queryValues, collapse=", "),
      sep="")
  dbSendUpdate(writeHandle, insertQuery)
  flog.info("Wrote MARKPREDICTIONS to %s", targets[[q]], name="quiet")
}

flog.info("Done writing mark estimates for %s", city, "quiet")
