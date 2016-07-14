if (!require('RODBC')) {
  install.packages('RODBC', repos="http://cran.rstudio.com/")
}
if (!require('randomForest')) {
  install.packages('randomForest', repos="http://cran.rstudio.com/")
}
if (!require('prodlim')) {
  install.packages('prodlim', repos="http://cran.rstudio.com/")
}
if (!require('yaml')) {
  install.packages('yaml', repos="http://cran.rstudio.com/")
}
if (!require('devtools')) {
  install.packages('devtools', repos="http://cran.rstudio.com/")
}
if (!require('futile.logger')) {
  install.packages('futile.logger', repos="http://cran.rstudio.com/")
}

install_github("ozagordi/weatherData")
library(weatherData)
#library('yaml')

#config <- yaml.load_file("ienforce-config.yml")

flog.appender(appender.file('/tmp/estimates.log'), 'quiet')

options(max.print=5)
## Connect to the database
flog.info("Generating estimates for %s", "DEVDENVER", name='quiet')
dbhandle <- odbcDriverConnect('driver={SQL Server};server=db.devdenver.citysightportal.com;database=DEVDENVER;uid=sa;pwd=password;trusted_connection=true')


# Query database to get features and combine into one frame

ticketcount <- sqlQuery(dbhandle,
                        "SELECT BADGENUMBER, OFFICERNAME, ISSUEDATE, BEATNAME, count(*) AS TICKETCOUNT FROM (SELECT I.BADGENUMBER,I.OFFICERNAME,CAST(I.ISSUEDATETIME AS DATE) AS ISSUEDATE, C.GPSBEAT AS BEATNAME
                          FROM ISSUANCE I JOIN CORRECTEDBEATS C ON I.TICKETNUMBER = C.TICKETNUMBER) A
                        GROUP BY BADGENUMBER, OFFICERNAME, ISSUEDATE, BEATNAME")
ticketcount$ISSUEDATE <- as.Date(ticketcount$ISSUEDATE)

oms_session_feats <- sqlQuery(dbhandle,
                              "SELECT O.OFFICERID AS BADGENUMBER, O.OFFICERNAME, CAST(O.DATETIME AS DATE) AS DATEBEAT, O.RECID AS SESSIONID,
                              O.DATETIME, O.DATETIME2, D.TOTALLENGTH AS SESSIONLENGTH,
                              D.PATROLLENGTH, D.SERVICELENGTH, D.OTHERLENGTH
                              FROM OMS_SESSION O JOIN DutyStatusFeats D on O.RECID = D.SESSIONID")
oms_session_feats$DATEBEAT <- as.Date(oms_session_feats$DATEBEAT)

weather_feats1 <- getSummarizedWeather("DEN", "2014-01-01", end_date = "2014-12-31", station_type = "airportCode", opt_all_columns = TRUE)
weather_feats2 <- getSummarizedWeather("DEN", "2015-01-01", end_date = "2015-12-31", station_type = "airportCode", opt_all_columns = TRUE)
weather_feats3 <- getSummarizedWeather("DEN", "2016-01-01", end_date = Sys.Date(), station_type = "airportCode", opt_all_columns = TRUE)
weather_feats_temp <- rbind(weather_feats1, weather_feats2)
colnames(weather_feats3) <- colnames(weather_feats_temp)
weather_feats <- rbind(weather_feats_temp, weather_feats3)
weather_feats$Date <- as.Date(weather_feats$Date)

ticketcount <- ticketcount[ticketcount$BEATNAME!=0,]
ticketcount <- ticketcount[!grepl(":",ticketcount$BEATNAME),]

combined_feats <- merge(oms_session_feats, weather_feats, by.x = "DATEBEAT", by.y = "Date", all.x=TRUE)

combined_feats_GT <- merge(combined_feats, ticketcount, by.x=c("BADGENUMBER","OFFICERNAME","DATEBEAT"), by.y=c("BADGENUMBER","OFFICERNAME","ISSUEDATE"), all.x=TRUE)

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
combined_feats_GT$Max_Wind_SpeedMPH <- NULL
combined_feats_GT$Max_Gust_SpeedMPH <- NULL
combined_feats_GT$Mean_TemperatureF <- NULL
combined_feats_GT$Mean_VisibilityMiles <- NULL
combined_feats_GT$Max_VisibilityMiles <- NULL

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
weather_feats$Min_VisibilityMiles[is.na(weather_feats$Min_VisibilityMiles)] <- -1
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

combined_feats_GT$BADGENUMBER <- NULL
combined_feats_GT$OFFICERNAME <- NULL


# Add Estimate Rows
PATROLLENGTH = 390
SERVICELENGTH = 0
OTHERLENGTH = 0
SESSIONLENGTH = 390

allBeats <- c("AM1","AM2","AM3","AM4","PM1","PM2","PM3","PM4","PM5","PM6","PM7","PM8","PM9","PM10",
              "PM11","PM12","PM13","PM15","PM15",seq(1,16),seq(49,75))
allBeatTypes <- c(rep("AM",4),rep("PM",15),rep("W",14),rep("WLHS",2),rep("D",25),rep("DLHS",2))

for(i in 1:length(allBeats)){
  newrow <- c(as.character(Sys.Date()), SESSIONLENGTH, PATROLLENGTH, SERVICELENGTH, OTHERLENGTH,
              weather_feats$Max_TemperatureF[weather_feats$Date == Sys.Date()],
              weather_feats$Min_TemperatureF[weather_feats$Date == Sys.Date()],
              weather_feats$Min_VisibilityMiles[weather_feats$Date == Sys.Date()],
              weather_feats$Mean_Wind_SpeedMPH[weather_feats$Date == Sys.Date()],
              weather_feats$PrecipitationIn[weather_feats$Date == Sys.Date()],
              weather_feats$CloudCover[weather_feats$Date == Sys.Date()],
              weather_feats$Events[weather_feats$Date == Sys.Date()],
              allBeats[i], "-1", weekdays(Sys.Date()), format(Sys.Date(), "%m"),
              ifelse(weekdays(Sys.Date())=="Sunday" | weekdays(Sys.Date())=="Saturday",1,0), allBeatTypes[i])
  combined_feats_GT <- rbind(combined_feats_GT,newrow)

}

combined_feats_GT$BEATTYPE <- as.factor(combined_feats_GT$BEATTYPE)
combined_feats_GT$Max_TemperatureF <- as.numeric(combined_feats_GT$Max_TemperatureF)
combined_feats_GT$Min_TemperatureF <- as.numeric(combined_feats_GT$Min_TemperatureF)
combined_feats_GT$Min_VisibilityMiles <- as.numeric(combined_feats_GT$Min_VisibilityMiles)
combined_feats_GT$Mean_Wind_SpeedMPH <- as.numeric(combined_feats_GT$Mean_Wind_SpeedMPH)
combined_feats_GT$PrecipitationIn <- as.numeric(combined_feats_GT$PrecipitationIn)
combined_feats_GT$PATROLLENGTH <- as.numeric(combined_feats_GT$PATROLLENGTH)
combined_feats_GT$SERVICELENGTH <- as.numeric(combined_feats_GT$SERVICELENGTH)
combined_feats_GT$OTHERLENGTH <- as.numeric(combined_feats_GT$OTHERLENGTH)
combined_feats_GT$SESSIONLENGTH <- as.numeric(combined_feats_GT$SESSIONLENGTH)
combined_feats_GT$CloudCover <- as.numeric(combined_feats_GT$CloudCover)
combined_feats_GT$TICKETCOUNT <- as.numeric(combined_feats_GT$TICKETCOUNT)
combined_feats_GT$Events <- as.factor(combined_feats_GT$Events)
combined_feats_GT$dayOfWeek <- as.factor(combined_feats_GT$dayOfWeek)
combined_feats_GT$monthOfYear <- as.factor(combined_feats_GT$monthOfYear)
combined_feats_GT$BEATNAME <- as.factor(combined_feats_GT$BEATNAME)
combined_feats_GT$DATEBEAT <- as.Date(combined_feats_GT$DATEBEAT)
combined_feats_GT$isWeekend <- as.factor(combined_feats_GT$isWeekend)
#Removing deprecated beats and remove BEATNAME field (too many factors for random forest)
combined_feats_GT <- combined_feats_GT[!(combined_feats_GT$BEATTYPE=="----"),]

# Remove incomplete cases from both data frames
do.call(data.frame,lapply(combined_feats_GT, function(x) replace(x, is.infinite(x),NA)))
combined_feats_GT <- combined_feats_GT[complete.cases(combined_feats_GT),]
combined_feats_GT <- combined_feats_GT[combined_feats_GT$SESSIONLENGTH<1440,]



citExpAllDays <- data.frame(citDate=as.Date(character()),
                            citBeat=character(),
                            citExp=numeric(),
                            citReason=character(),
                            stringsAsFactors=FALSE)
combined_feats_GT_test <- combined_feats_GT[combined_feats_GT$DATEBEAT == (Sys.Date()),]

print(nrow(combined_feats_GT_test))

print(nrow(combined_feats_GT))
for ( i in 1:nrow(combined_feats_GT_test)){
  print(combined_feats_GT_test$DATEBEAT[i])
  print(Sys.Date())
  if(combined_feats_GT_test$DATEBEAT[i] == Sys.Date()){
    citDate <- as.character(as.Date(combined_feats_GT_test$DATEBEAT[i]))
    citBeat <- as.character(combined_feats_GT_test$BEATNAME[i])
    traintest <- combined_feats_GT[combined_feats_GT$DATEBEAT <= combined_feats_GT_test$DATEBEAT[i] & combined_feats_GT$BEATTYPE == combined_feats_GT_test$BEATTYPE[i],]
    traintest$BEATNAME <- as.factor(as.character(traintest$BEATNAME))
    train <- traintest[traintest$DATEBEAT < combined_feats_GT_test$DATEBEAT[i],]
    testall <- traintest[traintest$DATEBEAT == combined_feats_GT_test$DATEBEAT[i],]
    test <- testall[row.match(combined_feats_GT_test[i,],testall),]
    if(nrow(train) < 100){
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
      citReason <- "Feature,percentMSE,percentNodePurity,ActualValue"
      for( j in 1:nrow(citReasonframe)){
        citReason <- paste(citReason,";",citReasonframe$feature[j],":",citReasonframe$percentMSE[j],":"
                           ,citReasonframe$percentNodePurity[j],":",
                           combined_feats_GT[i,grep(paste("^",citReasonframe$feature[j],"$",sep=""), names(combined_feats_GT))],sep="")
      }

      newrow <- data.frame(citDate=citDate,citBeat=citBeat,citExp=citExp,citReason=citReason)
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

write.table(citExpAllDaystest,file="citExpEstimatesToday.csv", row.names=FALSE, col.names=FALSE, sep=",", quote=FALSE)
