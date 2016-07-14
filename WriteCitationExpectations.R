# AB: util functions
isInstalled <- function(package) {
  is.element(package, installed.packages()[,1])
}

LoadOrInstallLibraries <- function(packages) {
  for(package in packages) {
    if(!isInstalled(package)) {
      install.packages(package,repos="http://cran.rstudio.com/")
    }
    require(package,character.only=TRUE,quietly=TRUE)
  }
}

LoadOrInstallLibraries(c("argparser", "RODBC", "futile.logger", "yaml"))
flog.appender(appender.file("expectations.log"), "quiet")

GetDBHandle <- function(city) {
  config <- yaml.load_file("D:/citysightanalytics/writeconfig.yml")
  ct <- config[[city]]
  connectionString <- paste("driver={SQL Server};server=", ct$server, ";database=", ct$db, ";uid=", ct$username,
      ";pwd=", ct$password, ";trusted_connection=true", sep="")
  flog.info("Writing citation expectation data for %s", city, name="quiet");
  dbhandle <- odbcDriverConnect(connectionString)
  return(dbhandle)
}


# AB: parse arguments
parser <- arg_parser("Write Citation Expectations")
parser <- add_argument(parser, "city", help="label in writeconfig.yml for DB credentials")

args <- parse_args(parser, commandArgs(trailingOnly=TRUE))

dbhandle <- GetDBHandle(args$city)

flog.info("Deleting expectations for yesterday", name="quiet")
sqlQuery(dbhandle,paste("DELETE FROM CITATIONPREDICTION WHERE DATE=\'",(Sys.Date() - 1),"\'", sep=""))

flog.info("Bulk inserting citation expectations", name="quiet")
sqlQuery(dbhandle,"BULK INSERT CITATIONPREDICTION FROM 'D:\\citysightanalytics\\expectations\\citExpToday.csv' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a')")

sqlQuery(dbhandle,"IF OBJECT_ID('CITATIONPREDICTIONCONVERTED', 'U') IS NOT NULL DROP TABLE CITATIONPREDICTIONCONVERTED")

sqlQuery(dbhandle,"SELECT * INTO CITATIONPREDICTIONCONVERTED FROM CITATIONPREDICTION")

sqlQuery(dbhandle,"ALTER TABLE CITATIONPREDICTIONCONVERTED ALTER COLUMN DATE date")
