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

LoadOrInstallLibraries(c("argparser", "RJDBC", "futile.logger", "yaml"))
flog.appender(appender.file("/tmp/expectations.log"), "quiet")

# AB: parse arguments
parser <- arg_parser("Write Citation Expectations")
parser <- add_argument(parser, "city", help="label in config.yml for DB credentials")

args <- parse_args(parser, commandArgs(trailingOnly=TRUE))

config <- BuildConfig(yaml.load_file("/opt/citysight-expectations/config.yml"), city)

## Connect to the database
flog.info("Generating citation expectations for %s", city, name="quiet")
dbhandle <- GetDBHandle(config)

flog.info("Deleting expectations for yesterday", name="quiet")
dbGetQuery(dbhandle, paste("DELETE FROM ", config$db, ".dbo.CITATIONPREDICTION WHERE DATE=\'",
    (Sys.Date() - 1),"\'", sep=""))

flog.info("Bulk inserting citation expectations", name="quiet")
dbGetQuery(dbhandle, paste("BULK INSERT ", config$db, ".dbo.CITATIONPREDICTION FROM
    '/opt/citysight-expectations/citExpToday.csv' WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a')", sep=""))

dbGetQuery(dbhandle, paste("IF OBJECT_ID('CITATIONPREDICTIONCONVERTED', 'U') IS NOT NULL DROP TABLE ",
    config$db, ".dbo.CITATIONPREDICTIONCONVERTED", sep=""))

dbGetQuery(dbhandle, paste("SELECT * INTO ", config$db, ".dbo.CITATIONPREDICTIONCONVERTED FROM ",
    config$db, ".dbo.CITATIONPREDICTION", sep=""))

dbGetQuery(dbhandle, paste("ALTER TABLE ", config$db,
    ".dbo.CITATIONPREDICTIONCONVERTED ALTER COLUMN DATE date", sep=""))
