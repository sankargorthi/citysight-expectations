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
parser <- arg_parser("Write Mark Expectations")
parser <- add_argument(parser, "city", help="label in config.yml for DB credentials")

args <- parse_args(parser, commandArgs(trailingOnly=TRUE))

dbhandle <- GetDBHandle(args$city)

flog.info("Deleting mark expectations for yesterday", name="quiet")
sqlQuery(dbhandle,paste("DELETE FROM ", config$db, ".dbo.MARKPREDICTION WHERE DATE=\'",
    (Sys.Date() - 1),"\'", sep=""))

flog.info("Bulk inserting mark expecation data", name="quiet")
sqlQuery(dbhandle, paste("BULK INSERT ", config$db, ".dbo.MARKPREDICTION FROM
    '/opt/citysight-expectations/markExpToday.csv'
    WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a')",
    sep=""))

sqlQuery(dbhandle, paste("IF OBJECT_ID('MARKPREDICTIONCONVERTED', 'U') IS NOT NULL DROP TABLE ",
    config$db, ".dbo.MARKPREDICTIONCONVERTED", sep=""))

sqlQuery(dbhandle, paste("SELECT * INTO ", config$db, ".dbo.MARKPREDICTIONCONVERTED FROM ", config$db,
    ".dbo.MARKPREDICTION", sep=""))

sqlQuery(dbhandle, paste("ALTER TABLE ", config$db,
    ".dbo.MARKPREDICTIONCONVERTED ALTER COLUMN DATE date", sep=""))
