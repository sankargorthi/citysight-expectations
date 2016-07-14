@echo off
set list=denver stagingdenver

Rscript D:\\citysightanalytics\\expectations\\TODAYCitationExpectationModelCreate.R
Rscript D:\\citysightanalytics\\expectations\\TODAYMarkExpectationModelCreate.R

for %%a in (%list%) do (
  echo Writing Expectations for %%a
  Rscript D:\\citysightanalytics\\expectations\\WriteCitationExpectations.R %%a
  Rscript D:\\citysightanalytics\\expectations\\WriteMarkExpectations.R %%a
)
