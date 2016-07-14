@echo off
set list=denver stagingdenver

Rscript D:\\citysightanalytics\\expectations\\TODAYCitationExpectationEstimateCreate.R

for %%a in (%list%) do (
  echo Writing Estimates for %%a
  Rscript D:\\citysightanalytics\\expectations\\WriteCitationEstimates.R %%a
)
