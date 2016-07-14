# Expectations
These scripts are used to generate Citation expectations for cities

They are driven off of the `writeconfig.yml` file located at the root folder.

## Workflow
- These scripts run on cron jobs setup on citysight DB boxes.
- The scripts consume data from `ISSUANCE`, `CORRECTEDBEATS`, etc.
- Expectations for the targeted cities will be written to CSV files.
- These CSV records are then inserted into the expectations tables.

## Setup

```bash
PS> copy config.dist.yml writeconfig.yml
```
Specify the city's DB credentials following the examples given.

## Usage

```bash
PS> .\expectations\Daily.bat
```

# License
(c) PARC 2016. All Rights Reserved.
