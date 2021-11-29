# Challenge: Search Data Analysis 

## Description
This project reads data from S3 bucket and creates a Spark aggregation that can produce a dataset that contains only the relevant competitors for account_ids in the Relevant Competitors dataset and their impressions across the relevant search terms for
the same account_ids across each day.
- The impression each domain has received based on appearances and scrape counts is rounded to the nearest integer greater than or equal to it.
- The start and end date is provided using reference.conf file and can be overridden using the external-reference.conf file.
- The code requires aws credentials to be provided at the run time and is passed via spark-submit command.
- In a prod environment, using IAM roles would be preferred.
- The project can be scaled using the appropriate cluster size, cores & memory for the Spark driver & executor daemons in a yarn-client or yarn-cluster mode.
- The performance can be optimized by using the appropriate cluster configuration that balances between fat & tiny executors.


## Tools/Resources Used

- scala 2.12.15                               (Coding Language)
- Apache Spark 3.2.0                          (Processing Framework)
- Intellij Idea Ultimate Edition   2021.2     (Development Tool)
- SBT 1.5.5                                   (Build Tool)

## Project Structure
```
├── spark-search-analysis
|   ├── build.sbt
|   ├── docker
│   ├── logs
│   │   └── spark-search-analysis.log
│   ├── output
│   │   └── result
│   │       ├── part-00000-936bf41a-0acf-4e70-88b9-ae4f708d201f-c000.csv
│   │       └── _SUCCESS
|   ├── project
|   │   ├── build.properties
|   │   ├── assembly.sbt
|   ├── README.md
│   ├── src
│       ├── main
│       │   ├── resources
│       │   │   ├── log4j.properties
│       │   │   └── reference.conf
│       │   └── scala
│       │       └── com
│       │           └── data
│       │               ├── Main.scala
│       │               ├── model
│       │               │   ├── CompetitorAppearances.scala
│       │               │   ├── RelevantCompetitors.scala
│       │               │   ├── RelevantSearchTerms.scala
│       │               │   ├── Results.scala
│       │               │   ├── ScrapeAppearances.scala
│       │               │   └── Volumes.scala
│       │               ├── package.scala
│       │               ├── Settings.scala
│       │               └── utils
│       │                   └── DateUtils.scala
│       │                   └── ReadWriteUtils.scala
│       └── test
│           └── scala
│               └── com
│                   └── data
│                       └── MainSpec.scala
```

## Steps to run using Docker

### 1. Install docker
https://docs.docker.com/install/

### 2. Using the terminal, Pull the docker image from docker hub
docker pull priyankamittal09/scala-spark-3.2.0

### 3. This step is to download the docker file and start the docker container.
docker run -it --rm priyankamittal09/scala-spark-3.2.0:latest /bin/sh

### 4. (optional step) Script to pull spark project from the git repository to run SBT tests. You should by default be on the /app directory level.
sh test-script.sh

### 5. Main script to run the jar using spark-submit using the same access key & secret key provided in the technical assignment
sh start-script.sh <access-key> <secret-key>

### 6 In the docker, the final tsv file is available at:
/app/output/result/part-xxx....xx.csv

### In github, the final tsv file is available at:
https://github.com/priyankamittal-09/spark-search-analysis/blob/master/output/result/part-00000-936bf41a-0acf-4e70-88b9-ae4f708d201f-c000.csv

### 7. To exit docker container
exit


