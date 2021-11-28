# Challenge: Search Data Analysis                                       

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
│   │       ├── part-00000-edf58c6b-ffbf-4f43-a2e6-02bd8118e96b-c000.csv
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

### 2. Pull the docker image from docker hub
docker pull priyankamittal09/scala-spark-3.2.0

### 3. This step is to download the docker file and start the docker container.
docker run -it --rm priyankamittal09/scala-spark-3.2.0:latest /bin/sh

### 4. (optional step) Script to pull spark project from the git repository to run SBT tests. You should by default be on the /app directory level.
sh test-script.sh

### 5. Main script to run the jar using spark-submit
sh start-script.sh

### 6. To exit docker container
exit


