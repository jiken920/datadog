## Log Analyzer Utility

Utility for analyzing logs. To build it, clone this repo and build the executable JAR by running: 
```
mvn clean package
```

To run, navigate to the directory where the JAR was built (usually /target) and run the following command. You must provide a file using the `--file` or `-f` option:
```
java -jar datadog-kenji-rudio-1.0-SNAPSHOT-jar-with-dependencies.jar --file d:\sample_csv.txt
```

By default, this utility runs with a default rate limit of 10 requests per second, a stats collection window of 10 seconds, and a rate-limit window of 120 seconds. You can override these values using the `--statsWindowSize` (or `-s`) and `--rateLimitWindowSize` (or `-w`) options resepctively: 
```
java -jar datadog-kenji-rudio-1.0-SNAPSHOT-jar-with-dependencies.jar --file d:\sample_csv.txt --rateLimit 15 --statsWindowSize 60 --rateLimitWindowSize 90
```


Potential future improvements:
1. Persist log statistics to a database for reporting.
2. Ship log and application statistics to a metrics provider (such as Graphite) for real-time dashboards and alerting.
3. Parameterize support for other file types besides CSV.
4. Support other types of fields (columns) in logs.
5. Help menu for users unfamiliar with the utility.
