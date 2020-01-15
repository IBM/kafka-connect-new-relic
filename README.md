## Newrelic Source Connector
This kafka source connector is designed to pull data from newrelic using **Insights Api** and dump that raw data into a kafka topic. It can be configured to run one or more adhoc NRQL queries.

## Build
It uses gradle build tool. To build this connector, run this from project root:

    gradle build

To bundle this as **uberjar**, run this from project root:

    gradle shadowJar

## Build Container
**Dockerfile** is provided at the root of this project that can be used to bundle this source connector into a docker container image. This docker image is based off of **confluentinc/kafka-connect** image.

Before you build a container, make sure to build an **uberjar** using instructions in **build** section above.

Once uberjar is built, run this command from project root to build a docker container:


    docker build --no-cache --rm -t connect-with-newrelic:5.2.3 .

## Run
Use provided **docker-compose.yml** to spin up this container along with other essential dependencies. This will start zookeeper, kafka-broker, kafka-schema-registry, and kafka connect runtime (with newrelic source connector bundled). Use this command to start all relevant containers:

    docker-compose up -d
 
**NOTE**:

Assuming connect container is built as per instructions above and is registered as **connect-with-newrelic:5.2.3**


Create newrelic-src-connector-cfg.json file using the following sample payload:

    {
      "name": "newrelic-src-connector-demo",
      "config": {
     	"connector.class": "com.ibm.connect.newrelic.NewRelicSourceConnector",
     	"tasks.max": "1",
     	"newrelic.accountId": "<Your Newrelic AccountId>",
     	"sink.account": "<customer name>",
     	"nrql.apikey": "<your nrql api key>",
     	"nrql.query.baseUrl": "https://insights-api.newrelic.com/v1/accounts/%s/query",
     	"nrql.metric.prefix": "newrelic.demo",
     	"nrql.query.metadata.empty.dim.value": "(none)",
     	"nrql.query.timestamp.column": "timestamp",
     	"nrql.query.start.timestamp": "",
     	"nrql.query.batchSize": "500",
     	"nrql.query.startDays": "3",
     	"nrql.metricName": "monitor",
     	"nrql.metricName.monitor.metricName": "urlcheck",
     	"nrql.metricName.monitor.table": "SyntheticCheck",
     	"nrql.metricName.monitor.metadata": "monitorName:monitor,locationLabel:location,typeLabel:type",
     	"nrql.metricName.monitor.value": "result,duration",
     	"nrql.metricName.monitor.where.clause": "monitorName like '%<your query if any>%'",
     	"sink.topic": "newrelic.sink.topic.raw.v1",
     	"newrelic.poll.interval": "15000"
      }
    }

Copy this sample config to connect container, using this command:

    docker cp ./newrelic-src-connector-cfg.json connector:/var/tmp/

Once **config** is copied to **connect runtime** container, and it is up and running, submit a sample newrelic source connector, using the following commands:

    docker-compose exec connector bash
    cd /var/tmp
    curl -X POST -H "Content-Type: application/json" --data-binary "@newrelic-src-connector-cfg.json" localhost:8083/connectors


## Configuration Options

**NOTE**:

* Currently each metric/query is assigned to 1 task
* Columns (in query select list) can have optional aliases of the form: **column:alias**
* Optional **string replacement** for empty data columns can be specified using this config-setting: **nrql.query.metadata.empty.dim.value**
* Sink topic partitioning is based off of metric **value** column
* All configuration values are **strings** as per [Connect Reference](https://docs.confluent.io/current/connect/references/restapi.html#post--connectors).  

**NOTE**

Insights metric name will be built by:
First splitting **nrql.metricName** key on **,**, then each of these keys will be appended to this prefix as specified by **nrql.metric.prefix** using this template tp generate **insights metric name**:

    nrql.metric.prefix-<Each Key From nrql.metricName>

 
### Required Configurations
Following connector configurations are required, and cannot be omitted:
* name - provide a name that capture the function of this connector
* connector.class - always use **com.ibm.connect.newrelic.NewRelicSourceConnector**
* newrelic.accountId - provide your newrelic accountId here
* nrql.apikey - provide nrql api key for running nrql queries
* nrql.query.baseUrl - always be **https://insights-api.newrelic.com/v1/accounts/%s/query**
* nrql.query.metadata.empty.dim.value - could be any value you want to use to represent empty column values
* nrql.metric.prefix - this will be used to create fully qualified topic name
* nrql.metricName - comma separated list of **keys** to identify each **nrql** queries
* nrql.metricName.<keyName> - for each comma separated key in **nrql.metricName** should have an associated config section of this form: **nrql.metricName.<keyName>.someSetting(s)**
* nrql.metricName.<keyName>.table - name of the insights event table such as **SyntheticCheck**, etc.
* nrql.metricName.<keyName>.metadata - metadata column list for nrql query
* nrql.metricName.<keyName>.value - value column list for nrql query

### Optional Configurations
Following configurations are optional and will get the specified **default** values if omitted:

```
"sink.account": "CDIR12", // optional, no default needed
"nrql.apikey": "api key for account id given above",
"nrql.query.baseUrl": "https://insights-api.newrelic.com/v1/accounts/%s/query",
"nrql.query.metadata.empty.dim.value": "empty", // optional will default to (none) 
"nrql.metricName..timestamp": "timestamp", // optional will default to timestamp
"nrql.metricName..batchSize": "5", // optional will default to 50 
"nrql.metricName..startTimestamp": "1569615251000", // optional will default to StartDays if available
"nrql.metricName..startDays": "3", // optional will default to 30
"nrql.metricName..where.clause": "domain in ('xyz')", // optional no default value
"newrelic.poll.interval": "1000" // optional will default to 1000,
"client.connection.timeout.seconds": 30 // optional will default to 30,
"client.request.timeout.seconds": 30 // optional will default to 30,
"client.read.timeout.seconds": 30 // optional will default to 30,
"client.connection.pool.max.idle.connections": 2 // optional will default to 2,
"client.connection.pool.keep.alive.duration.seconds": 30 // optional will default to 60,
"client.request.retries.max": -1 // optional will default to -1 which indicates no limit,
"client.request.retries.backoff.seconds": 30 // optional will default to 30
```
