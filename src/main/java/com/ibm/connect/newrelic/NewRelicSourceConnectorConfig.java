package com.ibm.connect.newrelic;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewRelicSourceConnectorConfig extends AbstractConfig {
    private static Logger log = LoggerFactory.getLogger(NewRelicSourceConnectorConfig.class);

    public static final String NEWRELIC_ACCOUNTID = "newrelic.accountId";
    private static final String NEWRELIC_ACCOUNTID_DOC = "New Relic Customer AccountID";

    public static final String SINK_ACCOUNT = "sink.account";
    private static final String SINK_ACCOUNT_DOC = "Account or similar attribute to pass through to the sink topic";

    public static final String NRQL_APIKEY = "nrql.apikey";
    private static final String NRQL_APIKEY_DOC = "Insights query API Key";

    public static final String NRQL_METRICNAME = "nrql.metricName";
    private static final String NRQL_METRICNAME_DOC = "NRQL list of metric/query names";

    public static final String NRQL_METRIC_PREFIX = "nrql.metric.prefix";
    private static final String NRQL_METRIC_PREFIX_DOC = "metric prefix";

    public static final String NRQL_BASEURL = "nrql.query.baseUrl";
    private static final String NRQL_BASEURL_DOC = "NRQL Base Url";

    public static final String NRQL_QUERY_METADATA_EMPTY_DIM_VALUE = "nrql.query.metadata.empty.dim.value";
    public static final String NRQL_QUERY_METADATA_EMPTY_DIM_VALUE_DEFAULT = "(none)";
    private static final String NRQL_QUERY_METADATA_EMPTY_DIM_VALUE_DOC = "NRQL Query Metadata empty value";    

    public static final String NRQL_QUERY_METRIC_NAME = "metricName";
    private static final String NRQL_QUERY_METRIC_NAME_DOC = "NRQL Query Metric Name override";

    public static final String NRQL_QUERY_TABLE = "table";
    private static final String NRQL_QUERY_TABLE_DOC = "NRQL Query Table";

    public static final String NRQL_QUERY_TIMESTAMP = "timestamp";
    private static final String NRQL_QUERY_TIMESTAMP_DOC = "Timestamp column used to track offsets";

    public static final String NRQL_QUERY_TIMESTAMP_GLOBAL = "nrql.query.timestamp.column";
    public static final String NRQL_QUERY_TIMESTAMP_GLOBAL_DEFAULT = "timestamp";
    private static final String NRQL_QUERY_TIMESTAMP_GLOBAL_DOC = "Global default Timestamp column used to track offsets";

    public static final String NRQL_QUERY_METADATA = "metadata";
    private static final String NRQL_QUERY_METADATA_DOC = "NRQL Query Metadata columns";
    
    public static final String NRQL_QUERY_METADATA_LABELS = "metadata.labels";
    private static final String NRQL_QUERY_METADATA_LABELS_DOC = "NRQL Query Metadata aliases";

    public static final String NRQL_QUERY_VALUE = "value";
    private static final String NRQL_QUERY_VALUE_DOC = "NRQL Query Value column";

    public static final String NRQL_QUERY_BATCH_SIZE = "batchSize";
    public static final int NRQL_QUERY_BATCH_SIZE_DEFAULT = 50;
    private static final String NRQL_QUERY_BATCH_SIZE_DOC = "NRQL Query batch size for requests";

    public static final String NRQL_QUERY_BATCH_SIZE_GLOBAL = "nrql.query.batchSize";
    public static final String NRQL_QUERY_BATCH_SIZE_GLOBAL_DEFAULT = "";
    private static final String NRQL_QUERY_BATCH_SIZE_GLOBAL_DOC = "Global NRQL Query batch size for requests";

    public static final String NRQL_QUERY_START_TIMESTAMP = "startTimestamp";
    private static final String NRQL_QUERY_START_TIMESTAMP_DOC = "NRQL Query start timestamp";

    public static final String NRQL_QUERY_START_TIMESTAMP_GLOBAL = "nrql.query.start.timestamp";
    public static final String NRQL_QUERY_START_TIMESTAMP_GLOBAL_DEFAULT = "";
    private static final String NRQL_QUERY_START_TIMESTAMP_GLOBAL_DOC = "Global NRQL Query start timestamp";

    public static final String NRQL_QUERY_STARTDAYS = "startDays";
    private static final String NRQL_QUERY_STARTDAYS_DOC = "NRQL Query start from days back";    
    private static final String NRQL_QUERY_STARTDAYS_DEFAULT = "30";
    private static final Integer NRQL_QUERY_STARTDAYS_DEFAULT_INT = 30;
    
    public static final String NRQL_QUERY_STARTDAYS_GLOBAL = "nrql.query.startDays";
    public static final String NRQL_QUERY_STARTDAYS_GLOBAL_DEFAULT = "";
    private static final String NRQL_QUERY_STARTDAYS_GLOBAL_DOC = "Global NRQL Query start from days back";    

    public static final String TOPIC_PREFIX = "sink.topic";
    private static final String TOPIC_PREFIX_DOC = "Name of sink topic";

    public static final String POLL_INTERVAL = "newrelic.poll.interval";
    private static final Long POLL_INTERVAL_DEFAULT = 1000l;
    private static final String POLL_INTERVAL_DOC = "Poll interval in milliseconds.";

    public static final String NRQL_QUERY_WHERE_CLAUSE = "where.clause";
    private static final String NRQL_QUERY_WHERE_CLAUSE_DOC = "optional where clause";

    public static final String HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS = "client.connection.timeout.seconds";
    private static final String HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS_DOC
            = "The amount of time in seconds that the client will wait while establishing a connection.";
    public static final int HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS_DEFAULT = 30;

    public static final String HTTP_CLIENT_TIMEOUT_SECONDS = "client.request.timeout.seconds";
    private static final String HTTP_CLIENT_TIMEOUT_SECONDS_DOC
            = "The amount of time in seconds that the client will wait for a response from the rest endpoint.";
    public static final int HTTP_CLIENT_TIMEOUT_SECONDS_DEFAULT = 30;

    public static final String HTTP_CLIENT_READ_TIMEOUT_SECONDS = "client.read.timeout.seconds";
    private static final String HTTP_CLIENT_READ_TIMEOUT_SECONDS_DOC
            = "The amount of time in seconds that the client will wait for data.";
    public static final int HTTP_CLIENT_READ_TIMEOUT_SECONDS_DEFAULT = 30;

    public static final String HTTP_CLIENT_MAX_IDLE_CONNECTIONS = "client.connection.pool.max.idle.connections";
    private static final String HTTP_CLIENT_MAX_IDLE_CONNECTIONS_DOC
            = "The maximum number of idle connections to hold in the connection pool.";
    public static final int HTTP_CLIENT_MAX_IDLE_CONNECTIONS_DEFAULT = 2;

    public static final String HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS = "client.connection.pool.keep.alive.duration.seconds";
    private static final String HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS_DOC
            = "The amount of time to hold onto idle connections.";
    public static final int HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS_DEFAULT = 60;

    public static final String HTTP_CLIENT_MAX_RETRIES = "client.request.retries.max";
    private static final String HTTP_CLIENT_MAX_RETRIES_DOC
            = "The number of times the connector task will retry a request before entering a failed state. If not specified, or a '-1' is used, then there is no retry limit.";
    public static final int HTTP_CLIENT_MAX_RETRIES_DEFAULT = -1;

    public static final String HTTP_CLIENT_RETRY_BACKOFF_SECONDS = "client.request.retries.backoff.seconds";
    private static final String HTTP_CLIENT_RETRY_BACKOFF_SECONDS_DOC
            = "The amount of time in seconds that the connector task will wait before retrying after a request failure.";
    public static final int HTTP_CLIENT_RETRY_BACKOFF_SECONDS_DEFAULT = 30;

    public static final ConfigDef Config_Def = config();

    public NewRelicSourceConnectorConfig(Map<?, ?> originals) {
        super(config(), originals);
    }

    public static ConfigDef config() {
        ConfigDef config = new ConfigDef();
        addCommonConfigs(config);
        return config;
    }

    public static void addCommonConfigs(ConfigDef config) {
        config
            .define(NEWRELIC_ACCOUNTID, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    NEWRELIC_ACCOUNTID_DOC)
            .define(SINK_ACCOUNT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    SINK_ACCOUNT_DOC)
            .define(NRQL_APIKEY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    NRQL_APIKEY_DOC)            
            .define(NRQL_BASEURL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    NRQL_BASEURL_DOC)            
            .define(NRQL_QUERY_METADATA_EMPTY_DIM_VALUE, ConfigDef.Type.STRING, NRQL_QUERY_METADATA_EMPTY_DIM_VALUE_DEFAULT, ConfigDef.Importance.LOW,
                    NRQL_QUERY_METADATA_EMPTY_DIM_VALUE_DOC)
            .define(TOPIC_PREFIX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, 
                    TOPIC_PREFIX_DOC)
            .define(NRQL_METRIC_PREFIX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    NRQL_METRIC_PREFIX_DOC)            
            .define(NRQL_METRICNAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    NRQL_METRICNAME_DOC)            
            .define(POLL_INTERVAL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW,
                    POLL_INTERVAL_DOC)
            .define(HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS, ConfigDef.Type.INT, HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_CONNECTION_TIMEOUT_SECONDS_DOC)
            .define(HTTP_CLIENT_TIMEOUT_SECONDS, ConfigDef.Type.INT, HTTP_CLIENT_TIMEOUT_SECONDS_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_TIMEOUT_SECONDS_DOC)
            .define(HTTP_CLIENT_READ_TIMEOUT_SECONDS, ConfigDef.Type.INT, HTTP_CLIENT_READ_TIMEOUT_SECONDS_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_READ_TIMEOUT_SECONDS_DOC)
            .define(HTTP_CLIENT_MAX_IDLE_CONNECTIONS, ConfigDef.Type.INT, HTTP_CLIENT_MAX_IDLE_CONNECTIONS_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_MAX_IDLE_CONNECTIONS_DOC)
            .define(HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS, ConfigDef.Type.INT, HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_KEEP_ALIVE_DURATION_SECONDS_DOC)
            .define(HTTP_CLIENT_MAX_RETRIES, ConfigDef.Type.INT, HTTP_CLIENT_MAX_RETRIES_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_MAX_RETRIES_DOC)
            .define(HTTP_CLIENT_RETRY_BACKOFF_SECONDS, ConfigDef.Type.INT, HTTP_CLIENT_RETRY_BACKOFF_SECONDS_DEFAULT, ConfigDef.Importance.LOW,
                    HTTP_CLIENT_RETRY_BACKOFF_SECONDS_DOC)
            .define(NRQL_QUERY_BATCH_SIZE_GLOBAL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, 
                    NRQL_QUERY_BATCH_SIZE_GLOBAL_DOC)
            .define(NRQL_QUERY_TIMESTAMP_GLOBAL, ConfigDef.Type.STRING, NRQL_QUERY_TIMESTAMP_GLOBAL_DEFAULT, ConfigDef.Importance.LOW, 
                    NRQL_QUERY_TIMESTAMP_GLOBAL_DOC)
            .define(NRQL_QUERY_START_TIMESTAMP_GLOBAL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, 
                    NRQL_QUERY_START_TIMESTAMP_GLOBAL_DOC)
            .define(NRQL_QUERY_STARTDAYS_GLOBAL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, 
                    NRQL_QUERY_STARTDAYS_GLOBAL_DOC);
    }
    
    @Override
    protected Map<String,Object> postProcessParsedConfig(Map<String,Object> parsedValues) {
        log.debug("Loading query specific vals in post proc");
        parsedValues.forEach( (k,v) -> log.debug("k:[{}] v:[{}]", k, Utils.sanitize(k, v)));
        String metricNames = parsedValues.get(NRQL_METRICNAME).toString();
        String[] metriclist = metricNames.split(",");

        HashMap<String, Object> properties = new HashMap<>();

        for(String metric : metriclist) {
            log.info("Processing [{}] of [{}]", metric, metricNames);

            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_METRIC_NAME)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_METRIC_NAME),
                        ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, NRQL_QUERY_METRIC_NAME_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_TABLE))) 
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_TABLE), 
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, NRQL_QUERY_TABLE_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_TIMESTAMP)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_TIMESTAMP), 
                    ConfigDef.Type.LONG, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW, NRQL_QUERY_TIMESTAMP_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_METADATA)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_METADATA), 
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, NRQL_QUERY_METADATA_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_METADATA_LABELS)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_METADATA_LABELS), 
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, NRQL_QUERY_METADATA_LABELS_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_VALUE)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_VALUE), 
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, NRQL_QUERY_VALUE_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_BATCH_SIZE)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_BATCH_SIZE), 
                    ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW, NRQL_QUERY_BATCH_SIZE_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_START_TIMESTAMP)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_START_TIMESTAMP), 
                    ConfigDef.Type.LONG, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW, NRQL_QUERY_START_TIMESTAMP_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_STARTDAYS)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_STARTDAYS), 
                    ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.LOW, NRQL_QUERY_STARTDAYS_DOC);
            if(!Config_Def.configKeys().containsKey(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_WHERE_CLAUSE)))
                Config_Def.define(String.format("%s.%s.%s", NRQL_METRICNAME, metric, NRQL_QUERY_WHERE_CLAUSE), 
                    ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, NRQL_QUERY_WHERE_CLAUSE_DOC);

            this.originals().forEach( (k,v) -> {
                if(k.startsWith(NRQL_METRICNAME + "." + metric + ".")) {
                    properties.put(k, v.toString());
                }
            });
        }

        return properties;
    }

    Map<String, String> returnPropertiesWithDefaultsValuesIfMissing() {
        Map<String, ?> uncastProperties = this.values();
        Map<String, String> config = new HashMap<>(uncastProperties.size());
        uncastProperties.forEach((key, valueToBeCast) -> config.put(key, valueToBeCast.toString()));

        return config;
    }

    public String getAccount() {
        return this.getString(NEWRELIC_ACCOUNTID);
    }

    public String getSinkAccount() {
        return this.getString(SINK_ACCOUNT);
    }
    public String getApiKey() {
        return this.getString(NRQL_APIKEY);
    }

	public String getMetricName() {
		return this.getString(NRQL_METRICNAME);
    }

	public String getBaseUrl() {
		return this.getString(NRQL_BASEURL);
    }

    public String getMetricFqn(String subMetric) {
        try {
            return String.format("%s.%s.%s", this.getMetricPrefix(), this.getMetricKey(NRQL_QUERY_METRIC_NAME), subMetric);
        } catch(Exception e){
            return String.format("%s.%s.%s", this.getMetricPrefix(), this.getMetricName(), subMetric);
        }
    }

    public String getNrqlQueryTable() {
        return this.getMetricKey(NRQL_QUERY_TABLE);
    }
    
    // Global default
    public String getNrqlQueryTimestampColumn() {
        String tsCol = this.getMetricKey(NRQL_QUERY_TIMESTAMP);
        String tsColGlobal = this.getString(NRQL_QUERY_TIMESTAMP_GLOBAL);

        log.debug("-> get tsCol [{}] tsColGlobal [{}]", tsCol, tsColGlobal);

        if(Utils.empty(tsCol) && !Utils.empty(tsColGlobal)) {
            log.debug("-> get tsCol empty so using tsColGlobal");
            return tsColGlobal;
        }

        log.debug("-> get tsCol returning tsCol [{}]", tsCol);
        return tsCol;
    }

    // Global default
    public String getNrqlQueryStartTimestamp() {
        String startTS = this.getMetricKey(NRQL_QUERY_START_TIMESTAMP);
        log.debug("-> getNrqlQueryStartTimestamp [{}]", startTS);
        if(Utils.empty(startTS)) {
            startTS = this.getString(NRQL_QUERY_START_TIMESTAMP_GLOBAL);
            log.debug("-> getNrqlQueryStartTimestamp global [{}]", startTS);
        }

        return startTS;
    }

    public String getNrqlQueryMetadataColumns() {
        return this.getMetricKey(NRQL_QUERY_METADATA).replaceAll(" ", "");
    }

    public String getNrqlQueryValueColumns() {
        return this.getMetricKey(NRQL_QUERY_VALUE).replaceAll(" ", "");
    }    

    public String getSinkTopic() {
        return this.getString(TOPIC_PREFIX);
    }
   
    public Long getPollInterval() {
        try {
            return Long.parseLong(this.getString(POLL_INTERVAL));
        } catch (NumberFormatException e) {
            return POLL_INTERVAL_DEFAULT;
        }
    }

    // Global default
    public Integer getNrqlQueryBatchSize() {
        String batchSize = this.getMetricKey(NRQL_QUERY_BATCH_SIZE);
        log.debug("-> NRQL_QUERY_BATCH_SIZE [{}]", batchSize);
        if(Utils.empty(batchSize)) {
            batchSize = this.getString(NRQL_QUERY_BATCH_SIZE_GLOBAL);
            log.debug("-> GLOBAL NRQL_QUERY_BATCH_SIZE [{}]", batchSize);
        }

        try {
            return Integer.parseInt(batchSize);
        } catch (NumberFormatException e) {
            log.debug("-> parse exception");
            return NRQL_QUERY_BATCH_SIZE_DEFAULT;
        }
    }

    // Global default
    public Integer getStartDays() {
        log.debug("\n\nstartdays from originals: {}\n\n", this.originals().get(NRQL_QUERY_STARTDAYS_GLOBAL));
        String startDays = this.getMetricKey(NRQL_QUERY_STARTDAYS);
        if(Utils.empty(startDays)) {
            startDays = (String) this.originals().getOrDefault(NRQL_QUERY_STARTDAYS_GLOBAL, NRQL_QUERY_STARTDAYS_DEFAULT);
        }

        try {
            return Integer.parseInt(startDays);
        } catch (NumberFormatException e) {
            return NRQL_QUERY_STARTDAYS_DEFAULT_INT;
        }
    }

    private String getMetricKey(String metricKey) {
        this.values().forEach((k, v) -> {
            log.debug("-> values().k[{}] v[{}]", k,v);
        });        
        String metric = this.getString(NRQL_METRICNAME);
        log.debug("-> {}:1: getMetricKey [{}]", metric, metricKey);

        String value = null;
        try {
            String keyName = String.format("%s.%s.%s", NRQL_METRICNAME, metric, metricKey);
            log.debug("-> {}:2: keyName [{}]", metric, keyName);

            if (this.values().containsKey(keyName)) {
                value = this.getString(keyName);
            } else {
                log.debug("-> key [{}] not found", keyName);
            }
        } catch (ConfigException ex) {
            log.error("possible missing config \n{}", ex.toString());
        }
        return value;
    }

    public String getMetricPrefix() {
        return this.getRequiredConfig(NRQL_METRIC_PREFIX);
    }

    private String getRequiredConfig(String key) {
        final String candidate = this.getString(key);
        if(candidate == null || candidate.trim().length() == 0) {
            throw new RuntimeException(String.format("Missing [%s] configuration.", key));
        }

        return candidate;
    }

	public String getNrqlQueryWhereClause() {
		return this.getMetricKey(NRQL_QUERY_WHERE_CLAUSE);
	}

	public String getEmptyDimValue() {
        String retVal = this.getString(NRQL_QUERY_METADATA_EMPTY_DIM_VALUE);
        if(Utils.empty(retVal)) {
            return NRQL_QUERY_METADATA_EMPTY_DIM_VALUE_DEFAULT;
        }

        return retVal;
	}
}
