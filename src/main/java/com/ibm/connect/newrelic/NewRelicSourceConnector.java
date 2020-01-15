package com.ibm.connect.newrelic;

import static com.ibm.connect.newrelic.NewRelicSourceConnectorConfig.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewRelicSourceConnector extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger(NewRelicSourceConnector.class);

    private NewRelicSourceConnectorConfig _config;                                                                                                 

    @Override
    public String version() {
        return "0.1.0.0";
    }

    @Override
    public void start(Map<String, String> map) {
        log.info("Starting [{}].", NewRelicSourceConnector.class.getName());
        this._config = new NewRelicSourceConnectorConfig(map);
        log.debug("after new nrsrccon config");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return NewRelicSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        String metricNames = _config.getMetricName();
        String[] metriclist = metricNames.split(",");

        log.info("-> doing [{}] metrics 1 per task", metriclist.length);
        
        List<Map<String, String>> configs = new ArrayList<>(metriclist.length);

        for(String metric : metriclist) {
            Map<String, String> config = new HashMap<>();
            log.info("taskConfigs [{}]", metric);

            // common config
            config.put(NEWRELIC_ACCOUNTID, _config.getAccount());
            config.put(SINK_ACCOUNT, _config.getSinkAccount());
            config.put(NRQL_APIKEY, _config.getApiKey());
            config.put(NRQL_BASEURL, _config.getBaseUrl());
            config.put(POLL_INTERVAL, _config.getPollInterval().toString());
            config.put(NRQL_METRIC_PREFIX, _config.getMetricPrefix());
            config.put(NRQL_QUERY_BATCH_SIZE_GLOBAL, _config.getNrqlQueryBatchSize().toString());
            config.put(NRQL_QUERY_STARTDAYS_GLOBAL, _config.getStartDays().toString());
            config.put(NRQL_QUERY_START_TIMESTAMP_GLOBAL, _config.getNrqlQueryStartTimestamp());
            config.put(NRQL_QUERY_TIMESTAMP_GLOBAL, _config.getNrqlQueryTimestampColumn());
            config.put(NRQL_QUERY_METADATA_EMPTY_DIM_VALUE, _config.getEmptyDimValue());
            
            // which query/metric
            config.put(NRQL_METRICNAME, metric);

            // query/metric specific
            config.put(TOPIC_PREFIX, _config.getSinkTopic() + "-" + metric);
            config.putAll((Map) this._config.originalsWithPrefix(NRQL_METRICNAME + "." + metric + ".", false));

            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping [{}].", NewRelicSourceConnector.class.getName());
    }

    @Override
    public ConfigDef config() {
        return NewRelicSourceConnectorConfig.Config_Def;
    }
}
