package com.ibm.connect.newrelic;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewRelicSourceTask extends SourceTask {

    static final Logger log = LoggerFactory.getLogger(NewRelicSourceTask.class);

    private Map<String, String> _props;
    private NewRelicHttpClient _client;

    public static final String LAST_READ = "last_read";
    public static final String LAST_TIMESTAMP = "last_timestamp";

    private Long fromDate;
    private Long last_execution = 0L;
    private Long interval;

    private Schema _cachedValueSchema;
    private Map<String, PartitioningSchema> _cachedPartitioningSchema;

    private String TARGET_TOPIC;

    private NewRelicSourceConnectorConfig _config;

    @Override
    public String version() {
        return "0.0.0.0";
    }

    public NewRelicSourceTask() {
    }

    private void setupTaskConfig() {
        interval = _config.getPollInterval();
        this.TARGET_TOPIC = _config.getSinkTopic();

        String metric = _config.getMetricName();
        Long startTimestamp = null;
        try {
            startTimestamp = Long.parseLong(_config.getNrqlQueryStartTimestamp());
        } catch (NumberFormatException e) {
            log.debug("{}", e);
        }

        if(startTimestamp != null) {
            log.debug("Found starting timestamp [{}] for metric [{}] so using it", startTimestamp, metric);
            fromDate = startTimestamp;
        } else {
            log.debug("NO starting timestamp for metric [{}] so using start days", metric);
            fromDate = Instant.now()
                        .plus(-1 * _config.getStartDays(), ChronoUnit.DAYS)
                        .toEpochMilli();
        }
    }

    private void setupSourceClient() {
        this._client = new NewRelicHttpClient(_config);
        try {
            this._client.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method’s responsibility also includes ensuring the task is able to start
     * up from the last processed point, in the event of a failure or restart. In
     * concept this is handled by attaching a reference offset to where you are in
     * the source system with each record you pass to Kafka. Then on start, checking
     * what the last processed record’s offset was and starting from that point.
     */
    @Override
    public void start(Map<String, String> props) {
        log.debug("Starting SourceTask");
        _props = props;

        _props.forEach( (k,v) -> log.debug("k:[{}] v:[{}]", k, Utils.sanitize(k, v)));

        _config = new NewRelicSourceConnectorConfig(props);

        // 1. Read in the url, api key and any other config details for NewRelic
        setupTaskConfig();
        // 2. Setup http client connection ref to NewRelic using #1
        setupSourceClient();
        // 3. Determin/Build partitions our work will be divided into
        // 4. Figure out our offset(s) for the partitions identified in #3 by reading
        // fron context.offsetStorageReader
        /**
         * Get the offset for the specified partition. If the data isn't already
         * available locally, this gets it from the backing store, which may require
         * some network round trips.
         *
         * @param partition object uniquely identifying the partition of data
         * @return object uniquely identifying the offset in the partition of data
         */
        // Map<String, Object> context.offsetStorageReader().offset(Map<String, T>
        // partition)

        /**
         * Get a set of offsets for the specified partition identifiers. This may be
         * more efficient than calling {@link #offset(Map)} repeatedly.
         * 
         * @param partitions set of identifiers for partitions of data
         * @return a map of partition identifiers to decoded offsets
         */
        // <T> Map<Map<String, T>, Map<String, Object>>
        // context.offsetStorageReader().offsets(Collection<Map<String, T>> partitions)
        log.debug("Trying to get persisted properties");
        Map<String, Object> persistedProperties = null;
        if (context != null && context.offsetStorageReader() != null) {
            HashMap<String, Object> partitionInfo = new HashMap<>(1);
            partitionInfo.put("partition", _config.getMetricName());
            persistedProperties = context.offsetStorageReader().offset(partitionInfo);
        }
        log.debug("The persisted properties are [{}]", persistedProperties);
        if (persistedProperties != null) {
            Long lastTimestamp = (Long) persistedProperties.get(LAST_TIMESTAMP);
            if (lastTimestamp != null) {
                fromDate = lastTimestamp;
            }
        }
        // 5. Once we have our offset(s) these will be used by the poll() method below
    }

    /**
     * This function is where all your actual “work” happens. Kafka will continually
     * call it in loop making the calls to the external system and then returning a
     * list of SourceRecords to the Connect framework convert the newrelic objects
     * into SourceRecords and stored the offset(s)
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // 0. Should we work or wait
        if (System.currentTimeMillis() > (last_execution + interval)) {
            last_execution = System.currentTimeMillis();
            // 1. Get data from NewRelic
            // 2. Set the internal offset(s) variables based on the new data from newrelic
            // for the next poll to continue from
            // this(ese) offset(s)
            // 3. Create List<SourceRecords> from #1
            // Each SourceRecord will keep the offset timestamp
            // new SourceRecord(sourcePartition, sourceOffset, topic, RecordSchema,
            // Record.toStruct());
            // 4. Return data or empty list if none
            List<SourceRecord> records = null;
            try {
                log.info("Polling new relic...");
                records = getBatch();
            } catch (IOException e) {
                e.printStackTrace();
                throw new ConnectException("An error occurred while trying to poll.", e);
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.info("got [{}] records", records != null ? records.size() : "NO");

            return records;
        } else {
            log.debug("Sleep more...");
            Thread.sleep(10000);
        }
        return Collections.emptyList();
    }

    @Override
    public void stop() {
        log.info("Stopping");
    }

    private List<SourceRecord> getBatch() throws IOException, InterruptedException {
        // matches the fromDate or timestamp of the query
        List<JSONObject> rawRecords = this._client.getRecords(fromDate);
        List<SourceRecord> records = new ArrayList<SourceRecord>(rawRecords.size());
      
        String values = _config.getNrqlQueryValueColumns().replaceAll("[a-zA-Z.]+:", "");
        String[] valuesList = values.split(",");        
        String metadataCols = _config.getNrqlQueryMetadataColumns().replaceAll("[a-zA-Z.]+:", "");

        // Build & Get Schema
        if (this._cachedValueSchema == null) {
            log.debug("-> CReating Value schema");
            this._cachedValueSchema = SchemaHelper.buildSchemaFromConfig(metadataCols);
        } else {
            log.debug("-> Value schema already created");
        }
        if(this._cachedPartitioningSchema == null) {
            log.debug("-> CReating Partitioning schema");
            this._cachedPartitioningSchema = SchemaHelper.buildSchemaForPartitioning(values);
        } else {
            log.debug("-> Partitioning schema already created");
        }
        
        for (JSONObject result : rawRecords) {
            log.debug("result\n{}", result);

            Long rawTimestamp = result.getLong("timestamp");
            log.debug("-> row:timestamp:[{}]", rawTimestamp);

            for(String value : valuesList) {
                log.debug("getBatch: working on [{}]", value);
                if(Utils.empty(result.optString(value))) {
                    // Drop empty values
                    log.debug("-> dropping empty value");
                    continue;
                }

                // Build DTO object and then create SourceRecord from it
                SourceRecord record = SchemaHelper.GetInstance()
                        .withAccount(_config.getSinkAccount())
                        .withMetricName(_config.getMetricFqn(value))
                        .withMetaData(metadataCols, _config.getEmptyDimValue())
                        .withValueColumn(value)
                        .withTimestampColumn(_config.getNrqlQueryTimestampColumn())
                        .withSourcePartition(getPartition())
                        .withOffset(buildSourceOffset(Long.toString(rawTimestamp)))
                        .withTopic(this.TARGET_TOPIC)
                        .withValueSchema(this._cachedValueSchema)
                        .withPartitioningSchema(this._cachedPartitioningSchema)
                        .withRow(result)
                        .build();
                log.debug("getBatch: Built record for [{}]", value);
                records.add(record);
            }
            fromDate = rawTimestamp;
        }

        log.debug("getBatch: got records collection");
        return records;
    }

    private Map<String, Object> getPartition() {
        HashMap<String, Object> partitionInfo = new HashMap<>(1);
        String metricName = _config.getMetricName();
        partitionInfo.put("partition", metricName);
        log.debug("\n\ngetPartition returning [{}]\n\n", metricName);
        return partitionInfo;
    }

    private Map<String, Object> buildSourceOffset(String lastRead) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put(LAST_READ, lastRead);
        return sourceOffset;
    }
}