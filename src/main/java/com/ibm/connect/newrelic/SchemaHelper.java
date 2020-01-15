package com.ibm.connect.newrelic;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaHelper {
    static final Logger log = LoggerFactory.getLogger(NewRelicSourceTask.class);

    private Schema _valueSchema;
    private Map<String, PartitioningSchema> _partitioningSchema;
    private String _topic;
    private String _account;
    private String _metricName;
    private String _metaData;
    private String _emptyDimValue;
    private Map<String, Object> _offset;
    private Map<String, Object> _sourcePartition;
    private JSONObject _row;

    private String _timestampColumn;
    private String _valueColumn;


    public static SchemaHelper GetInstance() {
        return new SchemaHelper();
    }

    public SchemaHelper withAccount(String account) {
        this._account = account;
        return this;
    }
    
    public SchemaHelper withMetricName(String metricName) {
        this._metricName = metricName;
        return this;
    }

    public SchemaHelper withMetaData(String metaData, String emptyDimValue) {
        this._metaData = metaData;
        this._emptyDimValue = emptyDimValue;
        return this;
    }

    public SchemaHelper withValueSchema(Schema valueSchema) {
        this._valueSchema = valueSchema;
        return this;
    }

	public SchemaHelper withPartitioningSchema(Map<String, PartitioningSchema> partitioningSchema) {
        this._partitioningSchema = partitioningSchema;
		return this;
    }
    
    public SchemaHelper withTopic(String topic) {
        this._topic = topic;
        return this;
    }

    public SchemaHelper withTimestampColumn(String timestampColumn) {
        this._timestampColumn = timestampColumn;
        return this;
    }

    public SchemaHelper withValueColumn(String valueColumn) {
        this._valueColumn = valueColumn;
        return this;
    }
    
    public SchemaHelper withOffset(Map<String, Object> offset) {
        this._offset = offset;
        return this;
    }

    public SchemaHelper withRow(JSONObject row) {
        this._row = row;
        return this;
    }

    public SchemaHelper withSourcePartition(Map<String, Object> sourcePartition) {
        this._sourcePartition = sourcePartition;
        return this;
    }

    public SourceRecord build() {
        log.debug("-> Trying to build SourceRecord");

        return new SourceRecord(
                this._sourcePartition, 
                this._offset, 
                this._topic, 
                null, 
                this._partitioningSchema.get(this._valueColumn).keySchema,
                this._partitioningSchema.get(this._valueColumn).key,
                this._valueSchema,
                buildStruct(this._valueSchema, this._row, this._timestampColumn, this._valueColumn));
    }

    // Values
    public Struct buildStruct(Schema schema, 
                              JSONObject row, 
                              String timestampColumn, 
                              String valueColumn) {
        log.debug("-> in buildStruct called from build") ;

        Struct value = new Struct(schema)
            .put("metricName", _metricName)
            .put("sampleTime", row.getLong(timestampColumn))
            .put("account", _account)
            .put("value", Utils.cleanTextContent(row.optString(valueColumn)))
            .put("metadata", getMetaDataStruct(schema, row));

        log.debug("buildStruct - done with getting values from records");
        return value;
    }

    private Struct getMetaDataStruct(Schema schema, JSONObject row) {
        log.debug("getMetaDataStruct [{}]", _metaData) ;

        String[] metaDataColumns = _metaData.split(",");
        if(metaDataColumns.length == 0) {
            throw new ConnectException("Metadata columns need to be provided, comma separated");
        }

        Struct metaDataStruct = new Struct(schema.field("metadata").schema());

        for(String col: metaDataColumns) {
            metaDataStruct = metaDataStruct.put(col, row.optString(col, _emptyDimValue));
        }

        return metaDataStruct;
    }

    // Get the different pieces we need to build the schema with
    public static Schema buildSchemaFromConfig(String metaData) throws InterruptedException {
        log.debug("buildschemafrom config [{}]", metaData) ;

        String[] metaDataColumns = metaData.split(",");
        if(metaDataColumns.length == 0) {
            throw new ConnectException("Metadata columns need to be provided, comma separated");
        }

        SchemaBuilder metaDataSchema = SchemaBuilder.struct().name("metaData");

        for(String col: metaDataColumns) {
            metaDataSchema = metaDataSchema.field(col, Schema.STRING_SCHEMA);
        }

        SchemaBuilder builder = SchemaBuilder.struct()
            .field("metricName", Schema.STRING_SCHEMA)
            .field("sampleTime", Schema.INT64_SCHEMA)
            .field("account", Schema.STRING_SCHEMA)
            .field("value", Schema.STRING_SCHEMA)
            .field("metadata", metaDataSchema.build());

        Schema valSchema = builder.build();
        log.debug("-> valSchema | {}", valSchema.fields().toString());
        log.debug("-> valSchema.METADATA | {}", valSchema.fields().get(valSchema.fields().size()-1).schema().toString());
        return valSchema;
    }

	public static Map<String, PartitioningSchema> buildSchemaForPartitioning(String values) {
        log.debug("build Partitioning schema for: [{}]", values);

        String valuesList[] = values.split(",");
        if(valuesList.length == 0) {
            throw new ConnectException("Value columns need to be provided as they are used for partitioning as well");
        }

        Map<String, PartitioningSchema> pSchemas = new HashMap<String, PartitioningSchema>();

        Schema keySchema = SchemaBuilder.struct().field("metricname", Schema.STRING_SCHEMA).build();        
        log.debug("-> keySchema | {}", keySchema.fields().toString());
        
        for(String val: valuesList) {
            Struct key = new Struct(keySchema);
            key.put("metricname", val);
            pSchemas.put(val, new PartitioningSchema(key, keySchema));
        }

        log.debug("-> created partitioning schema [{}]", pSchemas.size());

        return pSchemas;
	}
}