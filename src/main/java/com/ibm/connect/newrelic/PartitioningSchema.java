package com.ibm.connect.newrelic;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class PartitioningSchema {
    public Struct key;
    public Schema keySchema;

    public PartitioningSchema(Struct key, Schema keySchema) {
        this.key = key;
        this.keySchema = keySchema;
    }
}