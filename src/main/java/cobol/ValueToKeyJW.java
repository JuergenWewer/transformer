/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cobol;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ValueToKeyJW<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ValueToKeyJW.class);


    public static final String OVERVIEW_DOC = "Replace the record key with a new key formed from a subset of fields in the record value.";

    public static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, new NonEmptyListValidator(), ConfigDef.Importance.HIGH,
                    "Field names on the record value to extract as the record key.");

    private static final String PURPOSE = "copying fields from value to key";

    private List<String> fields;

    private Cache<Schema, Schema> valueToKeySchemaCache;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
        valueToKeySchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        System.out.println("Record value in VaueToKeyJW.apply to schema: " + record.valueSchema());
//        return applySchemaless(record);
        if (record.valueSchema() == null ){
            return applySchemaless(record);
//        } else if (record.valueSchema().equals(Schema.BYTES_SCHEMA)) {
//            return record;
        } else {
            System.out.println("apply withSchema: record: " + record.toString());
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
//        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
//        final Map<String, Object> key = new HashMap<>(fields.size());
//        for (String field : fields) {
//            key.put(field, value.get(field));
//        }
        System.out.println("Record topic in VaueToKeyJW.applyschemaless: " + record.topic());
        System.out.println("Record kafkaPartition in VaueToKeyJW.applyschemaless: " + record.kafkaPartition());
        System.out.println("Record keySchema in VaueToKeyJW.applyschemaless: " + record.keySchema());
        System.out.println("Record key in VaueToKeyJW.applyschemaless: " + record.key());
        System.out.println("Record valueSchema in VaueToKeyJW.applyschemaless: " + record.valueSchema());
        System.out.println("Record value in VaueToKeyJW.applyschemaless: " + record.value());
        System.out.println("Record timestamp in VaueToKeyJW.applyschemaless: " + record.timestamp());
        log.info("JWNeuRecordBuilder started");
        ECSA2JSONWrapper ecsa2JSONWrapper = new ECSA2JSONWrapper();
        log.info("ECSA2JSONWrapper created");
        String value = "";

//        Record value in VaueToKeyJW.apply to schema: null
//        Record topic in VaueToKeyJW.applyschemaless: jwtest
//        Record kafkaPartition in VaueToKeyJW.applyschemaless: null
//        Record keySchema in VaueToKeyJW.applyschemaless: null
//        Record key in VaueToKeyJW.applyschemaless: null
//        Record valueSchema in VaueToKeyJW.applyschemaless: null
//        Record value in VaueToKeyJW.applyschemaless: VSS207  000002300000023001V1                      VA486   E123456 0000000000                0000VAUD301001VSS21591                0001040000000000000           00000070000000700100test1     00VSS21591          麏⑌⺤㤲㹍㓱❰䊁魱뱄먔在äöüÄÖÜß
//        Record timestamp in VaueToKeyJW.applyschemaless: null
//                [2019-02-11 12:03:34,291] INFO JWNeuRecordBuilder started (cobol.ValueToKeyJW:96)


        try {
            value = ecsa2JSONWrapper.dispatchMessage(record.value().toString());
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), null, null, record.valueSchema(), value, record.timestamp());

    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);

        Schema keySchema = valueToKeySchemaCache.get(value.schema());
        if (keySchema == null) {
            final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
            for (String field : fields) {
                final Schema fieldSchema = value.schema().field(field).schema();
                keySchemaBuilder.field(field, fieldSchema);
            }
            keySchema = keySchemaBuilder.build();
            valueToKeySchemaCache.put(value.schema(), keySchema);
        }

        final Struct key = new Struct(keySchema);
        for (String field : fields) {
            System.out.println("Record value in VaueToKeyJW.applyWithschema field:" +field + " value: " + value.get(field));
            key.put(field, value.get(field));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, value.schema(), value, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        valueToKeySchemaCache = null;
    }

}
