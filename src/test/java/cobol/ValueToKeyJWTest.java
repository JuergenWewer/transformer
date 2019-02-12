package cobol;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ValueToKeyJWTest {

    //to run the test, in the project directory the subdirectory copybooks must exist

    private static final Logger log = LoggerFactory.getLogger(ValueToKeyJW.class);

    private ValueToKeyJW cut = new ValueToKeyJW();

    @Test
    public void applySchemalessTest() {


        final String topic = "testTransformer";
        final Integer kafkaPartition =0;
        final Schema keySchema = null;
        Object key = null;
        Schema valueSchema = null;
        Object value = "VSS207  000002300000023001V1                      VA486   E123456 0000000000                0000VAUD301001VSS21591                0001040000000000000           00000070000000700100test1     00VSS21591          麏\u244C⺤㤲㹍㓱❰䊁魱뱄\uEB39먔在äöüÄÖÜß";
        Long timestamp= null;

        TestRecord record = new TestRecord(topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp);
        record = (cobol.TestRecord) cut.apply(record);

        log.info("Result: %s", record.toString());

    }

}
