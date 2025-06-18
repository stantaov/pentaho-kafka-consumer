package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.pentaho.di.core.exception.KettleException;

import java.time.Duration;
import java.util.concurrent.Callable;

/**
 * Kafka reader callable
 *
 * @author Michael Spector
 */
public abstract class KafkaConsumerCallable implements Callable<Object> {

    private KafkaConsumerData data;
    private KafkaConsumerMeta meta;
    private KafkaConsumer step;

    public KafkaConsumerCallable(KafkaConsumerMeta meta, KafkaConsumerData data, KafkaConsumer step) {
        this.meta = meta;
        this.data = data;
        this.step = step;
    }

    /**
     * Called when new message arrives from Kafka stream
     *
     * @param message Kafka message
     * @param key     Kafka key
     */
    protected abstract void messageReceived(byte[] key, byte[] message) throws KettleException;

    public Object call() throws KettleException {
        try {
            long limit;
            String strData = meta.getLimit();

            limit = getLimit(strData);
            if (limit > 0) {
                step.logDebug("Collecting up to " + limit + " messages");
            } else {
                step.logDebug("Collecting unlimited messages");
            }
            while (!data.canceled && (limit <= 0 || data.processed < limit)) {
                ConsumerRecords<?, ?> records = data.consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    step.logDebug("Received an empty poll after " + data.processed + " messages");
                    if (meta.isStopOnEmptyTopic()) {
                        break;
                    } else {
                        continue;
                    }
                }
                for (ConsumerRecord<?, ?> record : records) {
                    byte[] keyBytes = convertToBytes(record.key());
                    byte[] valueBytes = convertToBytes(record.value());
                    messageReceived(keyBytes, valueBytes);
                    ++data.processed;
                    if (limit > 0 && data.processed >= limit) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            throw new KettleException(e);
        }
        // Notify that all messages were read successfully
        data.consumer.commitSync();
        step.setOutputDone();
        return null;
    }

    private long getLimit(String strData) throws KettleException {
        long limit;
        try {
            limit = KafkaConsumerMeta.isEmpty(strData) ? 0 : Long.parseLong(step.environmentSubstitute(strData));
        } catch (NumberFormatException e) {
            throw new KettleException("Unable to parse messages limit parameter", e);
        }
        return limit;
    }

    private byte[] convertToBytes(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        return obj.toString().getBytes();
    }

}
