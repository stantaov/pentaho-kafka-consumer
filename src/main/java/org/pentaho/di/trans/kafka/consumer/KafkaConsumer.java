package org.pentaho.di.trans.kafka.consumer;

// import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Kafka Consumer step processor
 *
 * @author Michael Spector
 */
public class KafkaConsumer extends BaseStep implements StepInterface {
    public static final String POLL_DURATION_KEY = "poll.duration.ms";

    public KafkaConsumer(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                         Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        super.init(smi, sdi);

        KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
        KafkaConsumerData data = (KafkaConsumerData) sdi;

        Properties properties = meta.getKafkaProperties();
        Properties substProperties = new Properties();
        for (Entry<Object, Object> e : properties.entrySet()) {
            substProperties.put(e.getKey(), environmentSubstitute(e.getValue().toString()));
        }
        if (meta.isStopOnEmptyTopic()) {

            // If there isn't already a provided value, set a default of 1s
            if (!substProperties.containsKey(POLL_DURATION_KEY)) {
                substProperties.put(POLL_DURATION_KEY, "1000");
            }
        } else {
            if (substProperties.containsKey(POLL_DURATION_KEY)) {
                logError(Messages.getString("KafkaConsumer.WarnConsumerTimeout"));
            }
        }

        if (!substProperties.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            substProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }
        if (!substProperties.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            substProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        }

        data.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<Object, Object>(substProperties);
        String topic = environmentSubstitute(meta.getTopic());
        data.consumer.subscribe(Arrays.asList(topic));

        logBasic(Messages.getString("KafkaConsumer.CreateKafkaConsumer.Message", substProperties.getProperty("bootstrap.servers")));

        return true;
    }

    public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
        KafkaConsumerData data = (KafkaConsumerData) sdi;
        if (data.consumer != null) {
            data.consumer.close();

        }
        super.dispose(smi, sdi);
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        Object[] r = getRow();
        if (r == null) {
            /*
			 * If we have no input rows, make sure we at least run once to
			 * produce output rows. This allows us to consume without requiring
			 * an input step.
			 */
            if (!first) {
                setOutputDone();
                return false;
            }
            r = new Object[0];
        } else {
            incrementLinesRead();
        }

        final Object[] inputRow = r;

        KafkaConsumerMeta meta = (KafkaConsumerMeta) smi;
        final KafkaConsumerData data = (KafkaConsumerData) sdi;

        if (first) {
            first = false;
            data.inputRowMeta = getInputRowMeta();
            // No input rows means we just dummy data
            if (data.inputRowMeta == null) {
                data.outputRowMeta = new RowMeta();
                data.inputRowMeta = new RowMeta();
            } else {
                data.outputRowMeta = getInputRowMeta().clone();
            }
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
        }

        try {
            long timeout;
            String strData = meta.getTimeout();

            timeout = getTimeout(strData);

            logDebug("Starting message consumption with overall timeout of " + timeout + "ms");

            KafkaConsumerCallable kafkaConsumer = new KafkaConsumerCallable(meta, data, this) {
                protected void messageReceived(byte[] key, byte[] message) throws KettleException {
                    Object[] newRow = RowDataUtil.addRowData(inputRow.clone(), data.inputRowMeta.size(),
                            new Object[]{message, key});
                    putRow(data.outputRowMeta, newRow);

                    if (isRowLevel()) {
                        logRowlevel(Messages.getString("KafkaConsumer.Log.OutputRow",
                                Long.toString(getLinesWritten()), data.outputRowMeta.getString(newRow)));
                    }
                }
            };
            if (timeout > 0) {
                logDebug("Starting timed consumption");
                ExecutorService executor = Executors.newSingleThreadExecutor();
                try {
                    Future<?> future = executor.submit(kafkaConsumer);
                    executeFuture(timeout, future);
                } finally {
                    executor.shutdown();
                }
            } else {
                logDebug("Starting direct consumption");
                kafkaConsumer.call();
            }
        } catch (KettleException e) {
            if (!getStepMeta().isDoingErrorHandling()) {
                logError(Messages.getString("KafkaConsumer.ErrorInStepRunning", e.getMessage()));
                setErrors(1);
                stopAll();
                setOutputDone();
                return false;
            }
            putError(getInputRowMeta(), r, 1, e.toString(), null, getStepname());
        }
        return true;
    }

    private void executeFuture(long timeout, Future<?> future) throws KettleException {
        try {
            future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logDebug("Timeout exception on the Future");
        } catch (Exception e) {
            throw new KettleException(e);
        }
    }

    private long getTimeout(String strData) throws KettleException {
        long timeout;
        try {
            timeout = KafkaConsumerMeta.isEmpty(strData) ? 0 : Long.parseLong(environmentSubstitute(strData));
        } catch (NumberFormatException e) {
            throw new KettleException("Unable to parse step timeout value", e);
        }
        return timeout;
    }

    public void stopRunning(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

        KafkaConsumerData data = (KafkaConsumerData) sdi;
        if (data.consumer != null) {
            data.consumer.wakeup();
        }
        data.canceled = true;

        super.stopRunning(smi, sdi);
    }
}
