package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Holds data processed by this step
 *
 * @author Michael
 */
public class KafkaConsumerData extends BaseStepData implements StepDataInterface {

    KafkaConsumer<byte[], byte[]> consumer;
    RowMetaInterface outputRowMeta;
    RowMetaInterface inputRowMeta;
    boolean canceled;
    int processed;
}
