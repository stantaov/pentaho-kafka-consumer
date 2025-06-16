package org.pentaho.di.trans.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KafkaConsumerTest {
    @Test
    public void testCallableConsumesRecords() throws Exception {
        KafkaConsumerMeta meta = new KafkaConsumerMeta();
        meta.setLimit("1");
        KafkaConsumerData data = new KafkaConsumerData();
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        data.consumer = consumer;

        TopicPartition tp = new TopicPartition("topic", 0);
        List<ConsumerRecord<byte[], byte[]>> recs = Arrays.asList(new ConsumerRecord<>("topic", 0, 0L, new byte[0], new byte[0]));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> map = new HashMap<>();
        map.put(tp, recs);
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(map);

        when(consumer.poll(any(Duration.class))).thenReturn(records).thenReturn(ConsumerRecords.empty());

        KafkaConsumer step = mock(KafkaConsumer.class);
        KafkaConsumerCallable callable = new KafkaConsumerCallable(meta, data, step) {
            @Override
            protected void messageReceived(byte[] key, byte[] message) {
                // no-op
            }
        };

        callable.call();

        verify(consumer, atLeastOnce()).commitSync();
        assertEquals(1, data.processed);
    }
}
