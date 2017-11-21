/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.flink.operation.handler;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Properties;

/**
 * A {@code AddElementsFromKafkaHandler} handles the {@link AddElementsFromKafka}
 * operation.
 *
 * This uses Flink to stream the {@link uk.gov.gchq.gaffer.data.element.Element}
 * objects from a Kafka queue into Gaffer.
 */
public class AddElementsFromKafkaHandler implements OperationHandler<AddElementsFromKafka> {
    private static final String FLINK_KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String FLINK_KAFKA_GROUP_ID = "group.id";

    @Override
    public Object doOperation(final AddElementsFromKafka op, final Context context, final Store store) throws OperationException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (null != op.getParallelism()) {
            env.setParallelism(op.getParallelism());
        }

        env.addSource(new FlinkKafkaConsumer010<>(op.getTopic(), new SimpleStringSchema(), createFlinkProperties(op)))
                .map(new GafferMapFunction(op.getElementGenerator()))
                .returns(GafferMapFunction.RETURN_CLASS)
                .rebalance()
                .addSink(new GafferSink(op, store));

        try {
            env.execute(op.getClass().getSimpleName() + "-" + op.getGroupId() + "-" + op.getTopic());
        } catch (final Exception e) {
            throw new OperationException("Failed to add elements from kafka topic: " + op.getTopic(), e);
        }

        return null;
    }

    private Properties createFlinkProperties(final AddElementsFromKafka operation) {
        final Properties properties = new Properties();
        if (null != operation.getOptions()) {
            properties.putAll(operation.getOptions());
        }
        properties.put(FLINK_KAFKA_GROUP_ID, operation.getGroupId());
        properties.put(FLINK_KAFKA_BOOTSTRAP_SERVERS, StringUtils.join(operation.getBootstrapServers(), ","));
        return properties;
    }
}
