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

import uk.gov.gchq.gaffer.flink.operation.AddElementsFromKafka;
import uk.gov.gchq.gaffer.flink.operation.utils.FlinkOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Map;
import java.util.Properties;

public class AddElementsFromKafkaHandler implements OperationHandler<AddElementsFromKafka> {
    @Override
    public Object doOperation(final AddElementsFromKafka operation, final Context context, final Store store) throws OperationException {

        final Map<String, String> options = validateOperation(operation);

        final Properties properties = new Properties();
        properties.putAll(options);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new FlinkKafkaConsumer010<>(operation.getTopic(), new SimpleStringSchema(), properties))
                .map(new GafferMapFunction(operation.getElementGenerator()))
                .returns(GafferMapFunction.getReturnClass())
                .rebalance()
                .addSink(new GafferSink(store));

        try {
            env.execute(operation.getJobName());
        } catch (final Exception e) {
            throw new OperationException("Failed to add elements from kafta topic: " + operation.getTopic(), e);
        }

        return null;
    }

    private Map<String, String> validateOperation(final AddElementsFromKafka operation) {

        final Map<String, String> options = operation.getOptions();

        if (operation.getJobName() == null || StringUtils.isEmpty(operation.getJobName()) ) {
            throw new IllegalArgumentException("Unable to build AddElementsFromKafka operation - operation.jobName is not set");
        }
        if (operation.getGroupId() ==  null || StringUtils.isEmpty(operation.getGroupId())) {
            throw new IllegalArgumentException("Unable to build AddElementsFromKafka operation - operation.groupId is not set");
        } else {
            options.put(FlinkOptions.FLINK_KAFKA_GROUP_ID, operation.getGroupId());
        }
        if (operation.getBootstrapServers() == null || operation.getBootstrapServers().length == 0) {
            throw new IllegalArgumentException("Unable to build AddElementsFromKafka operation - operation.bootstrapServers is not set");
        } else {
            options.put(FlinkOptions.FLINK_KAFKA_BOOTSTRAP_SERVERS, StringUtils.join(operation.getBootstrapServers(), ","));
        }
        return options;
    }
}
