/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.flink.operation.handler.util.FlinkConstants;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromKafka;
import uk.gov.gchq.gaffer.user.User;

/**
 * To run the demo follow these steps (based on the steps in the blog: https://data-artisans.com/kafka-flink-a-practical-how-to)
 * <pre>
 * wget http://apache.mirror.anlx.net/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz
 * tar xf kafka_2.11-0.10.2.0.tgz
 * cd kafka_2.11-0.10.2.0
 * ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
 * ./bin/kafka-server-start.sh ./config/server.properties
 * ./bin/kafka-topics.sh --create --topic test --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * ./bin/kafka-console-producer.sh --topic test --broker-list localhost:9092
 * Run this demo
 * In the terminal running kafka-console-producer, add some elements, e.g:
 * 1,2
 * 1,3
 * 3,2
 * They must be in the format [source],[destination].
 * You should see the elements have been added to Gaffer and logged in the IDE terminal
 * </pre>
 */
public class AddElementsFromKafkaDemo {
    public static void main(String[] args) throws Exception {
        new AddElementsFromKafkaDemo().run();
    }

    private void run() throws OperationException {
        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(StreamUtil.storeProps(getClass()))
                .addSchemas(StreamUtil.schemas(getClass()))
                .build();

        graph.execute(new AddElementsFromKafka.Builder()
                .parallelism(1)
                .topic("test")
                .groupId("group1")
                .bootstrapServers("localhost:9092")
                .generator(CsvToElement.class)
                .option(FlinkConstants.SKIP_REBALANCING, "true")
                .build(), new User());
    }
}
