/*
 * Copyright 2020-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.integration.operation.handler.scalardd;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;
import org.apache.spark.rdd.RDD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants;
import uk.gov.gchq.gaffer.spark.operation.scalardd.SplitStoreFromRDDOfElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class SplitStoreFromRDDOfElementsHandlerIT {

    private static final ClassTag<Element> ELEMENT_CLASS_TAG = ClassTagConstants.ELEMENT_CLASS_TAG;
    private static final String GRAPH_ID = "graphId";

    private final User user = new User();

    private ArrayBuffer<Element> elements;
    private RDD<Element> rdd;

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(currentClass));

    @BeforeEach
    public void setUp() {

        elements = createElements();
        rdd = createRDDContaining(elements);
    }

    private ArrayBuffer<Element> createElements() {

        final ArrayBuffer<Element> elements = new ArrayBuffer<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 4)
                    .build();

            elements.$plus$eq(edge1);
            elements.$plus$eq(edge2);
            elements.$plus$eq(entity);
        }

        return elements;
    }

    private RDD<Element> createRDDContaining(final ArrayBuffer<Element> elements) {

        return SparkSessionProvider.getSparkSession().sparkContext().parallelize(elements, 8, ELEMENT_CLASS_TAG);
    }

    @Test
    public void shouldCreateSplitPointsFromRDD() throws Exception {

        final int tabletServerCount = 3;
        final SingleUseAccumuloStoreWithTabletServers store = new SingleUseAccumuloStoreWithTabletServers(tabletServerCount);
        store.initialise(
                GRAPH_ID,
                Schema.fromJson(StreamUtil.openStreams(getClass(), "/schema-RDDSplitPointIntegrationTests/")),
                PROPERTIES
        );

        final Graph graph = new Graph.Builder()
                .store(store)
                .build();

        graph.execute(new SplitStoreFromRDDOfElements.Builder()
                .input(rdd)
                .fractionToSample(1d)
                .build(), user);

        // Then
        final List<Text> splitsOnTable = Lists.newArrayList(store.getConnection().tableOperations().listSplits(store.getTableName(), 10));
        final int expectedSplitCount = tabletServerCount - 1;

        assertThat(splitsOnTable).hasSize(expectedSplitCount);
        assertThat(Base64.encodeBase64String(splitsOnTable.get(0).getBytes())).isEqualTo("3A==");
        assertThat(Base64.encodeBase64String(splitsOnTable.get(1).getBytes())).isEqualTo("6A==");
    }

    private static final class SingleUseAccumuloStoreWithTabletServers extends SingleUseAccumuloStore {

        private final List<String> tabletServers;

        SingleUseAccumuloStoreWithTabletServers(final int size) {
            this.tabletServers = IntStream.range(0, size).mapToObj(Integer::toString).collect(Collectors.toList());
        }

        @Override
        public List<String> getTabletServers() throws StoreException {
            return tabletServers;
        }
    }

}
