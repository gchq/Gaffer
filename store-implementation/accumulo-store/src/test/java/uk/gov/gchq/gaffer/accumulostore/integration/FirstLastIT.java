/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.StandaloneIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.First;
import uk.gov.gchq.koryphe.impl.binaryoperator.Last;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.commonutil.TestGroups.ENTITY;
import static uk.gov.gchq.gaffer.commonutil.TestPropertyNames.PROP_1;
import static uk.gov.gchq.gaffer.commonutil.TestPropertyNames.PROP_2;
import static uk.gov.gchq.gaffer.store.TestTypes.ID_STRING;
import static uk.gov.gchq.gaffer.store.TestTypes.PROP_INTEGER;
import static uk.gov.gchq.gaffer.store.TestTypes.PROP_INTEGER_2;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;

class FirstLastIT extends StandaloneIT {
    private static final String GRAPH_ID = "graphId";
    private static final String VERTEX = "vertex";
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreITs.class));

    private static Stream<Arguments> getOperations() {
        return Stream.of(
                Arguments.of(new GetAllElements()),
                Arguments.of(new GetElements.Builder().input(new EntitySeed(VERTEX)).build()));
    }

    @ParameterizedTest
    @MethodSource("getOperations")
    void shouldReturnCorrectResultsAfterCompaction(final Output<Iterable<? extends Element>> getOperation) throws OperationException, InterruptedException, StoreException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        final Graph graph = createGraph();
        final AccumuloStore accumuloStore = new AccumuloStore();
        accumuloStore.initialise(GRAPH_ID, createSchema(), createStoreProperties());

        // Add some data
        for (int i = 1; i <= 3; i++) {
            graph.execute(new AddElements.Builder().input(getEntity(i)).build(), getUser());
        }

        // Check before and after compaction
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(3, 1));
        compact(accumuloStore);
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(3, 1));

        // Add more data
        for (int i = 4; i <= 6; i++) {
            graph.execute(new AddElements.Builder().input(getEntity(i)).build(), getUser());
        }

        // Check before and after compaction
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(6, 1));
        compact(accumuloStore);
        // This can fail with getEntity(3, 4) if Accumulo non-deterministic bug is hit
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(6, 1));

        // Add more data
        for (int i = 7; i <= 100; i++) {
            graph.execute(new AddElements.Builder().input(getEntity(i)).build(), getUser());
        }
        
         // Check before and after compaction
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(100, 1));
        compact(accumuloStore);
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(100, 1));
    }

    @ParameterizedTest
    @MethodSource("getOperations")
    void shouldReturnCorrectResultsAfterFlush(final Output<Iterable<? extends Element>> getOperation) throws OperationException, InterruptedException, StoreException, AccumuloException, AccumuloSecurityException {
        final Graph graph = createGraph();
        final AccumuloStore accumuloStore = new AccumuloStore();
        accumuloStore.initialise(GRAPH_ID, createSchema(), createStoreProperties());

        // Add some data
        for (int i = 1; i <= 3; i++) {
            graph.execute(new AddElements.Builder().input(getEntity(i)).build(), getUser());
        }
        // Check before and after flush
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(3, 1));
        flush(accumuloStore);
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(3, 1));

        // Add more data
        for (int i = 4; i <= 6; i++) {
            graph.execute(new AddElements.Builder().input(getEntity(i)).build(), getUser());
        }

        // Check before and after flush
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(6, 1));
        flush(accumuloStore);
        // This can fail with getEntity(3, 4) if Accumulo non-deterministic bug is hit
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(6, 1));

        // Add more data
        for (int i = 7; i <= 100; i++) {
            graph.execute(new AddElements.Builder().input(getEntity(i)).build(), getUser());
        }

        // Check before and after flush
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(100, 1));
        flush(accumuloStore);
        assertThat(graph.execute(getOperation, getUser())).containsExactly(getEntity(100, 1));
    }

    private void compact(final AccumuloStore accumuloStore) throws StoreException, AccumuloSecurityException, TableNotFoundException, AccumuloException, InterruptedException {
        accumuloStore.getConnection()
                .tableOperations()
                .compact(GRAPH_ID, new CompactionConfig());
        Thread.sleep(10000L);
    }

    private void flush(final AccumuloStore accumuloStore) throws StoreException, AccumuloSecurityException, AccumuloException, InterruptedException {
        accumuloStore.getConnection()
                .tableOperations()
                .flush(GRAPH_ID);
        Thread.sleep(10000L);
    }

    private Entity getEntity(final int propertyValue) {
        return getEntity(propertyValue, propertyValue);
    }

    private Entity getEntity(final int firstValue, final int lastValue) {
        return new Entity.Builder()
                .group(ENTITY)
                .vertex(VERTEX)
                .property(PROP_1, firstValue)
                .property(PROP_2, lastValue)
                .build();
    }

    @Override
    protected Schema createSchema() {
        return new Schema.Builder()
                .entity(ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(ID_STRING)
                        .property(PROP_1, PROP_INTEGER)
                        .property(PROP_2, PROP_INTEGER_2)
                        .build())
                .type(ID_STRING, STRING_TYPE)
                .type(PROP_INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new First())
                        .build())
                .type(PROP_INTEGER_2, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Last())
                        .build())
                .build();
    }

    @Override
    public StoreProperties createStoreProperties() {
        return PROPERTIES;
    }

}
