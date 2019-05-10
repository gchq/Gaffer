/*
 * Copyright 2017-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.hbasestore.HBaseProperties;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.util.CellUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AddElementsHandlerTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .type("string", new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .build())
            .type("int", new TypeDefinition.Builder()
                    .clazz(Integer.class)
                    .serialiser(new CompactRawIntegerSerialiser())
                    .build())
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .property("prop1", "string")
                    .property("visibility", "string")
                    .property("count", "int")
                    .groupBy("prop1")
                    .aggregate(false)
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .property("prop1", "string")
                    .property("visibility", "string")
                    .property("count", "int")
                    .groupBy("prop1")
                    .aggregate(false)
                    .build())
            .visibilityProperty("visibility")
            .vertexSerialiser(new StringSerialiser())
            .build();

    private static final Schema SCHEMA_WITH_AGGREGATION = new Schema.Builder()
            .type("string", new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .aggregateFunction(new StringConcat())
                    .build())
            .type("int", new TypeDefinition.Builder()
                    .clazz(Integer.class)
                    .serialiser(new CompactRawIntegerSerialiser())
                    .aggregateFunction(new Sum())
                    .build())
            .type("type", Boolean.class)
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source("string")
                    .destination("string")
                    .directed("true")
                    .property("prop1", "string")
                    .property("visibility", "string")
                    .property("count", "int")
                    .groupBy("prop1")
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex("string")
                    .property("prop1", "string")
                    .property("visibility", "string")
                    .property("count", "int")
                    .groupBy("prop1")
                    .build())
            .visibilityProperty("visibility")
            .vertexSerialiser(new StringSerialiser())
            .build();

    @Test
    public void shouldAddElements() throws OperationException, StoreException, IOException {
        // Given
        final AddElementsHandler handler = new AddElementsHandler();
        final List<Element> elements = createElements();
        final List<Element> elementsWithNull = new ArrayList<>(elements);
        elementsWithNull.add(null); // null should be skipped

        final AddElements addElements = new AddElements.Builder()
                .input(elementsWithNull)
                .build();
        final Context context = mock(Context.class);
        final HBaseStore store = mock(HBaseStore.class);

        final HTable table = mock(HTable.class);
        given(store.getTable()).willReturn(table);

        final HBaseProperties properties = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        final int writeBufferSize = 5;
        properties.setWriteBufferSize(writeBufferSize);
        given(store.getProperties()).willReturn(properties);

        given(store.getSchema()).willReturn(SCHEMA);

        // When
        handler.doOperation(addElements, context, store);

        // Then
        final ArgumentCaptor<List<Put>> putsCaptor = (ArgumentCaptor) ArgumentCaptor.forClass(List.class);
        verify(table, times(2)).put(putsCaptor.capture());
        verify(table, times(2)).flushCommits();
        final List<List<Put>> allPuts = putsCaptor.getAllValues();
        assertEquals(2, allPuts.size());
        final List<Put> combinedPuts = new ArrayList<>();
        combinedPuts.addAll(allPuts.get(0));
        combinedPuts.addAll(allPuts.get(1));

        final List<Element> expectedElements = new ArrayList<>();
        for (final Element element : elements) {
            expectedElements.add(element);
            if (element instanceof Edge && !((Edge) element).getSource().equals(((Edge) element).getDestination())) {
                expectedElements.add(element);
            }
        }
        final Element[] expectedElementsArr = expectedElements.toArray(new Element[expectedElements.size()]);
        final List<Element> elementsAdded = CellUtil.getElements(combinedPuts, new ElementSerialisation(SCHEMA), false);
        assertEquals(expectedElements.size(), elementsAdded.size());
        assertThat(elementsAdded, IsCollectionContaining.hasItems(expectedElementsArr));
    }

    @Test
    public void shouldDoNothingIfNoElementsProvided() throws OperationException, StoreException, IOException {
        // Given
        final AddElementsHandler handler = new AddElementsHandler();
        final AddElements addElements = new AddElements();
        final Context context = mock(Context.class);
        final HBaseStore store = mock(HBaseStore.class);

        final Table table = mock(Table.class);
        given(store.getTable()).willReturn(table);

        final HBaseProperties properties = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        given(store.getProperties()).willReturn(properties);

        given(store.getSchema()).willReturn(SCHEMA);

        // When
        handler.doOperation(addElements, context, store);

        // Then
        verify(table, never()).put(anyListOf(Put.class));
    }

    private List<Element> createElements() {
        return Lists.newArrayList(
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertexA")
                        .dest("vertexB")
                        .directed(true)
                        .property("prop1", "a")
                        .property("visibility", "public")
                        .property("count", 1)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertexD")
                        .dest("vertexC")
                        .directed(true)
                        .property("prop1", "a")
                        .property("visibility", "public")
                        .property("count", 1)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertexE")
                        .dest("vertexE")
                        .directed(true)
                        .property("prop1", "a")
                        .property("visibility", "public")
                        .property("count", 1)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertexF")
                        .dest("vertexG")
                        .directed(false)
                        .property("prop1", "a")
                        .property("visibility", "public")
                        .property("count", 1)
                        .build(),
                new Edge.Builder()
                        .group(TestGroups.EDGE)
                        .source("vertexH")
                        .dest("vertexH")
                        .directed(false)
                        .property("prop1", "a")
                        .property("visibility", "public")
                        .property("count", 1)
                        .build(),
                new Entity.Builder()
                        .group(TestGroups.ENTITY)
                        .vertex("vertexI")
                        .property("prop1", "a")
                        .property("visibility", "public")
                        .property("count", 1)
                        .build()
        );
    }

    @Test
    public void shouldThrowNoExceptionsWhenValidateFlagSetToFalse() throws OperationException, StoreException {
        final AddElements addElements = new AddElements.Builder()
                .input(new Edge("Unknown group", "source", "dest", true))
                .validate(false)
                .build();

        final AddElementsHandler handler = new AddElementsHandler();
        final Context context = mock(Context.class);
        final HBaseStore store = mock(HBaseStore.class);

        final Table table = mock(Table.class);
        given(store.getTable()).willReturn(table);

        final HBaseProperties properties = HBaseProperties.loadStoreProperties(StreamUtil.storeProps(getClass()));
        given(store.getProperties()).willReturn(properties);

        given(store.getSchema()).willReturn(SCHEMA);

        // When / Then - no exceptions
        handler.doOperation(addElements, context, store);
    }
}
