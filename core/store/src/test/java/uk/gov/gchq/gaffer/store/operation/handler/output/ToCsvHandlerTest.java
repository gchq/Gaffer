/*
 * Copyright 2017-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.CsvGenerator;
import uk.gov.gchq.gaffer.data.generator.Neo4jFormat;
import uk.gov.gchq.gaffer.data.generator.NeptuneFormat;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ToCsvHandlerTest {
    @Mock
    Store storeMock;

    @Test
    public void shouldConvertToCsv() throws OperationException {
        // Given
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .generator(new CsvGenerator.Builder()
                        .group("Group Label")
                        .vertex("Vertex Label")
                        .source("Source Label")
                        .property("count", "Count Label")
                        .constant("A Constant", "Some constant value")
                        .quoted(false)
                        .build())
                .includeHeader(false)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), null);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                "BasicEntity,vertex1,,1,A Constant", "BasicEntity,vertex2,,,A Constant",
                "BasicEdge,,source1,1,A Constant", "BasicEdge,,source2,,A Constant"
        );
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldConvertToQuotedCsv() throws OperationException {
        // Given
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .generator(new CsvGenerator.Builder()
                        .group("Group Label")
                        .vertex("Vertex Label")
                        .source("Source Label")
                        .property("count", "Count Label")
                        .constant("A Constant", "Some constant value")
                        .quoted(true)
                        .build())
                .includeHeader(false)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), null);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                "\"BasicEntity\",\"vertex1\",,\"1\",\"A Constant\"", "\"BasicEntity\",\"vertex2\",,,\"A Constant\"",
                "\"BasicEdge\",,\"source1\",\"1\",\"A Constant\"", "\"BasicEdge\",,\"source2\",,\"A Constant\""
        );
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldConvertToCsvWithCommaReplacement() throws OperationException {
        // Given
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1,with comma", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1,with comma", "count", 1),
                makeEdge("source2")
        );

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .generator(new CsvGenerator.Builder()
                        .group("Group Label")
                        .vertex("Vertex Label")
                        .source("Source Label")
                        .property("count", "Count Label")
                        .constant("A Constant", "Some constant value")
                        .quoted(false)
                        .commaReplacement("-")
                        .build())
                .includeHeader(false)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), null);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                "BasicEntity,vertex1-with comma,,1,A Constant", "BasicEntity,vertex2,,,A Constant",
                "BasicEdge,,source1-with comma,1,A Constant", "BasicEdge,,source2,,A Constant"
        );
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldConvertToCsvWithHeader() throws OperationException {
        // Given
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .generator(new CsvGenerator.Builder()
                        .group("Group Label")
                        .vertex("Vertex Label")
                        .source("Source Label")
                        .property("count", "Count Label")
                        .constant("A Constant", "Some constant value")
                        .quoted(false)
                        .build())
                .includeHeader(true)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), null);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                "Group Label,Vertex Label,Source Label,Count Label,Some constant value",
                "BasicEntity,vertex1,,1,A Constant",
                "BasicEntity,vertex2,,,A Constant",
                "BasicEdge,,source1,1,A Constant",
                "BasicEdge,,source2,,A Constant"
        );
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldConvertToNeptuneFormattedCsv() throws OperationException {
        // Given
        NeptuneFormat neptuneFormat = new NeptuneFormat();
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .csvFormat(neptuneFormat)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        when(storeMock.getSchema()).thenReturn(makeSchema());
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), storeMock);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                ":ID,:LABEL,:TYPE,:START_ID,:END_ID,count:Integer,DIRECTED:Boolean",
                "vertex1,BasicEntity,,,,1,",
                "vertex2,BasicEntity,,,,,",
                ",,BasicEdge,source1,dest1,1,true",
                ",,BasicEdge,source2,dest2,,true"
        );
        assertThat(expected).isEqualTo(resultList);
    }

    @Test
    public void shouldConvertToNeo4jFormattedCsv() throws OperationException {
        // Given
        Neo4jFormat neo4jFormat = new Neo4jFormat();
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .csvFormat(neo4jFormat)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        when(storeMock.getSchema()).thenReturn(makeSchema());
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), storeMock);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                "_id,_labels,_type,_start,_end,count:Integer,DIRECTED:Boolean",
                "vertex1,BasicEntity,,,,1,",
                "vertex2,BasicEntity,,,,,",
                ",,BasicEdge,source1,dest1,1,true",
                ",,BasicEdge,source2,dest2,,true"
        );
        assertThat(expected).isEqualTo(resultList);
    }


    private Entity makeEntity(final String vertex, final String propertyName, final int propertyValue) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .property(propertyName, propertyValue)
                .build();
    }

    private Entity makeEntity(final String vertex) {
        return new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex(vertex)
                .build();
    }

    private Edge makeEdge(final String source, final String propertyName, final int propertyValue) {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest("dest1")
                .directed(true)
                .property(propertyName, propertyValue)
                .build();
    }
    private Edge makeEdge(final String source) {
        return new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(source)
                .dest("dest2")
                .directed(true)
                .build();
    }

    private Schema makeSchema() {
        Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .description("anEdge")
                        .directed("true")
                        .property("count", "int")
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .description("anotherEdge")
                        .directed("true")
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .property("count", "int")
                        .description("anEntity")
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .aggregateFunction(new StringConcat())
                        .build())
                .type("int", Integer.class)
                .type("true", Boolean.class)
                .build();
        return schema;
    }
    private Iterable<String> getPropertiesFromSchema(Schema schema) {
        List<String> propertyNames = new ArrayList<String>();
        for (SchemaEntityDefinition schemaEntityDefinition : schema.getEntities().values()) {
            propertyNames.addAll(schemaEntityDefinition.getProperties());
        }
        for (SchemaEdgeDefinition schemaEdgeDefinition : schema.getEdges().values()) {
            propertyNames.addAll(schemaEdgeDefinition.getProperties());
        }
        return propertyNames;
    }
}
