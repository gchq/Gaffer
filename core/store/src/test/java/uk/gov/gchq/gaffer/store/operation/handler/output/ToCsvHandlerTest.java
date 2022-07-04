/*
 * Copyright 2017-2020 Crown Copyright
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
                "Foo,vertex1,,1,A Constant", "Foo,vertex2,,,A Constant",
                "Bar,,source1,1,A Constant", "Bar,,source2,,A Constant"
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
                "\"Foo\",\"vertex1\",,\"1\",\"A Constant\"", "\"Foo\",\"vertex2\",,,\"A Constant\"",
                "\"Bar\",,\"source1\",\"1\",\"A Constant\"", "\"Bar\",,\"source2\",,\"A Constant\""
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
                "Foo,vertex1-with comma,,1,A Constant", "Foo,vertex2,,,A Constant",
                "Bar,,source1-with comma,1,A Constant", "Bar,,source2,,A Constant"
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
                "Foo,vertex1,,1,A Constant",
                "Foo,vertex2,,,A Constant",
                "Bar,,source1,1,A Constant",
                "Bar,,source2,,A Constant"
        );
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldConvertToOpenCypherCsvWithHeader() throws OperationException {
        // Given
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        Schema schema = makeSchema();
        Iterable<String> propertyNames = getPropertiesFromSchema(schema);

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .openCypherFormat(true)
                .neo4jFormat(false)
                .includeHeader(true)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        when(storeMock.getSchema()).thenReturn(makeSchema());
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), storeMock);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                ":ID:string,:LABEL:string,:TYPE:string,:START_ID:string,:END_ID:string,count:int,DIRECTED:boolean",
                "vertex1,Foo,,,,1,",
                "vertex2,Foo,,,,,",
                ",,Bar,source1,dest1,1,true",
                ",,Bar,source2,dest2,,true"
        );
        assertEquals(expected, resultList);
    }

    @Test
    public void shouldConvertToOpenCypherCsvUsingNeo4jFormatWithHeader() throws OperationException {
        // Given
        final List<Element> elements = Lists.newArrayList(
                makeEntity("vertex1", "count", 1),
                makeEntity("vertex2"),
                makeEdge("source1", "count", 1),
                makeEdge("source2")
        );

        Schema schema = makeSchema();
        Iterable<String> propertyNames = getPropertiesFromSchema(schema);

        final ToCsv operation = new ToCsv.Builder()
                .input(elements)
                .openCypherFormat(true)
                .neo4jFormat(true)
                .includeHeader(true)
                .build();

        final ToCsvHandler handler = new ToCsvHandler();

        //When
        when(storeMock.getSchema()).thenReturn(makeSchema());
        final Iterable<? extends String> results = handler.doOperation(operation, new Context(), storeMock);

        //Then
        final List<String> resultList = Lists.newArrayList(results);
        final List<String> expected = Arrays.asList(
                "_id:string,_labels:string,_type:string,_start:string,_end:string,count:int,DIRECTED:boolean",
                "vertex1,Foo,,,,1,",
                "vertex2,Foo,,,,,",
                ",,Bar,source1,dest1,1,true",
                ",,Bar,source2,dest2,,true"
        );
        assertEquals(expected, resultList);
    }

    private Entity makeEntity(final String vertex, final String propertyName, final int propertyValue) {
        return new Entity.Builder()
                .group("Foo")
                .vertex(vertex)
                .property(propertyName, propertyValue)
                .build();
    }

    private Entity makeEntity(final String vertex) {
        return new Entity.Builder()
                .group("Foo")
                .vertex(vertex)
                .build();
    }

    private Edge makeEdge(final String source, final String propertyName, final int propertyValue) {
        return new Edge.Builder()
                .group("Bar")
                .source(source)
                .dest("dest1")
                .directed(true)
                .property(propertyName, propertyValue)
                .build();
    }

    private Edge makeEdge(final String source) {
        return new Edge.Builder()
                .group("Bar")
                .source(source)
                .dest("dest2")
                .directed(true)
                .build();
    }

    private Schema makeSchema() {
        Schema schema = new Schema.Builder()
                .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                        .groupBy("bar")
                        .source("string")
                        .destination("string")
                        .description("anEdge")
                        .directed("boolean")
                        .property("count", "int")
                        .build())
                .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                        .groupBy("bar")
                        .source("string")
                        .destination("string")
                        .description("anotherEdge")
                        .directed("boolean")
                        .build())
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .groupBy("Foo")
                        .property("count", "int")
                        .description("anEntity")
                        .build())
                .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                        .vertex("string")
                        .groupBy("Foo")
                        .build())
                .type("string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .serialiser(new StringSerialiser())
                        .aggregateFunction(new StringConcat())
                        .build())
                .type("true", Boolean.class)
                .build();
        return schema;
    }
    Iterable<String> getPropertiesFromSchema(Schema schema) {
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
