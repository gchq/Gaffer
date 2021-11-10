/*
 * Copyright 2021 Crown Copyright
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
package uk.gov.gchq.gaffer.data.element.function;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;

public class ReduceRelatedElementsTest extends FunctionTest<ReduceRelatedElements> {
    public static final String RELATES_TO = "relatesTo";
    public static final String VISIBILITY = "visibility";

    public static final Object VERTEX1A = "vertex1a longest";
    public static final Object VERTEX1B = "vertex1b";
    public static final Object VERTEX2A = "vertex2a";
    public static final Object VERTEX2B = "vertex2b longest";
    public static final Object VERTEX3A = "vertex3a";
    public static final Object VERTEX3B = "vertex3b longest";
    public static final Object VERTEX4A = "vertex4a";
    public static final Object VERTEX4B = "vertex4b longest";
    public static final Object VERTEX5A = "vertex5a";
    public static final Object VERTEX5B = "vertex5b longest";

    @Test
    public void shouldReturnNullForNullValue() {
        // Given
        final ReduceRelatedElements function = getInstance();

        // When
        final Object result = function.apply(null);

        // Then
        assertNull(result);
    }

    @Test
    public void shouldReduceRelatedElements() {
        // Given
        final ReduceRelatedElements function = getInstance();
        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1B)
                        .dest(VERTEX3A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(VERTEX2A)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX1B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX2A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX3A)
                        .dest(VERTEX3B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build()
        );

        // When
        final Iterable<Element> result = function.apply(elements);

        // Then
        final List<Element> expectedElements = Arrays.asList(
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .property("sourceRelatedVertices", Sets.newHashSet(VERTEX1B))
                        .property("destinationRelatedVertices", Sets.newHashSet(VERTEX2A))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX3B)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public", "private"))
                        .property("sourceRelatedVertices", Sets.newHashSet(VERTEX1B))
                        .property("destinationRelatedVertices", Sets.newHashSet(VERTEX3A))
                        .build(),
                new Entity.Builder()
                        .vertex(VERTEX2B)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .property("relatedVertices", Sets.newHashSet(VERTEX2A))
                        .build()
        );
        assertElementEquals(expectedElements, result);
    }

    @Test
    public void shouldReduceRelatedElementsWithManyRelationships() {
        // Given
        final ReduceRelatedElements function = getInstance();
        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1B)
                        .dest(VERTEX3A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(VERTEX2A)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX1B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX2A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX3A)
                        .dest(VERTEX3B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX2A)
                        .dest(VERTEX3B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX2B)
                        .dest(VERTEX3A)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX3A)
                        .dest(VERTEX4B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX5B)
                        .dest(VERTEX4A)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build()
        );

        // When
        final Iterable<Element> result = function.apply(elements);

        // Then
        final List<Element> expectedElements = Arrays.asList(
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public", "private"))
                        .property("sourceRelatedVertices", Sets.newHashSet(VERTEX1B))
                        .property("destinationRelatedVertices", Sets.newHashSet(VERTEX3B, VERTEX3A, VERTEX2A, VERTEX4B))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public", "private"))
                        .property("sourceRelatedVertices", Sets.newHashSet(VERTEX1B))
                        .property("destinationRelatedVertices", Sets.newHashSet(VERTEX3B, VERTEX3A, VERTEX2A, VERTEX4B))
                        .build(),
                new Entity.Builder()
                        .vertex(VERTEX2B)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public", "private"))
                        .property("relatedVertices", Sets.newHashSet(VERTEX3B, VERTEX3A, VERTEX2A, VERTEX4B))
                        .build()
        );
        assertElementEquals(expectedElements, result);
    }

    @Test
    public void shouldReturnInputIfNoRelatedVertexGroups() {
        // Given
        final ReduceRelatedElements function = new ReduceRelatedElements();
        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1B)
                        .dest(VERTEX3A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(VERTEX2A)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX1B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX2A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX3A)
                        .dest(VERTEX3B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build()
        );

        // When
        final Iterable<Element> result = function.apply(elements);

        // Then
        assertElementEquals(elements, result);
    }

    @Test
    public void shouldFailIfNoVisibilityAggregatorProvided() {
        // Given
        final ReduceRelatedElements function = new ReduceRelatedElements();
        function.setVisibilityProperty(VISIBILITY);
        function.setVisibilityAggregator(null);
        function.setVertexAggregator(new Longest());
        function.setRelatedVertexGroups(Collections.singleton(RELATES_TO));

        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX2A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1B)
                        .dest(VERTEX3A)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(VERTEX2A)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX1A)
                        .dest(VERTEX1B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX2A)
                        .dest(VERTEX2B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(VERTEX3A)
                        .dest(VERTEX3B)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build()
        );

        // When
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> {
                function.apply(elements);
            })
            .withMessageContaining("No visibility aggregator provided, so visibilities cannot be combined.");
    }

    @Override
    protected ReduceRelatedElements getInstance() {
        final ReduceRelatedElements function = new ReduceRelatedElements();
        function.setVisibilityProperty(VISIBILITY);
        function.setVisibilityAggregator(new CollectionConcat<>());
        function.setVertexAggregator(new Longest());
        function.setRelatedVertexGroups(Collections.singleton(RELATES_TO));
        return function;
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<? extends ReduceRelatedElements> getFunctionClass() {
        return ReduceRelatedElements.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Iterable.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Iterable.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final ReduceRelatedElements function = getInstance();

        // When
        final byte[] json = JSONSerialiser.serialise(function);
        final ReduceRelatedElements deserialisedObj = JSONSerialiser.deserialise(json, ReduceRelatedElements.class);

        // Then
        JsonAssert.assertEquals(
                "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ReduceRelatedElements\",\"vertexAggregator\":{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ReduceRelatedElementsTest$Longest\"},\"visibilityAggregator\":{\"class\":\"uk.gov.gchq.koryphe.impl.binaryoperator.CollectionConcat\"},\"visibilityProperty\":\"visibility\",\"relatedVertexGroups\":[\"relatesTo\"]}\n",
                new String(json)
        );
        assertNotNull(deserialisedObj);
    }

    private static final class Longest extends KorypheBinaryOperator<String> {
        @Override
        protected String _apply(final String s, final String t) {
            if (s.length() > t.length()) {
                return s;
            }

            return t;
        }
    }
}
