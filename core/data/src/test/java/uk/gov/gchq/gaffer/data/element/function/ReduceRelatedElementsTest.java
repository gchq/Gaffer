/*
 * Copyright 2017-2021 Crown Copyright
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
import org.junit.Test;

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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static uk.gov.gchq.gaffer.data.util.ElementUtil.assertElementEquals;

public class ReduceRelatedElementsTest extends FunctionTest {
    public static final String RELATES_TO = "relatesTo";
    public static final String VISIBILITY = "visibility";

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
        Object vertex1a = "vertex1a longest";
        Object vertex1b = "vertex1b";
        Object vertex2a = "vertex2a";
        Object vertex2b = "vertex2b longest";
        Object vertex3a = "vertex3a";
        Object vertex3b = "vertex3b longest";

        // Given
        final ReduceRelatedElements function = getInstance();
        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex2a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1b)
                        .dest(vertex3a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(vertex2a)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex1b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex2a)
                        .dest(vertex2b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex3a)
                        .dest(vertex3b)
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
                        .source(vertex1a)
                        .dest(vertex2b)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .property("sourceRelatedVertices", Sets.newHashSet(vertex1b))
                        .property("destinationRelatedVertices", Sets.newHashSet(vertex2a))
                        .build(),
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex3b)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public", "private"))
                        .property("sourceRelatedVertices", Sets.newHashSet(vertex1b))
                        .property("destinationRelatedVertices", Sets.newHashSet(vertex3a))
                        .build(),
                new Entity.Builder()
                        .vertex(vertex2b)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .property("relatedVertices", Sets.newHashSet(vertex2a))
                        .build()
        );
        assertElementEquals(expectedElements, result);
    }

    @Test
    public void shouldReduceRelatedElementsWithManyRelationships() {
        Object vertex1a = "vertex1a longest";
        Object vertex1b = "vertex1b";
        Object vertex2a = "vertex2a";
        Object vertex2b = "vertex2b longest";
        Object vertex3a = "vertex3a";
        Object vertex3b = "vertex3b longest";
        Object vertex4a = "vertex4a";
        Object vertex4b = "vertex4b longest";
        Object vertex5a = "vertex5a";
        Object vertex5b = "vertex5b longest";

        // Given
        final ReduceRelatedElements function = getInstance();
        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex2a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1b)
                        .dest(vertex3a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(vertex2a)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex1b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex2a)
                        .dest(vertex2b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex3a)
                        .dest(vertex3b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build(),
                new Edge.Builder()
                        .source(vertex2a)
                        .dest(vertex3b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build(),
                new Edge.Builder()
                        .source(vertex2b)
                        .dest(vertex3a)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex3a)
                        .dest(vertex4b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build(),
                new Edge.Builder()
                        .source(vertex5b)
                        .dest(vertex4a)
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
                        .source(vertex1a)
                        .dest(vertex2b)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .property("sourceRelatedVertices", Sets.newHashSet(vertex1b))
                        .property("destinationRelatedVertices", Sets.newHashSet(vertex2a))
                        .build(),
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex3b)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public", "private"))
                        .property("sourceRelatedVertices", Sets.newHashSet(vertex1b))
                        .property("destinationRelatedVertices", Sets.newHashSet(vertex3a))
                        .build(),
                new Entity.Builder()
                        .vertex(vertex2b)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .property("relatedVertices", Sets.newHashSet(vertex2a))
                        .build()
        );
//        assertElementEquals(expectedElements, result);
    }

    @Test
    public void shouldReturnInputIfNoRelatedVertexGroups() {
        Object vertex1a = "vertex1a longest";
        Object vertex1b = "vertex1b";
        Object vertex2a = "vertex2a";
        Object vertex2b = "vertex2b longest";
        Object vertex3a = "vertex3a";
        Object vertex3b = "vertex3b longest";

        // Given
        final ReduceRelatedElements function = new ReduceRelatedElements();
        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex2a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1b)
                        .dest(vertex3a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(vertex2a)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex1b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex2a)
                        .dest(vertex2b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex3a)
                        .dest(vertex3b)
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
        Object vertex1a = "vertex1a longest";
        Object vertex1b = "vertex1b";
        Object vertex2a = "vertex2a";
        Object vertex2b = "vertex2b longest";
        Object vertex3a = "vertex3a";
        Object vertex3b = "vertex3b longest";

        // Given
        final ReduceRelatedElements function = new ReduceRelatedElements();
        function.setVisibilityProperty(VISIBILITY);
        function.setVisibilityAggregator(null);
        function.setVertexAggregator(new Longest());
        function.setRelatedVertexGroups(Collections.singleton(RELATES_TO));

        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex2a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1b)
                        .dest(vertex3a)
                        .directed(true)
                        .group(TestGroups.EDGE)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Entity.Builder()
                        .vertex(vertex2a)
                        .group(TestGroups.ENTITY)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex1a)
                        .dest(vertex1b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex2a)
                        .dest(vertex2b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("public"))
                        .build(),
                new Edge.Builder()
                        .source(vertex3a)
                        .dest(vertex3b)
                        .directed(true)
                        .group(RELATES_TO)
                        .property(VISIBILITY, Sets.newHashSet("private"))
                        .build()
        );

        // When
        try {
            function.apply(elements);

            fail();
        } catch (final IllegalArgumentException e) {
            // Then
            assertTrue(e.getMessage().contains("No visibility aggregator provided, so visibilities cannot be combined."));
        }
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
    protected Class<? extends ReduceRelatedElements> getFunctionClass() {
        return ReduceRelatedElements.class;
    }

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
