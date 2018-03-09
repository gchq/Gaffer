/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.get;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

public class GetElementsTest extends OperationTest<GetElements> {
    @Test
    public void shouldSetSeedMatchingTypeToEquals() {
        // Given
        final ElementId elementId1 = new EntitySeed("identifier");

        // When
        final GetElements op = new GetElements.Builder()
                .input(elementId1)
                .seedMatching(SeedMatchingType.EQUAL)
                .build();

        // Then
        assertEquals(SeedMatchingType.EQUAL, op.getSeedMatching());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(CloseableIterable.class, outputClass);
    }

    @Test
    public void shouldSerialiseAndDeserialiseOperationWithElementIds() throws SerialisationException {
        // Given
        final ElementSeed elementSeed1 = new EntitySeed("identifier");
        final ElementSeed elementSeed2 = new EdgeSeed("source2", "destination2", true);
        final GetElements op = new GetElements.Builder()
                .input(elementSeed1, elementSeed2)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final GetElements deserialisedOp = JSONSerialiser.deserialise(json, GetElements.class);

        // Then
        final Iterator itr = deserialisedOp.getInput().iterator();
        assertEquals(elementSeed1, itr.next());
        assertEquals(elementSeed2, itr.next());
        assertFalse(itr.hasNext());
    }

    @Test
    public void shouldDeserialiseOperationWithVertices() throws SerialisationException {
        // Given
        final String json = "{\"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetElements\"," +
                "\"input\":[" +
                "1," +
                "{\"class\":\"uk.gov.gchq.gaffer.types.TypeSubTypeValue\",\"type\":\"t\",\"subType\":\"s\",\"value\":\"v\"}," +
                "[\"java.lang.Long\",2]" +
                "]}";

        // When
        final GetElements deserialisedOp = JSONSerialiser.deserialise(json, GetElements.class);

        // Then
        assertEquals(
                Lists.newArrayList(new EntitySeed(1), new EntitySeed(new TypeSubTypeValue("t", "s", "v")), new EntitySeed(2L)),
                Lists.newArrayList(deserialisedOp.getInput())
        );
    }

    @Test
    public void shouldDeserialiseOperationWithVerticesAndIds() throws SerialisationException {
        // Given
        final String json = String.format("{\"class\":\"uk.gov.gchq.gaffer.operation.impl.get.GetElements\"," +
                "\"input\":[" +
                "1," +
                "{\"class\":\"uk.gov.gchq.gaffer.types.TypeSubTypeValue\",\"type\":\"t\",\"subType\":\"s\",\"value\":\"v\"}," +
                "{\"vertex\":{\"java.lang.Long\":2},\"class\":\"uk.gov.gchq.gaffer.operation.data.EntitySeed\"}" +
                "]}");

        // When
        final GetElements deserialisedOp = JSONSerialiser.deserialise(json, GetElements.class);

        // Then
        assertEquals(
                Lists.newArrayList(new EntitySeed(1), new EntitySeed(new TypeSubTypeValue("t", "s", "v")), new EntitySeed(2L)),
                Lists.newArrayList(deserialisedOp.getInput())
        );
    }

    private void builderShouldCreatePopulatedOperationAll() {
        final GetElements op = new GetElements.Builder()
                .input(new EntitySeed("A"), 1, new EdgeSeed(2L, 3L))
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();

        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER,
                op.getIncludeIncomingOutGoing());
        assertNotNull(op.getView());
        assertEquals(Lists.newArrayList(new EntitySeed("A"), new EntitySeed(1), new EdgeSeed(2L, 3L)), Lists.newArrayList(op.getInput()));
    }

    @Test
    public void shouldSetSeedMatchingTypeToRelated() {
        final ElementId elementId1 = new EntitySeed("identifier");
        final ElementId elementId2 = new EdgeSeed("source2", "destination2", true);

        // When
        final GetElements op = new GetElements.Builder()
                .input(elementId1, elementId2)
                .seedMatching(SeedMatchingType.RELATED)
                .build();

        // Then
        assertEquals(SeedMatchingType.RELATED, op.getSeedMatching());
    }

    private void builderShouldCreatePopulatedOperationIncoming() {
        ElementSeed seed = new EntitySeed("A");
        GetElements op = new GetElements.Builder()
                .input(seed)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING,
                op.getIncludeIncomingOutGoing());
        assertNotNull(op.getView());
        assertEquals(seed, op.getInput().iterator().next());
    }

    @Test
    public void shouldSetDirectedTypeToBoth() {
        // When
        final GetElements op = new GetElements.Builder()
                .directedType(DirectedType.EITHER)
                .input(new EntitySeed())
                .build();

        // Then
        assertEquals(DirectedType.EITHER, op.getDirectedType());
    }

    @Test
    public void shouldSetOptionToValue() {
        // When
        final GetElements op = new GetElements.Builder()
                .option("key", "value")
                .input(new EntitySeed())
                .build();

        // Then
        assertThat(op.getOptions(), is(notNullValue()));
        assertThat(op.getOptions().get("key"), is("value"));
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationAll();
        builderShouldCreatePopulatedOperationIncoming();
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        EntitySeed input = new EntitySeed("A");
        View view = new View.Builder()
                .edge("testEdgeGroup")
                .build();
        final GetElements getElements = new GetElements.Builder()
                .input(input)
                .inOutType(IncludeIncomingOutgoingType.EITHER)
                .view(view)
                .directedType(DirectedType.DIRECTED)
                .seedMatching(SeedMatchingType.RELATED)
                .option("testOption", "true")
                .build();

        // When
        GetElements clone = getElements.shallowClone();

        // Then
        assertNotSame(getElements, clone);
        assertEquals(Lists.newArrayList(input), Lists.newArrayList(clone.getInput()));
        assertEquals(IncludeIncomingOutgoingType.EITHER, clone.getIncludeIncomingOutGoing());
        assertEquals(view, clone.getView());
        assertEquals(DirectedType.DIRECTED, clone.getDirectedType());
        assertEquals(SeedMatchingType.RELATED, clone.getSeedMatching());
        assertEquals("true", clone.getOption("testOption"));
    }

    @Test
    public void shouldCreateInputFromVertices() {
        // When
        final GetElements op = new GetElements.Builder()
                .input("1", new EntitySeed("2"), new Entity("group1", "3"), new EdgeSeed("4", "5"), new Edge("group", "6", "7", true))
                .build();

        // Then
        assertEquals(
                Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"), new Entity("group1", "3"), new EdgeSeed("4", "5"), new Edge("group", "6", "7", true)),
                Lists.newArrayList(op.getInput())
        );
    }

    @Override
    protected GetElements getTestObject() {
        return new GetElements.Builder()
                .input(new EntitySeed())
                .build();
    }
}
