/*
 * Copyright 2016-2023 Crown Copyright
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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

public class GetElementsTest extends OperationTest<GetElements> {

    // This demonstrates how to do the equivalent of seedMatching EQUALS
    @Test
    public void shouldSetViewToCorrectGroup() {
        // Given
        final ElementId elementId1 = new EntitySeed("identifier");

        // When
        final GetElements op = new GetElements.Builder()
                .input(elementId1)
                .view(new View.Builder()
                        .entity("group1")
                        .build())
                .build();

        // Then
        assertThat(op.getView().getGroups()).contains("group1");
    }

    // This demonstrates how to do the equivalent of seedMatching RELATED
    @Test
    public void shouldHaveNoViewSet() {
        final ElementId elementId1 = new EntitySeed("identifier");
        final ElementId elementId2 = new EdgeSeed("source2", "destination2", true);

        // When
        final GetElements op = new GetElements.Builder()
                .input(elementId1, elementId2)
                .build();

        // Then
        assertThat(op.getView()).isNull();
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Iterable.class);
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
        final byte[] json = JSONSerialiser.serialise(op, true);
        final GetElements deserialisedOp = JSONSerialiser.deserialise(json, GetElements.class);

        // Then
        final Iterator<?> itr = deserialisedOp.getInput().iterator();
        assertThat(itr.next()).isEqualTo(elementSeed1);
        assertThat(itr.next()).isEqualTo(elementSeed2);
        assertThat(itr).isExhausted();
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
        assertThat((Iterable<ElementId>) deserialisedOp.getInput())
                .containsExactly(new EntitySeed(1), new EntitySeed(new TypeSubTypeValue("t", "s", "v")), new EntitySeed(2L));
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
        assertThat((Iterable<ElementId>) deserialisedOp.getInput())
                .containsExactly(new EntitySeed(1), new EntitySeed(new TypeSubTypeValue("t", "s", "v")), new EntitySeed(2L));
    }

    private void builderShouldCreatePopulatedOperationAll() {
        final GetElements op = new GetElements.Builder()
                .input(new EntitySeed("A"), 1, new EdgeSeed(2L, 3L))
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();

        assertThat(op.getIncludeIncomingOutGoing()).isEqualTo(SeededGraphFilters.IncludeIncomingOutgoingType.EITHER);
        assertThat(op.getView()).isNotNull();
        assertThat((Iterable<ElementId>) op.getInput())
                .containsExactly(new EntitySeed("A"), new EntitySeed(1), new EdgeSeed(2L, 3L));
    }

    private void builderShouldCreatePopulatedOperationIncoming() {
        final ElementSeed seed = new EntitySeed("A");
        final GetElements op = new GetElements.Builder()
                .input(seed)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertThat(op.getIncludeIncomingOutGoing()).isEqualTo(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING);
        assertThat(op.getView()).isNotNull();
        assertThat(op.getInput().iterator().next()).isEqualTo(seed);
    }

    @Test
    public void shouldSetDirectedTypeToBoth() {
        // When
        final GetElements op = new GetElements.Builder()
                .directedType(DirectedType.EITHER)
                .input(new EntitySeed())
                .build();

        // Then
        assertThat(op.getDirectedType()).isEqualTo(DirectedType.EITHER);
    }

    @Test
    public void shouldSetOptionToValue() {
        // When
        final GetElements op = new GetElements.Builder()
                .option("key", "value")
                .input(new EntitySeed())
                .build();

        // Then
        assertThat(op.getOptions())
                .isNotNull()
                .containsEntry("key", "value");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        builderShouldCreatePopulatedOperationAll();
        builderShouldCreatePopulatedOperationIncoming();
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final EntitySeed input = new EntitySeed("A");
        final View view = new View.Builder()
                .edge("testEdgeGroup")
                .build();
        final GetElements getElements = new GetElements.Builder()
                .input(input)
                .inOutType(IncludeIncomingOutgoingType.EITHER)
                .view(view)
                .directedType(DirectedType.DIRECTED)
                .option("testOption", "true")
                .build();

        // When
        final GetElements clone = getElements.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(getElements);
        assertThat(clone.getInput()).asInstanceOf(InstanceOfAssertFactories.iterable(EntitySeed.class)).containsExactly(input);
        assertThat(clone.getIncludeIncomingOutGoing()).isEqualTo(IncludeIncomingOutgoingType.EITHER);
        assertThat(clone.getView()).isEqualTo(view);
        assertThat(clone.getDirectedType()).isEqualTo(DirectedType.DIRECTED);
        assertThat(clone.getOption("testOption")).isEqualTo("true");
    }

    @Test
    public void shouldCreateInputFromVertices() {
        // When
        final GetElements op = new GetElements.Builder()
                .input("1", new EntitySeed("2"), new Entity("group1", "3"), new EdgeSeed("4", "5"), new Edge("group", "6", "7", true))
                .build();

        // Then
        assertThat((Iterable<ElementId>) op.getInput())
                .containsExactly(new EntitySeed("1"), new EntitySeed("2"), new Entity("group1", "3"), new EdgeSeed("4", "5"),
                        new Edge("group", "6", "7", true));
    }

    @Override
    protected GetElements getTestObject() {
        return new GetElements();
    }
}
