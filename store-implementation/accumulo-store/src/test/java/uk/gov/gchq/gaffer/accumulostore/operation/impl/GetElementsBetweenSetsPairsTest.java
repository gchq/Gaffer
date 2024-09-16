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

package uk.gov.gchq.gaffer.accumulostore.operation.impl;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class GetElementsBetweenSetsPairsTest extends OperationTest<GetElementsBetweenSetsPairs> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetElementsBetweenSetsPairs op = new GetElementsBetweenSetsPairs.Builder()
                .input(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1)
                .inputB(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final GetElementsBetweenSetsPairs deserialisedOp = JSONSerialiser.deserialise(json, GetElementsBetweenSetsPairs.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getInput().getFirst().iterator();
        assertThat(itrSeedsA.next()).isEqualTo(AccumuloTestData.SEED_SOURCE_1);
        assertThat(itrSeedsA.next()).isEqualTo(AccumuloTestData.SEED_DESTINATION_1);
        assertThat(itrSeedsA).isExhausted();

        final Iterator itrSeedsB = deserialisedOp.getInput().getSecond().iterator();
        assertThat(itrSeedsB.next()).isEqualTo(AccumuloTestData.SEED_SOURCE_2);
        assertThat(itrSeedsB.next()).isEqualTo(AccumuloTestData.SEED_DESTINATION_2);
        assertThat(itrSeedsB).isExhausted();

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsBetweenSetsPairs getElementsBetweenSets = new GetElementsBetweenSetsPairs.Builder()
                .input(AccumuloTestData.SEED_B)
                .inputB(AccumuloTestData.SEED_A)
                .directedType(DirectedType.UNDIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder()
                        .edge("testEdgeGroup")
                        .build())
                .build();
        assertEquals("true", getElementsBetweenSets.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(DirectedType.UNDIRECTED, getElementsBetweenSets.getDirectedType());
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING, getElementsBetweenSets.getIncludeIncomingOutGoing());
        assertThat(getElementsBetweenSets.getInput().getFirst().iterator().next()).isEqualTo(AccumuloTestData.SEED_B);
        assertThat(getElementsBetweenSets.getInput().getSecond().iterator().next()).isEqualTo(AccumuloTestData.SEED_A);
        assertNotNull(getElementsBetweenSets.getView());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final View view = new View.Builder()
                .edge("testEdgeGroup")
                .build();
        final GetElementsBetweenSetsPairs getElementsBetweenSets = new GetElementsBetweenSetsPairs.Builder()
                .input(AccumuloTestData.SEED_B)
                .inputB(AccumuloTestData.SEED_A)
                .directedType(DirectedType.UNDIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(view)
                .build();

        // When
        final GetElementsBetweenSetsPairs clone = getElementsBetweenSets.shallowClone();

        // Then
        assertNotSame(getElementsBetweenSets, clone);
        assertEquals("true", clone.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(DirectedType.UNDIRECTED, clone.getDirectedType());
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING, clone.getIncludeIncomingOutGoing());
        assertThat(clone.getInput().getFirst().iterator().next()).isEqualTo(AccumuloTestData.SEED_B);
        assertThat(clone.getInput().getSecond().iterator().next()).isEqualTo(AccumuloTestData.SEED_A);
        assertEquals(view, clone.getView());
    }

    @Test
    public void shouldCreateInputFromVertices() {
        // Given
        final GetElementsBetweenSetsPairs op = new GetElementsBetweenSetsPairs.Builder()
                .input(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1.getVertex())
                .inputB(AccumuloTestData.SEED_A, AccumuloTestData.SEED_A1.getVertex())
                .build();

        // Then
        assertEquals(
                Lists.newArrayList(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1),
                Lists.newArrayList(op.getInput().getFirst())
        );
        assertEquals(
                Lists.newArrayList(AccumuloTestData.SEED_A, AccumuloTestData.SEED_A1),
                Lists.newArrayList(op.getInput().getSecond())
        );
    }

    @Override
    protected GetElementsBetweenSetsPairs getTestObject() {
        return new GetElementsBetweenSetsPairs();
    }
}
