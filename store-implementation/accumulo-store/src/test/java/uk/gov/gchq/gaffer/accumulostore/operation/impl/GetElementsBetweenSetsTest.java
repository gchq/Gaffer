/*
 * Copyright 2016-2019 Crown Copyright
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
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloTestData;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class GetElementsBetweenSetsTest extends OperationTest<GetElementsBetweenSets> {

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder()
                .input(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1)
                .inputB(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final GetElementsBetweenSets deserialisedOp = JSONSerialiser.deserialise(json, GetElementsBetweenSets.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getInput().iterator();
        assertEquals(AccumuloTestData.SEED_SOURCE_1, itrSeedsA.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_1, itrSeedsA.next());
        assertFalse(itrSeedsA.hasNext());

        final Iterator itrSeedsB = deserialisedOp.getInputB().iterator();
        assertEquals(AccumuloTestData.SEED_SOURCE_2, itrSeedsB.next());
        assertEquals(AccumuloTestData.SEED_DESTINATION_2, itrSeedsB.next());
        assertFalse(itrSeedsB.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsBetweenSets getElementsBetweenSets = new GetElementsBetweenSets.Builder()
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
        assertEquals(AccumuloTestData.SEED_B, getElementsBetweenSets.getInput().iterator().next());
        assertEquals(AccumuloTestData.SEED_A, getElementsBetweenSets.getInputB().iterator().next());
        assertNotNull(getElementsBetweenSets.getView());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final View view = new View.Builder()
                .edge("testEdgeGroup")
                .build();
        final GetElementsBetweenSets getElementsBetweenSets = new GetElementsBetweenSets.Builder()
                .input(AccumuloTestData.SEED_B)
                .inputB(AccumuloTestData.SEED_A)
                .directedType(DirectedType.UNDIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .seedMatching(SeedMatchingType.EQUAL)
                .view(view)
                .build();

        // When
        final GetElementsBetweenSets clone = getElementsBetweenSets.shallowClone();

        // Then
        assertNotSame(getElementsBetweenSets, clone);
        assertEquals("true", clone.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertEquals(DirectedType.UNDIRECTED, clone.getDirectedType());
        assertEquals(SeedMatchingType.EQUAL, clone.getSeedMatching());
        assertEquals(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING, clone.getIncludeIncomingOutGoing());
        assertEquals(AccumuloTestData.SEED_B, clone.getInput().iterator().next());
        assertEquals(AccumuloTestData.SEED_A, clone.getInputB().iterator().next());
        assertEquals(view, clone.getView());
    }

    @Test
    public void shouldCreateInputFromVertices() {
        // Given
        final GetElementsBetweenSets op = new GetElementsBetweenSets.Builder()
                .input(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1.getVertex())
                .inputB(AccumuloTestData.SEED_A, AccumuloTestData.SEED_A1.getVertex())
                .build();

        // Then
        assertEquals(
                Lists.newArrayList(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1),
                Lists.newArrayList(op.getInput())
        );
        assertEquals(
                Lists.newArrayList(AccumuloTestData.SEED_A, AccumuloTestData.SEED_A1),
                Lists.newArrayList(op.getInputB())
        );
    }

    @Override
    protected GetElementsBetweenSets getTestObject() {
        return new GetElementsBetweenSets();
    }
}
