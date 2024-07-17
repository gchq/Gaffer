/*
 * Copyright 2016-2024 Crown Copyright
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

class GetElementsWithinSetTest extends OperationTest<GetElementsWithinSet> {

    @Test
    void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_SOURCE_1,
                        AccumuloTestData.SEED_DESTINATION_1,
                        AccumuloTestData.SEED_SOURCE_2,
                        AccumuloTestData.SEED_DESTINATION_2)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);

        final GetElementsWithinSet deserialisedOp = JSONSerialiser.deserialise(json, GetElementsWithinSet.class);

        // Then
        final Iterator itrSeedsA = deserialisedOp.getInput().iterator();
        assertThat(itrSeedsA.next()).isEqualTo(AccumuloTestData.SEED_SOURCE_1);
        assertThat(itrSeedsA.next()).isEqualTo(AccumuloTestData.SEED_DESTINATION_1);
        assertThat(itrSeedsA.next()).isEqualTo(AccumuloTestData.SEED_SOURCE_2);
        assertThat(itrSeedsA.next()).isEqualTo(AccumuloTestData.SEED_DESTINATION_2);
        assertThat(itrSeedsA).isExhausted();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_A)
                .directedType(DirectedType.DIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(new View.Builder()
                        .edge("testEdgegroup")
                        .build())
                .build();

        assertThat(getElementsWithinSet.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY)).isEqualTo("true");
        assertThat(getElementsWithinSet.getDirectedType()).isEqualTo(DirectedType.DIRECTED);
        assertThat(getElementsWithinSet.getIncludeIncomingOutGoing()).isEqualTo(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING);
        assertThat(getElementsWithinSet.getInput().iterator().next()).isEqualTo(AccumuloTestData.SEED_A);
        assertThat(getElementsWithinSet.getView()).isNotNull();
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final View view = new View.Builder()
                .edge("testEdgegroup")
                .build();
        final GetElementsWithinSet getElementsWithinSet = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_A)
                .directedType(DirectedType.DIRECTED)
                .inOutType(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .view(view)
                .build();

        // When
        final GetElementsWithinSet clone = getElementsWithinSet.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(getElementsWithinSet);
        assertThat(clone.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY)).isEqualTo("true");
        assertThat(clone.getIncludeIncomingOutGoing()).isEqualTo(SeededGraphFilters.IncludeIncomingOutgoingType.INCOMING);
        assertThat(clone.getInput().iterator().next()).isEqualTo(AccumuloTestData.SEED_A);
        assertThat(clone.getView()).isEqualTo(view);
    }

    @Test
    void shouldCreateInputFromVertices() {
        // When
        final GetElementsWithinSet op = new GetElementsWithinSet.Builder()
                .input(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1.getVertex())
                .build();

        // Then
        assertThat(Lists.newArrayList(op.getInput())).isEqualTo(Lists.newArrayList(AccumuloTestData.SEED_B, AccumuloTestData.SEED_B1));
    }

    @Override
    protected GetElementsWithinSet getTestObject() {
        return new GetElementsWithinSet();
    }
}
