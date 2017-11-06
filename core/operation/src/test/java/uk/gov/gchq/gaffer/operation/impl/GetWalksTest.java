/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GetWalksTest extends OperationTest<GetWalks> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new GetElements())
                .resultsLimit(100)
                .build();

        // Then
        assertThat(getWalks.getInput(), is(notNullValue()));
        assertThat(getWalks.getInput(), iterableWithSize(2));
        assertThat(getWalks.getResultsLimit(), is(equalTo(100)));
        assertThat(getWalks.getOperations(), iterableWithSize(1));
        assertThat(getWalks.getInput(), containsInAnyOrder(new EntitySeed("1"), new EntitySeed("2")));
    }

    @Test
    public void shouldValidateOperationWhenNoOperationsProvided() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .build();

        // Then
        assertFalse(getWalks.validate().isValid());
    }

    @Test
    public void shouldValidateOperationWhenSecondOperationContainsNonNullInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new GetElements.Builder()
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build(),
                        new GetElements.Builder()
                                .input(new EntitySeed("seed"))
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                .build();

        // Then
        assertFalse(getWalks.validate().isValid());
    }

    @Test
    public void shouldValidateOperationWhenFirstOperationContainsNonNullInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new GetElements.Builder()
                        .input()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build())
                .build();

        // Then
        assertFalse(getWalks.validate().isValid());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final List<EntitySeed> input = Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"));
        final List<GetElements> operations = Lists.newArrayList(new GetElements());
        final GetWalks getWalks = new GetWalks.Builder()
                .input(input)
                .operations(operations)
                .build();

        // When
        final GetWalks clone = getWalks.shallowClone();

        // Then
        assertNotSame(getWalks, clone);
        assertEquals(input, Lists.newArrayList(clone.getInput()));
        int i = 0;
        for (final GetElements op : clone.getOperations()) {
            final GetElements original = operations.get(i);
            assertNotSame(original, clone);
            assertEquals(original.getInput(), op.getInput());
            assertEquals(original.getIncludeIncomingOutGoing(), op.getIncludeIncomingOutGoing());
            assertEquals(original.getView(), op.getView());
            assertEquals(original.getDirectedType(), op.getDirectedType());
            assertEquals(original.getSeedMatching(), op.getSeedMatching());
            i++;
        }
    }

    @Override
    protected GetWalks getTestObject() {
        return new GetWalks.Builder().operations(new GetElements.Builder().view(new View.Builder().edge(TestGroups.EDGE).build()).build()).build();
    }
}