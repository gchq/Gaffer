/*
 * Copyright 2017-2018 Crown Copyright
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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.koryphe.ValidationResult;

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
                .addOperations(new GetElements.Builder()
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
                        .input(new EntitySeed("some value"))
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build())
                .build();

        // Then
        assertFalse(getWalks.validate().isValid());
    }

    @Test
    public void shouldValidateWhenPreFiltersContainsAnOperationWhichDoesNotAllowAnInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new OperationChain.Builder()
                        .first(new ScoreOperationChain())
                        .then(new DiscardOutput())
                        .then(new GetElements.Builder()
                                .input()
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .build())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("The first operation in operation chain 0: " + ScoreOperationChain.class.getName() + " is not be able to accept the input seeds."));
    }

    @Test
    public void shouldValidateWhenOperationListContainsAnEmptyOperationChain() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new GetElements.Builder().input((Iterable<? extends ElementId>) null)
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build(), new OperationChain())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("Operation chain 1 contains no operations"));
    }

    @Test
    public void shouldValidateWhenOperationListDoesNotContainAGetElementsOperation() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new GetElements.Builder().input((Iterable<? extends ElementId>) null)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build(),
                        new OperationChain.Builder()
                                .first(new AddElements())
                                .build(),
                        new GetElements.Builder().input((Iterable<? extends ElementId>) null)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("All operations must contain a single hop. Operation 1 does not contain a hop."));
    }

    @Test
    public void shouldValidateWhenOperationContainsMultipleHops() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .operations(new OperationChain.Builder()
                        .first(new GetElements.Builder().input((Iterable<? extends ElementId>) null)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .then(new GetElements.Builder().input((Iterable<? extends ElementId>) null)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .build())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString(), result.getErrorString().contains("All operations must contain a single hop. Operation ") && result.getErrorString().contains(" contains multiple hops"));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final List<EntitySeed> input = Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"));
        final GetElements getElements = new GetElements();
        final GetWalks getWalks = new GetWalks.Builder()
                .input(input)
                .operations(getElements)
                .build();

        // When
        final GetWalks clone = getWalks.shallowClone();

        // Then
        assertNotSame(getWalks, clone);
        assertEquals(input, Lists.newArrayList(clone.getInput()));
        int i = 0;
        for (final Output<Iterable<Element>> operation : clone.getOperations()) {
            assertNotSame(getElements, operation);
            assertEquals(OperationChain.class, operation.getClass());
            i++;
        }
    }

    @Override
    protected GetWalks getTestObject() {
        return new GetWalks.Builder()
                .operations(new GetElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build())
                .build();
    }
}
