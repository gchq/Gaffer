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

package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.operation.impl.GetWalks.HOP_DEFINITION;

class GetWalksTest extends OperationTest<GetWalks> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(new GetElements())
                .resultsLimit(100)
                .build();

        // Then
        Assertions.<EntityId>assertThat(getWalks.getInput())
                .hasSize(2)
                .containsOnly(new EntitySeed("1"), new EntitySeed("2"));
        assertThat(getWalks.getResultsLimit()).isEqualTo(100);
        assertThat(getWalks.getOperations()).hasSize(1);
    }

    @Test
    void shouldFailValidationWithNoHops() {
        // Given
        final GetWalks operation = getTestObject();

        // When
        ValidationResult result = operation.validate();
        Set<String> expectedErrors = Sets.newHashSet("No hops were provided. " + HOP_DEFINITION);

        // Then
        assertEquals(result.getErrors(), expectedErrors);
    }

    @Test
    void shouldValidateOperationWhenNoOperationsProvided() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .build();

        // Then
        assertFalse(getWalks.validate().isValid());
    }

    @Test
    void shouldValidateOperationWhenSecondOperationContainsNonNullInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(
                new GetElements.Builder()
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
    void shouldValidateOperationWhenFirstOperationContainsNonNullInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(new GetElements.Builder()
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
    void shouldValidateWhenPreFiltersContainsAnOperationWhichDoesNotAllowAnInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(new OperationChain.Builder()
                        .first(new ScoreOperationChain())
                        .then(new DiscardOutput())
                        .then(new GetElements.Builder()
                                .input()
                                .view(new View.Builder().edge(TestGroups.EDGE).build())
                                .build())
                        .build())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains("The first operation in operation chain 0: "
                        + ScoreOperationChain.class.getName() + " is not be able to accept the input seeds."),
                        result.getErrorString());
    }

    @Test
    void shouldValidateWhenOperationListContainsAnEmptyOperationChain() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(new GetElements.Builder()
                        .input((Iterable<? extends ElementId>) null)
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build(),
                        new OperationChain())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains("Operation chain 1 contains no operations"),
                        result.getErrorString());
    }

    @Test
    void shouldValidateWhenOperationListDoesNotContainAGetElementsOperation() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(
                new GetElements.Builder()
                        .input((Iterable<? extends ElementId>) null)
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build(),
                new OperationChain.Builder()
                        .first(new AddElements())
                        .build(),
                new GetElements.Builder()
                        .input((Iterable<? extends ElementId>) null)
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE)
                                .build())
                        .build())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains(
                        "All operations must contain a single hop. Operation 1 does not contain a hop."),
                        result.getErrorString());
    }

    @Test
    void shouldValidateWhenOperationContainsMultipleHops() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .addOperations(new OperationChain.Builder()
                        .first(new GetElements.Builder()
                                .input((Iterable<? extends ElementId>) null)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .then(new GetElements.Builder()
                                .input((Iterable<? extends ElementId>) null)
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .build())
                .build();

        // Then
        final ValidationResult result = getWalks.validate();
        assertFalse(result.isValid());
        assertTrue(result.getErrorString().contains("All operations must contain a single hop. Operation ")
                        && result.getErrorString().contains(" contains multiple hops"),
                        result.getErrorString());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final List<EntitySeed> input = Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"));
        final GetElements getElements = new GetElements();
        final GetWalks getWalks = new GetWalks.Builder()
                .input(input)
                .addOperations(getElements)
                .build();

        // When
        final GetWalks clone = getWalks.shallowClone();

        // Then
        assertNotSame(getWalks, clone);
        assertEquals(input, Lists.newArrayList(clone.getInput()));
        for (final Output<Iterable<Element>> operation : clone.getOperations()) {
            assertNotSame(getElements, operation);
            assertEquals(OperationChain.class, operation.getClass());
        }
    }

    @Override
    protected GetWalks getTestObject() {
        return new GetWalks();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("operations");
    }
}
