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
import uk.gov.gchq.gaffer.operation.data.WalkDefinition;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.comparesEqualTo;
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
                .walkDefinitions(new WalkDefinition.Builder()
                        .operation(new GetElements())
                        .build())
                .resultsLimit(100)
                .build();

        // Then
        assertThat(getWalks.getInput(), is(notNullValue()));
        assertThat(getWalks.getInput(), iterableWithSize(2));
        assertThat(getWalks.getResultsLimit(), is(equalTo(100)));
        assertThat(getWalks.getWalkDefinitions(), iterableWithSize(1));
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
                .walkDefinition(new WalkDefinition.Builder()
                        .operation(new GetElements.Builder()
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .build())
                .walkDefinition(new WalkDefinition.Builder()
                        .operation(new GetElements.Builder()
                                .input(new EntitySeed("seed"))
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
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
                .walkDefinitions(new WalkDefinition.Builder()
                        .operation(new GetElements.Builder()
                                .input()
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
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
                .walkDefinitions(new WalkDefinition.Builder()
                        .preFilter(new ScoreOperationChain())
                        .operation(new GetElements.Builder()
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
        assertTrue(result.getErrorString().contains("The pre operation filter " +
                "uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain does not accept an input."));
    }

    @Test
    public void shouldValidateWhenPostFiltersContainsAnOperationWhichDoesNotAllowAnInput() {
        // Given
        final GetWalks getWalks = new GetWalks.Builder()
                .input(new EntitySeed("1"), new EntitySeed("2"))
                .walkDefinitions(new WalkDefinition.Builder()
                        .postFilter(new ScoreOperationChain())
                        .operation(new GetElements.Builder()
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
        assertTrue(result.getErrorString().contains("The post operation filter " +
                "uk.gov.gchq.gaffer.operation.impl.ScoreOperationChain does not accept an input."));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final List<EntitySeed> input = Lists.newArrayList(new EntitySeed("1"), new EntitySeed("2"));
        final List<WalkDefinition> walkDefinitions = Lists.newArrayList(new WalkDefinition.Builder().operation(new GetElements()).build());
        final GetWalks getWalks = new GetWalks.Builder()
                .input(input)
                .walkDefinitions(walkDefinitions)
                .build();

        // When
        final GetWalks clone = getWalks.shallowClone();

        // Then
        assertNotSame(getWalks, clone);
        assertEquals(input, Lists.newArrayList(clone.getInput()));
        int i = 0;
        for (final WalkDefinition walkDef : clone.getWalkDefinitions()) {

            final WalkDefinition original = walkDefinitions.get(i);
            final GetElements originalOp = original.getOperation();

            assertNotSame(original, clone);
            assertEquals(original.getPostFilters(), walkDef.getPostFilters());
            assertEquals(original.getPreFilters(), walkDef.getPreFilters());

            assertEquals(originalOp.getSeedMatching(), walkDef.getOperation().getSeedMatching());
            assertEquals(originalOp.getDirectedType(), walkDef.getOperation().getDirectedType());
            assertEquals(originalOp.getView(), walkDef.getOperation().getView());
            assertEquals(originalOp.getIncludeIncomingOutGoing(), walkDef.getOperation().getIncludeIncomingOutGoing());
            assertEquals(originalOp.getInput(), walkDef.getOperation().getInput());
            assertEquals(originalOp.getOptions(), walkDef.getOperation().getOptions());

            i++;
        }
    }

    @Override
    protected GetWalks getTestObject() {
        return new GetWalks.Builder()
                .walkDefinition(new WalkDefinition.Builder()
                        .operation(new GetElements.Builder()
                                .view(new View.Builder()
                                        .edge(TestGroups.EDGE)
                                        .build())
                                .build())
                        .build())
                .build();
    }
}