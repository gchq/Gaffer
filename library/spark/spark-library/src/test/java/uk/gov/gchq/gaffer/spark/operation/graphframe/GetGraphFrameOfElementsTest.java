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

package uk.gov.gchq.gaffer.spark.operation.graphframe;

import org.hamcrest.core.StringContains;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.koryphe.ValidationResult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GetGraphFrameOfElementsTest extends OperationTest<GetGraphFrameOfElements> {

    @Test
    public void shouldValidateOperationIfViewDoesNotContainEdgesOrEntities() {
        // Given
        final GetGraphFrameOfElements opWithEdgesOnly = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        final GetGraphFrameOfElements opWithEntitiesOnly = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();

        final GetGraphFrameOfElements opWithEmptyView = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .build())
                .build();

        // Then
        assertTrue(opWithEdgesOnly.validate().isValid());
        assertTrue(opWithEntitiesOnly.validate().isValid());
        assertFalse(opWithEmptyView.validate().isValid());
    }

    @Test
    public void shouldValidateOperation() {
        // Given
        final Operation op = getTestObject();

        // When
        final ValidationResult validationResult = op.validate();

        // Then
        assertTrue(validationResult.isValid());
    }

    @Test
    public void shouldValidateOperationWhenViewContainsElementsWithReservedPropertyNames() {
        // Given
        final Operation op = new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                .properties("vertex")
                                .build())
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        // When
        final ValidationResult validationResult = op.validate();

        // Then
        assertTrue(validationResult.isValid());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final GetGraphFrameOfElements op = getTestObject();

        // Then
        assertThat(op.getView(), is(notNullValue()));
        assertThat(op.getView().getEdgeGroups(), hasItem(TestGroups.EDGE));
        assertThat(op.getView().getEntityGroups(), hasItem(TestGroups.ENTITY));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final GetGraphFrameOfElements op = getTestObject();

        // When
        final GetGraphFrameOfElements clone = op.shallowClone();

        // Then
        assertThat(op, is(not(sameInstance(clone))));
        assertThat(op.getView(), is(equalTo(clone.getView())));
    }

    @Override
    protected GetGraphFrameOfElements getTestObject() {
        return new GetGraphFrameOfElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .entity(TestGroups.ENTITY)
                        .build())
                .build();
    }
}
