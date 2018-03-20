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

package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.CustomVertex;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Date;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class OperationImplTest extends OperationTest<OperationImpl> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final String requiredField1 = "value1";
        final CustomVertex requiredField2 = new CustomVertex("type1", "value1");
        final Date optionalField1 = new Date(1L);
        final CustomVertex optionalField2 = new CustomVertex("type2", "value2");
        final OperationImpl op = new OperationImpl.Builder()
                .requiredField1(requiredField1)
                .requiredField2(requiredField2)
                .optionalField1(optionalField1)
                .optionalField2(optionalField2)
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final OperationImpl deserialisedOp = JSONSerialiser.deserialise(json, OperationImpl.class);

        // Then
        assertEquals(requiredField1, deserialisedOp.getRequiredField1());
        assertEquals(requiredField2, deserialisedOp.getRequiredField2());
        assertEquals(optionalField1, deserialisedOp.getOptionalField1());
        assertEquals(optionalField2, deserialisedOp.getOptionalField2());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given / When
        final String requiredField1 = "value1";
        final CustomVertex requiredField2 = new CustomVertex("type1", "value1");
        final Date optionalField1 = new Date(1L);
        final CustomVertex optionalField2 = new CustomVertex("type2", "value2");
        final OperationImpl op = new OperationImpl.Builder()
                .requiredField1(requiredField1)
                .requiredField2(requiredField2)
                .optionalField1(optionalField1)
                .optionalField2(optionalField2)
                .build();

        // Then
        assertEquals(requiredField1, op.getRequiredField1());
        assertEquals(requiredField2, op.getRequiredField2());
        assertEquals(optionalField1, op.getOptionalField1());
        assertEquals(optionalField2, op.getOptionalField2());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final String requiredField1 = "value1";
        final CustomVertex requiredField2 = new CustomVertex("type1", "value1");
        final Date optionalField1 = new Date(1L);
        final CustomVertex optionalField2 = new CustomVertex("type2", "value2");
        final OperationImpl op = new OperationImpl.Builder()
                .requiredField1(requiredField1)
                .requiredField2(requiredField2)
                .optionalField1(optionalField1)
                .optionalField2(optionalField2)
                .build();

        // When
        OperationImpl clone = op.shallowClone();

        // Then
        assertNotSame(op, clone);
        assertEquals(requiredField1, clone.getRequiredField1());
        assertEquals(requiredField2, clone.getRequiredField2());
        assertEquals(optionalField1, clone.getOptionalField1());
        assertEquals(optionalField2, clone.getOptionalField2());
    }

    @Test
    public void shouldValidateASingleMissingRequiredField() throws SerialisationException {
        // Given
        final String requiredField1 = "value1";
        final Date optionalField1 = new Date(1L);
        final CustomVertex optionalField2 = new CustomVertex("type2", "value2");
        final OperationImpl op = new OperationImpl.Builder()
                .requiredField1(requiredField1)
                .optionalField1(optionalField1)
                .optionalField2(optionalField2)
                .build();

        // When
        final ValidationResult validationResult = op.validate();

        // Then
        assertEquals(
                Sets.newHashSet("requiredField2 is required for: " + op.getClass().getSimpleName()),
                validationResult.getErrors()
        );
    }

    @Override
    public Set<String> getRequiredFields() {
        return Sets.newHashSet("requiredField1", "requiredField2");
    }

    @Override
    protected OperationImpl getTestObject() {
        return new OperationImpl();
    }
}

