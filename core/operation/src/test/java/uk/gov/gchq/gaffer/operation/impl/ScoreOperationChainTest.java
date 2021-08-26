/*
 * Copyright 2016-2021 Crown Copyright
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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class ScoreOperationChainTest extends OperationTest<ScoreOperationChain> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException, JsonProcessingException {
        // Given
        final ScoreOperationChain op = new ScoreOperationChain();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final ScoreOperationChain deserialisedOp = JSONSerialiser.deserialise(json, ScoreOperationChain.class);

        // Then
        assertNotNull(deserialisedOp);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final OperationChain opChain = new OperationChain();
        final ScoreOperationChain scoreOperationChain = new ScoreOperationChain.Builder()
                .operationChain(opChain)
                .build();

        // Then
        assertThat(scoreOperationChain.getOperationChain()).isNotNull();
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final OperationChain opChain = new OperationChain();
        final ScoreOperationChain scoreOperationChain = new ScoreOperationChain.Builder()
                .operationChain(opChain)
                .build();

        // When
        ScoreOperationChain clone = scoreOperationChain.shallowClone();

        // Then
        assertNotSame(scoreOperationChain, clone);
        assertEquals(opChain, clone.getOperationChain());
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Integer.class, outputClass);
    }

    @Override
    protected ScoreOperationChain getTestObject() {
        return new ScoreOperationChain();
    }
}
