/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.operation.HasTrait.Builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.store.StoreTrait.VISIBILITY;

public class HasTraitTest extends OperationTest<HasTrait> {

    @Override
    protected HasTrait getTestObject() {
        return new HasTrait();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        HasTrait op = new Builder()
                .currentTraits(false)
                .trait(VISIBILITY)
                .build();

        assertEquals(false, op.isCurrentTraits());
        assertEquals(VISIBILITY, op.getTrait());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        HasTrait op = new Builder()
                .currentTraits(false)
                .trait(VISIBILITY)
                .build();

        HasTrait clone = op.shallowClone();

        assertEquals(op.isCurrentTraits(), clone.isCurrentTraits());
        assertEquals(op.getTrait(), clone.getTrait());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final HasTrait obj = new HasTrait.Builder()
                .currentTraits(true)
                .trait(VISIBILITY)
                .build();

        // When
        final byte[] json = toJson(obj);
        final HasTrait deserialisedObj = fromJson(json);

        // Then
        assertEquals(obj.isCurrentTraits(), deserialisedObj.isCurrentTraits());
        assertEquals(obj.getTrait(), deserialisedObj.getTrait());
    }
}
