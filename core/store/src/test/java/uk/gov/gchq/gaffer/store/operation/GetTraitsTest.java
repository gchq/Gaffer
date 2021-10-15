/*
 * Copyright 2018-2021 Crown Copyright
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
import uk.gov.gchq.gaffer.store.operation.GetTraits.Builder;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetTraitsTest extends OperationTest<GetTraits> {

    @Override
    protected GetTraits getTestObject() {
        return new GetTraits();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetTraits op = new Builder().currentTraits(false).build();

        assertEquals(false, op.isCurrentTraits());
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        GetTraits op = new Builder().currentTraits(false).build();

        GetTraits clone = op.shallowClone();

        assertEquals(op.isCurrentTraits(), clone.isCurrentTraits());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final GetTraits obj = new GetTraits.Builder().currentTraits(true).build();

        // When
        final byte[] json = toJson(obj);
        final GetTraits deserialisedObj = fromJson(json);

        // Then
        assertEquals(obj.isCurrentTraits(), deserialisedObj.isCurrentTraits());
    }

    @Test
    @Override
    public void shouldHaveSinceAnnotation() {
        // TODO Auto-generated method stub
        super.shouldHaveSinceAnnotation();
    }

    @Test
    @Override
    public void shouldHaveSummaryAnnotation() {
        // TODO Auto-generated method stub
        super.shouldHaveSummaryAnnotation();
    }
}
