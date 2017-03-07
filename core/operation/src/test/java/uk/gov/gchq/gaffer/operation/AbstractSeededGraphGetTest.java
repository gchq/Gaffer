/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.operation;

import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.SeededGraphGetImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class AbstractSeededGraphGetTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final String identifier = "identifier";
        final ElementId input = new EntitySeed(identifier);
        final SeededGraphGetImpl<ElementId, Element> op = new SeededGraphGetImpl.Builder<ElementId, Element>()
                .addSeed(input)
                .build();

        // When
        byte[] json = serialiser.serialise(op, true);
        final SeededGraphGetImpl<ElementId, Element> deserialisedOp = serialiser.deserialise(json, SeededGraphGetImpl.class);

        // Then
        assertNotNull(deserialisedOp);
        assertEquals(identifier, ((EntityId) deserialisedOp.getInput().iterator().next()).getVertex());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        //GetOperationImpl is a test object and has no builder.
    }
}
