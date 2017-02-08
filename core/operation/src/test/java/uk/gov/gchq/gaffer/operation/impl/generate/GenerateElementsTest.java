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

package uk.gov.gchq.gaffer.operation.impl.generate;

import org.junit.Test;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.generator.ElementGeneratorImpl;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import java.util.Arrays;
import java.util.Iterator;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class GenerateElementsTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final GenerateElements<String> op = new GenerateElements<>(Arrays.asList("obj 1", "obj 2"), new ElementGeneratorImpl());

        // When
        byte[] json = serialiser.serialise(op, true);
        final GenerateElements deserialisedOp = serialiser.deserialise(json, GenerateElements.class);

        // Then
        final Iterator itr = deserialisedOp.getInput().iterator();
        assertEquals("obj 1", itr.next());
        assertEquals("obj 2", itr.next());
        assertFalse(itr.hasNext());

        assertTrue(deserialisedOp.getElementGenerator() instanceof ElementGeneratorImpl);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GenerateElements generateElements = new GenerateElements.Builder<String>().generator(new ElementGeneratorImpl())
                .objects(Arrays.asList("Test1", "Test2"))
                .view(new View.Builder().edge("TestEdgeGroup").build())
                .option("testOption", "true").build();
        assertNotNull(generateElements.getView());
        assertEquals("true", generateElements.getOption("testOption"));
        Iterator iter = generateElements.getInput().iterator();
        assertEquals("Test1", iter.next());
        assertEquals("Test2", iter.next());
    }
}
