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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.generator.ElementGeneratorImpl;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;


public class GenerateElementsTest extends OperationTest<GenerateElements> {
    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("elementGenerator");
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertEquals(Iterable.class, outputClass);
    }

    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final GenerateElements<String> op = new GenerateElements.Builder<String>()
                .input("obj 1", "obj 2")
                .generator(new ElementGeneratorImpl())
                .build();

        // When
        byte[] json = JSONSerialiser.serialise(op, true);
        final GenerateElements<String> deserialisedOp = JSONSerialiser.deserialise(json, GenerateElements.class);

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
        GenerateElements<?> generateElements = new GenerateElements.Builder<String>()
                .generator(new ElementGeneratorImpl())
                .input("Test1", "Test2")
                .build();
        Iterator iter = generateElements.getInput().iterator();
        assertEquals("Test1", iter.next());
        assertEquals("Test2", iter.next());
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        List<String> input = Lists.newArrayList("test1", "test2");
        ElementGeneratorImpl generator = new ElementGeneratorImpl();
        GenerateElements<?> generateElements = new GenerateElements.Builder<String>()
                .generator(generator)
                .input(input)
                .build();

        // When
        GenerateElements clone = generateElements.shallowClone();

        // Then
        assertNotSame(generateElements, clone);
        assertEquals(input, clone.getInput());
        assertEquals(generator, clone.getElementGenerator());
    }

    @Override
    protected GenerateElements getTestObject() {
        return new GenerateElements();
    }
}
