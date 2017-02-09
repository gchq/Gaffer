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
package uk.gov.gchq.gaffer.function.transform;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.function.TransformFunctionTest;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class ConcatTest extends TransformFunctionTest {
    @Test
    public void shouldConcatStringsWithDefaultSeparator() {
        // Given
        final Concat concat = new Concat();

        // When
        final Object[] output = concat.transform(new String[]{"1", "2", "3"});

        assertArrayEquals(new String[]{"1,2,3"}, output);
    }

    @Test
    public void shouldConcatStringsWithGivenSeparator() {
        // Given
        final Concat concat = new Concat();
        concat.setSeparator(" ");

        // When
        final Object[] output = concat.transform(new String[]{"1", "2", "3"});

        assertArrayEquals(new String[]{"1 2 3"}, output);
    }

    @Test
    public void shouldConvertNullValuesToEmptyStringWhenConcatenating() {
        // Given
        final Concat concat = new Concat();

        // When
        final Object[] output = concat.transform(new String[]{"1", null, "3"});

        assertArrayEquals(new String[]{"1,,3"}, output);
    }

    @Test
    public void shouldReturnEmptyArrayForNullInput() {
        // Given
        final Concat concat = new Concat();

        // When
        final Object[] output = concat.transform(null);

        assertEquals(1, output.length);
        assertNull(output[0]);
    }

    @Test
    public void shouldReturnClonedConcatWithEmptyStateAndDefaultSeparator() {
        // Given
        final Concat concat = new Concat();
        concat.transform(new String[]{"1", "2", "3"});

        // When
        Concat clone = concat.statelessClone();
        final Object[] output = concat.transform(new String[]{"1", "2", "3"});

        // Then
        assertNotSame(concat, clone);
        assertArrayEquals(new String[]{"1,2,3"}, output);
    }

    @Test
    public void shouldReturnClonedConcatWithEmptyStateAndCustomSeparator() {
        // Given
        final Concat concat = new Concat();
        concat.setSeparator(" ");
        concat.transform(new String[]{"1", "2", "3"});

        // When
        Concat clone = concat.statelessClone();
        final Object[] output = concat.transform(new String[]{"1", "2", "3"});

        // Then
        assertNotSame(concat, clone);
        assertArrayEquals(new String[]{"1 2 3"}, output);
    }

    @Override
    protected Concat getInstance() {
        return new Concat();
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final String separator = "-";
        final Concat concat = new Concat();
        concat.setSeparator(separator);

        // When
        final String json = new String(new JSONSerialiser().serialise(concat, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.transform.Concat\",%n" +
                "  \"separator\" : \"-\"%n" +
                "}"), json);

        // When 2
        final Concat deserialisedConcat = new JSONSerialiser().deserialise(json.getBytes(), Concat.class);

        // Then 2
        assertNotNull(deserialisedConcat);
        assertEquals(separator, deserialisedConcat.getSeparator());
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return Concat.class;
    }
}