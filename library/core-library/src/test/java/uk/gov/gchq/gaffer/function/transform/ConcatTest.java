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
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.bifunction.BiFunctionTest;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ConcatTest extends BiFunctionTest {
    @Test
    public void shouldConcatStringsWithDefaultSeparator() {
        // Given
        final Concat concat = new Concat();

        // When
        String output = concat.apply("1", "2");

        assertEquals("1,2", output);
    }

    @Test
    public void shouldConcatStringsWithGivenSeparator() {
        // Given
        final Concat concat = new Concat();
        concat.setSeparator(" ");

        // When
        final String output = concat.apply("1", "2");

        assertEquals("1 2", output);
    }

    @Test
    public void shouldConvertNullValuesToEmptyStringWhenConcatenating() {
        // Given
        final Concat concat = new Concat();

        // When
        final String output = concat.apply("1", null);

        assertEquals("1", output);
    }

    @Test
    public void shouldReturnNullForNullInput() {
        // Given
        final Concat concat = new Concat();

        // When
        final String output = concat.apply(null, null);

        assertNull(output);
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
    protected Class<Concat> getFunctionClass() {
        return Concat.class;
    }
}