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
package uk.gov.gchq.gaffer.function.filter;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.FilterFunctionTest;
import uk.gov.gchq.gaffer.function.Function;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class MultiRegexTest extends FilterFunctionTest {

    @Test
    public void shouldAccepValidValue() {
        // Given
        Pattern[] patterns = new Pattern[2];
        patterns[0] = Pattern.compile("fail");
        patterns[1] = Pattern.compile("pass");
        final MultiRegex filter = new MultiRegex(patterns);

        // When
        boolean accepted = filter.isValid("pass");

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldRejectInvalidValue() {
        // Given
        Pattern[] patterns = new Pattern[2];
        patterns[0] = Pattern.compile("fail");
        patterns[1] = Pattern.compile("reallyFail");
        final MultiRegex filter = new MultiRegex(patterns);

        // When
        boolean accepted = filter.isValid("pass");

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final MultiRegex filter = new MultiRegex();

        // When
        final MultiRegex clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        Pattern[] patterns = new Pattern[2];
        patterns[0] = Pattern.compile("test");
        patterns[1] = Pattern.compile("test2");
        final MultiRegex filter = new MultiRegex(patterns);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        JsonUtil.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.function.filter.MultiRegex\",%n" +
                "  \"value\" : [ {%n" +
                "    \"java.util.regex.Pattern\" : \"test\"%n" +
                "  }, {%n" +
                "    \"java.util.regex.Pattern\" : \"test2\"%n" +
                "  } ]%n" +
                "}"), json);

        // When 2
        final MultiRegex deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), MultiRegex.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(patterns[0].pattern(), deserialisedFilter.getPatterns()[0].pattern());
        assertEquals(patterns[1].pattern(), deserialisedFilter.getPatterns()[1].pattern());
    }

    @Override
    protected FilterFunction getInstance() {
        Pattern[] patterns = new Pattern[2];
        patterns[0] = Pattern.compile("NOTHING");
        patterns[1] = Pattern.compile("[t,T].*[t,T]");
        MultiRegex multi = new MultiRegex(patterns);
        return multi;
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return MultiRegex.class;
    }
}