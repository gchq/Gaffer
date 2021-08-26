/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation.tostring;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.SerialisationTest;
import uk.gov.gchq.gaffer.serialisation.Serialiser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StringToStringSerialiserTest extends SerialisationTest<String, String> {

    public static final String STRING_VALUE_1 = "StringValue1";

    @Test
    public void shouldSerialiseAndDeserialise() throws Exception {
        final String serialised = serialiser.serialise(STRING_VALUE_1);
        final String deserialise = serialiser.deserialise(serialised);
        assertEquals(STRING_VALUE_1, serialised);
        assertEquals(STRING_VALUE_1, deserialise);
        assertEquals(serialised, deserialise);
    }


    @Test
    @Override
    public void shouldSerialiseNull() throws SerialisationException {
        assertNull(serialiser.serialiseNull());
    }

    @Test
    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        assertNull(serialiser.serialiseNull());
    }

    @Override
    public Serialiser<String, String> getSerialisation() {
        return new StringToStringSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<String, String>[] getHistoricSerialisationPairs() {
        String s = "this is a string to be used for checking the serialisation.";
        return new Pair[]{
                new Pair<>(s, s)};
    }
}
