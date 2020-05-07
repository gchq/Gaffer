/*
 * Copyright 2018-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MultiSerialiserTest extends ToBytesSerialisationTest<Object> {

    private static final String PATH = "multiSerialiser.json";

    @Test
    public void shouldAcceptSupportedSerialisers() throws Exception {
        MultiSerialiser multiSerialiser = new MultiSerialiser();
        multiSerialiser.setSerialisers(null);
    }

    @Test
    public void shouldMatchHistoricalFileSerialisation() throws IOException, GafferCheckedException {
        final String fromDisk = String.join("\n", IOUtils.readLines(StreamUtil.openStream(getClass(), PATH)));

        final MultiSerialiser multiSerialiser = new MultiSerialiser()
                .addSerialiser((byte) 0, new StringSerialiser(), String.class)
                .addSerialiser((byte) 1, new CompactRawLongSerialiser(), Long.class)
                .addSerialiser((byte) 2, new CompactRawIntegerSerialiser(), Integer.class);

        final String fromCode = new String(JSONSerialiser.serialise(multiSerialiser, true));

        assertEquals(fromDisk, fromCode);
    }

    @Test
    public void shouldNotAddMultiSerialiser() {
        final Exception exception = assertThrows(GafferCheckedException.class, () -> new MultiSerialiser().addSerialiser((byte) 0, new MultiSerialiser(), Object.class));

        assertEquals(MultiSerialiserStorage.ERROR_ADDING_MULTI_SERIALISER, exception.getMessage());
    }

    @Override
    public Pair<Object, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[] {
                new Pair<>("hello world", new byte[] {0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}),
                new Pair<>(420L, new byte[] {1, -114, 1, -92}),
        };
    }

    @Override
    public Serialiser<Object, byte[]> getSerialisation() {
        MultiSerialiser multiSerialiser;
        try {
            multiSerialiser = JSONSerialiser.deserialise(StreamUtil.openStream(getClass(), PATH), MultiSerialiser.class);
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
        return multiSerialiser;
    }

}
