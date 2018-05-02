/*
 * Copyright 2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.serialisation.implementation;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MultiSerialiserTest extends ToBytesSerialisationTest<Object> {


    public static final String path = "multiSerialiser.json";

    @Override
    public Serialiser<Object, byte[]> getSerialisation() {
        MultiSerialiser multiSerialiser;
        try {
            multiSerialiser = JSONSerialiser.deserialise(StreamUtil.openStream(getClass(), path), MultiSerialiser.class);
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
        return multiSerialiser;
    }

    @Override
    public Pair<Object, byte[]>[] getHistoricSerialisationPairs() {
        Pair[] pairs = new Pair[]{
                new Pair("hello world", new byte[]{0, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100}),
                new Pair(420L, new byte[]{1, -114, 1, -92}),
        };

        return pairs;
    }

    @Test
    public void shouldAcceptSupportedSerialisers() throws Exception {
        MultiSerialiser multiSerialiser = new MultiSerialiser();
        multiSerialiser.setSerialisers(null);
    }

    @Test
    public void shouldMatchHistoricalSerialisation() throws IOException {
        String fromDisk;
        File file = new File(getClass().getClassLoader().getSystemResource(path).getFile());
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            StringBuilder stringBuilder = new StringBuilder();

            while (bufferedReader.ready()) {
                stringBuilder.append(bufferedReader.readLine());
                if (bufferedReader.ready()) {
                    stringBuilder.append('\n');
                }
            }

            fromDisk = stringBuilder.toString();
        } catch (Exception e) {
            throw e;
        }

        MultiSerialiser multiSerialiser = null;
        try {
            multiSerialiser = new MultiSerialiser()
                    .addSerialiser((byte) 0, new StringSerialiser(), String.class)
                    .addSerialiser((byte) 1, new CompactRawLongSerialiser(), Long.class)
                    .addSerialiser((byte) 2, new CompactRawIntegerSerialiser(), Integer.class);
        } catch (GafferCheckedException e) {
            e.printStackTrace();
        }

        String fromCode = new String(JSONSerialiser.serialise(multiSerialiser, true));

        assertEquals(fromDisk, fromCode);
    }

    @Test
    public void shouldNotAddMultiSerialiser() throws Exception {
        try {
            new MultiSerialiser().addSerialiser((byte) 0, new MultiSerialiser(), Object.class);
            fail("exception not thrown");
        } catch (GafferCheckedException e) {
            assertEquals(MultiSerialiserStorage.ERROR_ADDING_MULTI_SERIALISER, e.getMessage());
        }
    }
}
