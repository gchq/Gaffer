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

package uk.gov.gchq.gaffer.serialisation;

import org.junit.Test;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertArrayEquals;

public abstract class SerialisationTest<T> {

    protected final Serialisation<T> serialiser;

    public SerialisationTest() {
        this.serialiser = getSerialisation();
    }

    @Test
    public void shouldSerialiseNull() throws SerialisationException {
        // When
        final byte[] bytes = serialiser.serialiseNull();

        // Then
        assertArrayEquals(new byte[0], bytes);
    }

    @Test
    public abstract void shouldDeserialiseEmptyBytes() throws SerialisationException;

    public abstract Serialisation<T> getSerialisation();
}
