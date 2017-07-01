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

/**
 * Created on 15/05/2017.
 */
public abstract class SerialisationTest<INPUT, OUTPUT> {
    protected final Serialiser<INPUT, OUTPUT> serialiser;

    public SerialisationTest() {
        this.serialiser = getSerialisation();
    }

    @Test
    public abstract void shouldSerialiseNull() throws SerialisationException;

    @Test
    public abstract void shouldDeserialiseEmpty() throws SerialisationException;

    public abstract Serialiser<INPUT, OUTPUT> getSerialisation();
}
