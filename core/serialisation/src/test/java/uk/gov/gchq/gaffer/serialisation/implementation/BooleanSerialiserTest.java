/*
 * Copyright 2017-2018 Crown Copyright
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

import org.junit.Assert;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialisationTest;

public class BooleanSerialiserTest extends ToBytesSerialisationTest<Boolean> {

    @Override
    public Serialiser<Boolean, byte[]> getSerialisation() {
        return new BooleanSerialiser();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Pair<Boolean, byte[]>[] getHistoricSerialisationPairs() {
        return new Pair[]{
                new Pair(false, new byte[]{0}),
                new Pair(true, new byte[]{1})
        };
    }

    @Override
    public void shouldDeserialiseEmpty() throws SerialisationException {
        Assert.assertFalse(serialiser.deserialiseEmpty());
    }
}
