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
package uk.gov.gchq.gaffer.sketches.serialisation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.io.IOException;

public class HyperLogLogPlusSerialiser implements Serialisation<HyperLogLogPlus> {
    private static final long serialVersionUID = 2782098698280905174L;

    @Override
    public boolean canHandle(final Class clazz) {
        return HyperLogLogPlus.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final HyperLogLogPlus value) throws SerialisationException {
        try {
            return value.getBytes();
        } catch (final IOException e) {
            throw new RuntimeException("Failed to get bytes from HyperLogLogPlus sketch", e);
        }
    }

    @Override
    public HyperLogLogPlus deserialise(final byte[] bytes) throws SerialisationException {
        try {
            return HyperLogLogPlus.Builder.build(bytes);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create HyperLogLogPlus sketch from given bytes", e);
        }
    }

    @Override
    public HyperLogLogPlus deserialiseEmptyBytes() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
