/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.io.IOException;

/**
 * A {@code HyperLogLogPlusSerialiser} is used to serialise and deserialise
 * {@link HyperLogLogPlus} objects.
 */
@Deprecated
public class HyperLogLogPlusSerialiser implements ToBytesSerialiser<HyperLogLogPlus> {
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
    public HyperLogLogPlus deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return HyperLogLogPlusSerialiser.class.getName().hashCode();
    }
}
