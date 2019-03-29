/*
 * Copyright 2017-2019 Crown Copyright
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

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

/**
 * A {@code NullSerialiser} is a {@link ToBytesSerialiser} that always returns
 * null regardless of the input. This could be useful if properties need to be
 * removed.
 */
public class NullSerialiser implements ToBytesSerialiser<Object> {
    private static final long serialVersionUID = 282624951140644367L;

    @Override
    public boolean canHandle(final Class clazz) {
        return true;
    }

    @Override
    public byte[] serialise(final Object value) throws SerialisationException {
        return new byte[0];
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        return null;
    }

    @Override
    public Object deserialiseEmpty() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return NullSerialiser.class.getName().hashCode();
    }
}
