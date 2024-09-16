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

package uk.gov.gchq.gaffer.serialisation;

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * A {@code TypeValueSerialiser} is used to serialise and deserialise {@link TypeValue}
 * instances.
 */
public class TypeValueSerialiser implements ToBytesSerialiser<TypeValue> {

    private static final long serialVersionUID = 8675867261911636738L;

    @Override
    public boolean canHandle(final Class clazz) {
        return TypeValue.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final TypeValue typeValue) throws SerialisationException {
        String type = typeValue.getType();
        String value = typeValue.getValue();
        if ((null == type || type.isEmpty()) && (null == value || value.isEmpty())) {
            throw new SerialisationException("TypeValue passed to serialiser is blank");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (null != type) {
            try {
                out.write(ByteArrayEscapeUtils.escape(type.getBytes(StandardCharsets.UTF_8)));
            } catch (final IOException e) {
                throw new SerialisationException("Failed to serialise the Type from TypeValue Object", e);
            }
        }
        out.write(ByteArrayEscapeUtils.DELIMITER);
        if (null != value) {
            try {
                out.write(ByteArrayEscapeUtils.escape(value.getBytes(StandardCharsets.UTF_8)));
            } catch (final IOException e) {
                throw new SerialisationException("Failed to serialise the Value from TypeValue Object", e);
            }
        }
        return out.toByteArray();
    }

    @Override
    public TypeValue deserialise(final byte[] bytes) throws SerialisationException {
        int lastDelimiter = 0;
        TypeValue typeValue = new TypeValue();
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (i > 0) {
                    typeValue.setType(new String(ByteArrayEscapeUtils.unEscape(bytes, lastDelimiter, i), StandardCharsets.UTF_8));
                }
                lastDelimiter = i + 1;
                break;
            }
        }
        if (bytes.length > lastDelimiter) {
            typeValue.setValue(new String(ByteArrayEscapeUtils.unEscape(bytes, lastDelimiter, bytes.length), StandardCharsets.UTF_8));
        }
        return typeValue;
    }

    @Override
    public TypeValue deserialiseEmpty() throws SerialisationException {
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
        return TypeValueSerialiser.class.getName().hashCode();
    }
}
