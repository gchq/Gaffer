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

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class TypeSubTypeValueSerialiser implements Serialisation<TypeSubTypeValue> {

    private static final long serialVersionUID = 4687862916179832187L;

    @Override
    public boolean canHandle(final Class clazz) {
        return TypeSubTypeValue.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final TypeSubTypeValue typeSubTypeValue) throws SerialisationException {
        String type = typeSubTypeValue.getType();
        String subType = typeSubTypeValue.getSubType();
        String value = typeSubTypeValue.getValue();
        if ((null == type || type.isEmpty()) && (null == subType || subType.isEmpty()) && (null == value || value.isEmpty())) {
            throw new SerialisationException("TypeSubTypeValue passed to serialiser is blank");
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (type != null) {
            try {
                out.write(ByteArrayEscapeUtils.escape(type.getBytes(CommonConstants.UTF_8)));
            } catch (final IOException e) {
                throw new SerialisationException("Failed to serialise the Type from TypeSubTypeValue Object", e);
            }
        }
        out.write(ByteArrayEscapeUtils.DELIMITER);
        if (subType != null) {
            try {
                out.write(ByteArrayEscapeUtils.escape(subType.getBytes(CommonConstants.UTF_8)));
            } catch (final IOException e) {
                throw new SerialisationException("Failed to serialise the SubType from TypeSubTypeValue Object", e);
            }
        }
        out.write(ByteArrayEscapeUtils.DELIMITER);
        if (value != null) {
            try {
                out.write(ByteArrayEscapeUtils.escape(value.getBytes(CommonConstants.UTF_8)));
            } catch (final IOException e) {
                throw new SerialisationException("Failed to serialise the Value from TypeSubTypeValue Object", e);
            }
        }
        return out.toByteArray();
    }

    @Override
    public TypeSubTypeValue deserialise(final byte[] bytes) throws SerialisationException {
        int lastDelimiter = 0;
        TypeSubTypeValue typeSubTypeValue = new TypeSubTypeValue();
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (i > 0) {
                    try {
                        typeSubTypeValue.setType(new String(ByteArrayEscapeUtils.unEscape(Arrays.copyOfRange(bytes, lastDelimiter, i)), CommonConstants.UTF_8));
                    } catch (UnsupportedEncodingException e) {
                        throw new SerialisationException("Failed to deserialise the Type from TypeSubTypeValue Object", e);
                    }
                }
                lastDelimiter = i + 1;
                break;
            }
        }
        for (int i = lastDelimiter; i < bytes.length; i++) {
            if (bytes[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (i > lastDelimiter) {
                    try {
                        typeSubTypeValue.setSubType(new String(ByteArrayEscapeUtils.unEscape(Arrays.copyOfRange(bytes, lastDelimiter, i)), CommonConstants.UTF_8));
                    } catch (UnsupportedEncodingException e) {
                        throw new SerialisationException("Failed to deserialise the SubType from TypeSubTypeValue Object", e);
                    }
                }
                lastDelimiter = i + 1;
                break;
            }
        }
        if (bytes.length > lastDelimiter) {
            try {
                typeSubTypeValue.setValue(new String(ByteArrayEscapeUtils.unEscape(Arrays.copyOfRange(bytes, lastDelimiter, bytes.length)), CommonConstants.UTF_8));
            } catch (UnsupportedEncodingException e) {
                throw new SerialisationException("Failed to deserialise the Value from TypeSubTypeValue Object", e);
            }
        }
        return typeSubTypeValue;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public TypeSubTypeValue deserialiseEmptyBytes() {
        return new TypeSubTypeValue();
    }
}
