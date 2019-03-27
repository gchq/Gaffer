/*
 * Copyright 2016-2019 Crown Copyright
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A {@code FreqMapSerialiser} serialises and deserialises {@code FreqMap}s.
 * Any null keys or values are skipped.
 */
public class FreqMapSerialiser implements ToBytesSerialiser<FreqMap> {
    private static final long serialVersionUID = 6530929395214726384L;
    private final CompactRawLongSerialiser longSerialiser = new CompactRawLongSerialiser();

    @Override
    public byte[] serialise(final FreqMap map) throws SerialisationException {
        Set<Entry<String, Long>> entrySet = map.entrySet();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean isFirst = true;
        for (final Entry<String, Long> entry : entrySet) {
            if (null != entry.getKey() && null != entry.getValue()) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    out.write(ByteArrayEscapeUtils.DELIMITER);
                }

                try {
                    out.write(ByteArrayEscapeUtils.escape(entry.getKey().getBytes(CommonConstants.UTF_8)));
                } catch (final IOException e) {
                    throw new SerialisationException("Failed to serialise a key from a FreqMap: " + entry.getKey(), e);
                }
                out.write(ByteArrayEscapeUtils.DELIMITER);

                try {
                    out.write(ByteArrayEscapeUtils.escape(longSerialiser.serialise(entry.getValue())));
                } catch (final IOException e) {
                    throw new SerialisationException("Failed to serialise a value from a FreqMap: " + entry.getValue(), e);
                }
            }
        }

        return out.toByteArray();
    }

    @Override
    public FreqMap deserialise(final byte[] bytes) throws
            SerialisationException {
        FreqMap freqMap = new FreqMap();
        if (bytes.length == 0) {
            return freqMap;
        }

        int lastDelimiter = 0;
        String key = null;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == ByteArrayEscapeUtils.DELIMITER) {
                if (null == key) {
                    // Deserialise key
                    if (i > lastDelimiter) {
                        try {
                            key = new String(ByteArrayEscapeUtils.unEscape(bytes, lastDelimiter, i), CommonConstants.UTF_8);
                        } catch (final UnsupportedEncodingException e) {
                            throw new SerialisationException("Failed to deserialise a key from a FreqMap", e);
                        }
                    } else {
                        key = "";
                    }
                } else {
                    // Deserialise value
                    if (i > lastDelimiter) {
                        final Long value = longSerialiser.deserialise(ByteArrayEscapeUtils.unEscape(bytes, lastDelimiter, i));
                        freqMap.put(key, value);
                        key = null;
                    }
                }

                lastDelimiter = i + 1;
            }
        }

        if (null != key) {
            // Deserialise value
            if (bytes.length > lastDelimiter) {
                final Long value = longSerialiser.deserialise(ByteArrayEscapeUtils.unEscape(bytes, lastDelimiter, bytes.length));
                freqMap.put(key, value);
            }
        }

        return freqMap;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return FreqMap.class.equals(clazz);
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
    public FreqMap deserialiseEmpty() {
        return new FreqMap();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final FreqMapSerialiser serialiser = (FreqMapSerialiser) obj;

        return new EqualsBuilder()
                .append(longSerialiser, serialiser.longSerialiser)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(longSerialiser)
                .toHashCode();
    }
}
