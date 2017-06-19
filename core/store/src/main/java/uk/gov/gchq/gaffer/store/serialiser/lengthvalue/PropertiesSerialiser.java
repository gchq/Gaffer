/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser.lengthvalue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public abstract class PropertiesSerialiser {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesSerialiser.class);
    protected static final byte[] EMPTY_BYTES = new byte[0];
    protected Schema schema;

    // Required for serialisation
    PropertiesSerialiser() {
    }

    protected PropertiesSerialiser(final Schema schema) {
        updateSchema(schema);
    }

    public void updateSchema(final Schema schema) {
        this.schema = schema;
    }

    public boolean canHandle(final Class clazz) {
        return Properties.class.isAssignableFrom(clazz);
    }

    public boolean preservesObjectOrdering() {
        return false;
    }

    protected byte[] serialiseProperties(final Properties properties, final String group) throws SerialisationException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        try {
            LengthValueBytesSerialiserUtil.serialise(StringUtil.toBytes(group), out);
        } catch (IOException e) {
            throw new SerialisationException("Failed to write serialise entity vertex to ByteArrayOutputStream", e);
        }

        serialiseProperties(properties, elementDefinition, out);
        return out.toByteArray();
    }

    protected void serialiseProperties(final Properties properties, final SchemaElementDefinition elementDefinition, final ByteArrayOutputStream out) throws SerialisationException {
        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        String propertyName;
        while (propertyNames.hasNext()) {
            propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser serialiser = (typeDefinition != null) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
            try {
                if (null != serialiser) {
                    Object value = properties.get(propertyName);
                    if (null != value) {
                        final byte[] bytes = serialiser.serialise(value);
                        LengthValueBytesSerialiserUtil.serialise(bytes, out);
                    } else {
                        final byte[] bytes = serialiser.serialiseNull();
                        LengthValueBytesSerialiserUtil.serialise(bytes, out);
                    }
                } else {
                    LengthValueBytesSerialiserUtil.serialise(EMPTY_BYTES, out);
                }
            } catch (final IOException e) {
                throw new SerialisationException("Failed to write serialise property to ByteArrayOutputStream" + propertyName, e);
            }
        }
    }

    protected Properties deserialiseProperties(final byte[] bytes) throws SerialisationException {
        int lastDelimiter = 0;

        final byte[] groupBytes = LengthValueBytesSerialiserUtil.deserialise(bytes, lastDelimiter);
        final String group = StringUtil.toString(groupBytes);
        lastDelimiter = LengthValueBytesSerialiserUtil.getLastDelimiter(bytes, groupBytes, lastDelimiter);

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema or do your table iterators need updating?");
        }

        final Properties properties = new Properties();
        deserialiseProperties(bytes, properties, elementDefinition, lastDelimiter);
        return properties;
    }

    protected void deserialiseProperties(final byte[] bytes, final Properties properties, final SchemaElementDefinition elementDefinition, final int lastDelimiterInitial) throws SerialisationException {
        int lastDelimiter = lastDelimiterInitial;
        final int arrayLength = bytes.length;
        long currentPropLength;

        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        while (propertyNames.hasNext() && lastDelimiter < arrayLength) {
            final String propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser serialiser = (typeDefinition != null) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
            if (null != serialiser) {
                final int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[lastDelimiter]);
                final byte[] length = new byte[numBytesForLength];
                System.arraycopy(bytes, lastDelimiter, length, 0, numBytesForLength);
                try {
                    currentPropLength = CompactRawSerialisationUtils.readLong(length);
                } catch (final SerialisationException e) {
                    throw new SerialisationException("Exception reading length of property");
                }
                lastDelimiter += numBytesForLength;
                if (currentPropLength > 0) {
                    try {
                        properties.put(propertyName, serialiser.deserialise(Arrays.copyOfRange(bytes, lastDelimiter, lastDelimiter += currentPropLength)));
                    } catch (final SerialisationException e) {
                        throw new SerialisationException("Failed to deserialise property " + propertyName, e);
                    }
                } else {
                    try {
                        properties.put(propertyName, serialiser.deserialiseEmpty());
                    } catch (final SerialisationException e) {
                        throw new SerialisationException("Failed to deserialise property " + propertyName, e);
                    }
                }
            } else {
                LOGGER.warn("No serialiser found in schema for property {}", propertyName);
            }
        }
    }
}

