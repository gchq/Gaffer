/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser;

import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.data.element.GroupedProperties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serialiser to serialise and deserialise {@link GroupedProperties} objects in
 * a byte array representation.
 */
public class GroupedPropertiesSerialiser extends PropertiesSerialiser<GroupedProperties> {
    private static final long serialVersionUID = 3307260143698122796L;
    private final StringSerialiser stringSerialiser = new StringSerialiser();

    // Required for serialisation
    GroupedPropertiesSerialiser() {
    }

    public GroupedPropertiesSerialiser(final Schema schema) {
        super(schema);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return GroupedProperties.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final GroupedProperties properties) throws SerialisationException {
        if (null == properties) {
            return new byte[0];
        }

        if (null == properties.getGroup() || properties.getGroup().isEmpty()) {
            throw new IllegalArgumentException("Group is required for serialising " + GroupedProperties.class.getSimpleName());
        }

        final SchemaElementDefinition elementDefinition = schema.getElement(properties.getGroup());
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + properties.getGroup() + ", is this group in your schema?");
        }

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            LengthValueBytesSerialiserUtil.serialise(stringSerialiser, properties.getGroup(), out);
            serialiseProperties(properties, elementDefinition, out);
            return out.toByteArray();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to serialise entity into bytes", e);
        }
    }

    @Override
    public GroupedProperties deserialise(final byte[] bytes) throws SerialisationException {
        final int[] lastDelimiter = {0};


        final String group = LengthValueBytesSerialiserUtil.deserialise(stringSerialiser, bytes, lastDelimiter);
        if (group.isEmpty()) {
            throw new IllegalArgumentException("Group is required for deserialising " + GroupedProperties.class.getSimpleName());
        }

        final SchemaElementDefinition elementDefinition = schema.getElement(group);
        if (null == elementDefinition) {
            throw new SerialisationException("No SchemaElementDefinition found for group " + group + ", is this group in your schema?");
        }

        final GroupedProperties properties = new GroupedProperties(group);
        deserialiseProperties(bytes, properties, elementDefinition, lastDelimiter);
        return properties;
    }

    @Override
    public GroupedProperties deserialiseEmpty() throws SerialisationException {
        return null;
    }

    public String getGroup(final byte[] bytes) throws SerialisationException {
        return StringUtil.toString(LengthValueBytesSerialiserUtil.deserialise(bytes, 0));
    }

    @Override
    public boolean isConsistent() {
        return false;
    }
}
