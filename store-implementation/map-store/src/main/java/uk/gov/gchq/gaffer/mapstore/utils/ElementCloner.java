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
package uk.gov.gchq.gaffer.mapstore.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

/**
 * Utility class to create a clone of an {@link Element}.
 */
public class ElementCloner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElementCloner.class);

    /**
     * Clone an {@link Element}, based on a target {@link Schema}.
     *
     * @param element the element to clone
     * @param schema the schema
     * @return the cloned element
     */
    public Element cloneElement(final Element element, final Schema schema) {
        try {
            final Element clone = element.emptyClone();
            final SchemaElementDefinition sed = schema.getElement(clone.getGroup());
            for (final String propertyName : element.getProperties().keySet()) {
                final Object property = element.getProperty(propertyName);
                if (null == sed.getPropertyTypeDef(propertyName) || null == sed.getPropertyTypeDef(propertyName).getSerialiser()) {
                    // This can happen if transient properties are derived - they will not have serialisers.
                    LOGGER.warn("Can't find Serialisation for {}, returning uncloned property", propertyName);
                    clone.putProperty(propertyName, property);
                } else if (null != property) {
                    final Serialiser serialiser = sed.getPropertyTypeDef(propertyName).getSerialiser();
                    clone.putProperty(propertyName, serialiser.deserialise(serialiser.serialise(property)));
                } else {
                    clone.putProperty(propertyName, null);
                }
            }
            return clone;
        } catch (final SerialisationException e) {
            throw new RuntimeException("SerialisationException converting elements", e);
        }
    }
}
