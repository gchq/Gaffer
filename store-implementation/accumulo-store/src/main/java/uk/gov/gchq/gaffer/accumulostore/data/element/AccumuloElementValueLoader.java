/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.data.element;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.data.element.ElementValueLoader;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

public abstract class AccumuloElementValueLoader implements ElementValueLoader {
    private static final long serialVersionUID = 3874766099103158427L;

    protected final AccumuloElementConverter elementConverter;
    protected final Key key;

    private final Schema schema;
    private final String group;
    private final Value value;

    private SchemaElementDefinition eDef;

    protected AccumuloElementValueLoader(final String group,
                                         final Key key,
                                         final Value value,
                                         final AccumuloElementConverter elementConverter,
                                         final Schema schema) {
        this.group = group;
        this.key = key;
        this.value = value;
        this.elementConverter = elementConverter;
        this.schema = schema;
    }

    @Override
    public Object getProperty(final String name, final Properties lazyProperties) {
        if (null == eDef) {
            eDef = schema.getElement(group);
            if (null == eDef) {
                throw new IllegalArgumentException("Element definition for " + group + " could not be found in the schema");
            }
        }

        final Properties props;
        if (eDef.getGroupBy().contains(name)) {
            props = elementConverter.getPropertiesFromColumnQualifier(group, key.getColumnQualifierData().getBackingArray());
        } else if (name.equals(schema.getVisibilityProperty())) {
            props = elementConverter.getPropertiesFromColumnVisibility(group, key.getColumnVisibilityData().getBackingArray());
        } else if (name.equals(schema.getTimestampProperty())) {
            props = elementConverter.getPropertiesFromTimestamp(group, key.getTimestamp());
        } else {
            props = elementConverter.getPropertiesFromValue(group, value);
        }
        lazyProperties.putAll(props);
        return props.get(name);
    }
}
