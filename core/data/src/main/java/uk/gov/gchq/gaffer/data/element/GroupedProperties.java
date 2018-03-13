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
package uk.gov.gchq.gaffer.data.element;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * GroupedProperties are just {@link Properties} with the Element group attached.
 * This allows the property serialisers to be looked up in the schema according to the
 * element group. So, this class can be used in place of the normal Properties
 * class when serialising and deserialising is required.
 * See GroupedPropertiesSerialiser in the store module.
 */
public class GroupedProperties extends Properties {
    private static final long serialVersionUID = -3424853199115841290L;
    private String group;

    public GroupedProperties() {
    }

    public GroupedProperties(final String group) {
        this.group = group;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final GroupedProperties properties = (GroupedProperties) obj;

        return new EqualsBuilder()
                .append(group, properties.getGroup())
                .appendSuper(super.equals(properties))
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(23, 5)
                .append(group)
                .appendSuper(super.hashCode())
                .toHashCode();
    }
}
