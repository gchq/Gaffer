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

package uk.gov.gchq.gaffer.data.element;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.id.ElementId;

import java.util.Map.Entry;

/**
 * {@code Elements} are the fundamental building blocks of the Graph.
 * Elements should have identifier(s), an group and an optional collection of properties.
 * Elements are designed so that multiple elements can share the same identifier(s) but are distinguished via their
 * group.
 * <ul>
 * <li>An group is a way of categorising the element and grouping elements together that share the same set of
 * properties.</li>
 * <li>The identifier(s) along with the group should uniquely identify an element.</li>
 * <li>The properties are any other properties of the element. Properties should be split out as much as possible into
 * simple properties, enabling validation, aggregation, transformation and filtering to be done more precisely.</li>
 * </ul>
 * <p>
 * Equals has been overridden to check groups are equal. NOTE - it does not compare property values.
 */
public abstract class Element implements ElementId {
    public static final String DEFAULT_GROUP = "UNKNOWN";

    private Properties properties;
    private String group;

    Element() {
        this(DEFAULT_GROUP);
    }

    Element(final String group) {
        this(group, new Properties());
    }

    Element(final String group, final Properties properties) {
        this.group = group;
        if (null == properties) {
            this.properties = new Properties();
        } else {
            this.properties = properties;
        }
    }

    public void putProperty(final String name, final Object value) {
        properties.put(name, value);
    }

    public void copyProperties(final Properties properties) {
        if (null != properties) {
            for (final Entry<String, Object> entry : properties.entrySet()) {
                putProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    public Object getProperty(final String name) {
        return properties.get(name);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Properties getProperties() {
        return properties;
    }

    public Object removeProperty(final String propName) {
        return properties.remove(propName);
    }

    public String getGroup() {
        return group;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(53, 17)
                .append(group)
                .append(properties)
                .toHashCode();
    }

    public boolean shallowEquals(final Object obj) {
        return null != obj
                && (obj instanceof Element)
                && shallowEquals((Element) obj);
    }

    public boolean shallowEquals(final Element element) {
        return null != element
                && new EqualsBuilder()
                .append(group, element.getGroup())
                .isEquals();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final Element element = (Element) obj;

        return new EqualsBuilder()
                .append(group, element.group)
                .append(properties, element.properties)
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("group", group)
                .append("properties", properties)
                .build();
    }

    @JsonIgnore
    public abstract Element emptyClone();

    public Element shallowClone() {
        final Element element = emptyClone();
        element.setProperties(getProperties().clone());
        return element;
    }

    @JsonIgnore
    public abstract Object getIdentifier(final IdentifierType identifierType);

    abstract void putIdentifier(final IdentifierType identifierType, final Object value);

    @JsonIgnore
    public Element getElement() {
        return this;
    }

    /**
     * Setter for group. Used for the JSON deserialisation.
     *
     * @param group the group.
     */
    void setGroup(final String group) {
        this.group = group;
    }

    /**
     * Setter for properties. Used for the JSON deserialisation.
     *
     * @param properties the element properties.
     */
    void setProperties(final Properties properties) {
        this.properties = properties;
    }
}

