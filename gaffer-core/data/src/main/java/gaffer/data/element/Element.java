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

package gaffer.data.element;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import java.io.Serializable;
import java.util.Map;

/**
 * <code>Elements</code> are the fundamental building blocks of the Graph.
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
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
public abstract class Element<IdType extends ElementId> implements Serializable, Id<IdType> {
    public static final String DEFAULT_GROUP = "UNKNOWN";

    @JsonIgnore
    private IdType id;

    private Properties properties;
    private String group;

    Element(final IdType elementId) {
        this(DEFAULT_GROUP, elementId);
    }

    Element(final String group, final IdType elementId) {
        setGroup(group);
        setId(elementId);
        properties = new Properties();
    }

    public void setId(final IdType elementId) {
        this.id = elementId;
    }

    public IdType getId() {
        return id;
    }

    public void putProperty(final String name, final Object value) {
        properties.put(name, value);
    }

    public void copyProperties(final Properties properties) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            putProperty(entry.getKey(), entry.getValue());
        }
    }

    public Object getProperty(final String name) {
        return properties.get(name);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public Properties getProperties() {
        return properties;
    }

    public String getGroup() {
        return group;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 17)
                .appendSuper(id.hashCode())
                .append(group)
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
                .isEquals()
                && id.equals(element.getId());
    }

    @Override
    public boolean equals(final Object obj) {
        return null != obj
                && (obj instanceof Element)
                && equals((Element) obj);
    }

    public boolean equals(final Element element) {
        return null != element
                && shallowEquals(element)
                && getProperties().equals(element.getProperties());
    }

    @JsonIgnore
    public abstract Element emptyClone();

    @JsonIgnore
    public Object getIdentifier(final IdentifierType identifierType) {
        return id.getIdentifier(identifierType);
    }

    public void putIdentifier(final IdentifierType identifierType, final Object propertyToBeSet) {
        id.putIdentifier(identifierType, propertyToBeSet);
    }

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

    @Override
    public String toString() {
        return ", group='" + group
                + "\', properties=" + properties;
    }

    @JsonGetter("class")
    String getClassName() {
        return getClass().getName();
    }

    @JsonSetter("class")
    void setClassName(final String className) {
        // ignore the className as it will be picked up by the JsonTypeInfo annotation.
    }
}

