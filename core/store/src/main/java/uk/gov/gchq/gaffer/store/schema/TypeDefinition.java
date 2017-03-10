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

package uk.gov.gchq.gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

/**
 * A <code>TypeDefinition</code> contains the an object's java class along with how to validate and aggregate the object.
 * It is used to deserialise/serialise a {@link Schema} to/from JSON.
 */
@JsonFilter(JSONSerialiser.FILTER_FIELDS_BY_NAME)
public class TypeDefinition {
    private Class<?> clazz;
    private Serialisation serialiser;
    private List<Predicate> predicates;
    private BinaryOperator aggregateFunction;
    private String description;

    public TypeDefinition() {
    }

    public TypeDefinition(final Class<?> clazz) {
        this.clazz = clazz;
    }

    @JsonIgnore
    public Class<?> getClazz() {
        return clazz;
    }

    public void setClazz(final Class<?> clazz) {
        this.clazz = clazz;
    }

    @JsonGetter("class")
    public String getClassString() {
        return null != clazz ? clazz.getName() : null;
    }

    @JsonSetter("class")
    public void setClassString(final String classType) throws ClassNotFoundException {
        this.clazz = null != classType ? Class.forName(classType) : null;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public List<Predicate> getPredicates() {
        return predicates;
    }

    public void setPredicates(final List<Predicate> predicates) {
        this.predicates = predicates;
    }

    /**
     * @return the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} for the property.
     */
    @JsonIgnore
    public Serialisation getSerialiser() {
        return serialiser;
    }

    /**
     * @param serialiser the {@link uk.gov.gchq.gaffer.serialisation.Serialisation} for the property.
     */
    public void setSerialiser(final Serialisation serialiser) {
        this.serialiser = serialiser;
    }

    public String getSerialiserClass() {
        if (null == serialiser) {
            return null;
        }

        return serialiser.getClass().getName();
    }

    public void setSerialiserClass(final String clazz) {
        if (null == clazz) {
            this.serialiser = null;
        } else {
            final Class<? extends Serialisation> serialiserClass;
            try {
                serialiserClass = Class.forName(clazz).asSubclass(Serialisation.class);
            } catch (ClassNotFoundException e) {
                throw new SchemaException(e.getMessage(), e);
            }
            try {
                this.serialiser = serialiserClass.newInstance();
            } catch (IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
                throw new SchemaException(e.getMessage(), e);
            }
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public BinaryOperator getAggregateFunction() {
        return aggregateFunction;
    }

    public void setAggregateFunction(final BinaryOperator aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public void merge(final TypeDefinition type) {
        if (null == clazz) {
            clazz = type.getClazz();
        } else if (null != type.getClazz() && !clazz.equals(type.getClazz())) {
            throw new SchemaException("Unable to merge schemas. Conflict with type class, options are: "
                    + clazz.getName() + " and " + type.getClazz().getName());
        }

        if (null != type.getSerialiser()) {
            if (null == getSerialiser()) {
                setSerialiser(type.getSerialiser());
            } else if (!getSerialiser().getClass().equals(type.getSerialiser().getClass())) {
                throw new SchemaException("Unable to merge schemas. Conflict with type (" + clazz + ") serialiser, options are: "
                        + getSerialiser().getClass().getName() + " and " + type.getSerialiser().getClass().getName());
            }
        }

        if (null == predicates) {
            predicates = type.getPredicates();
        } else if (null != type.getPredicates()) {
            predicates.addAll(type.getPredicates());
        }

        if (null == aggregateFunction) {
            aggregateFunction = type.getAggregateFunction();
        } else if (null != type.getAggregateFunction() && !aggregateFunction.equals(type.getAggregateFunction())) {
            throw new SchemaException("Unable to merge schemas. Conflict with type (" + clazz + ") aggregate function, options are: "
                    + aggregateFunction + " and " + type.getAggregateFunction());
        }

        if (null == description) {
            description = type.getDescription();
        } else if (null != type.getDescription() && !description.contains(type.getDescription())) {
            description = description + " | " + type.getDescription();
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("clazz", clazz)
                .append("predicates", predicates)
                .append("serialiser", serialiser)
                .append("aggregateFunction", aggregateFunction)
                .append("description", description)
                .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final TypeDefinition that = (TypeDefinition) o;

        return new EqualsBuilder()
                .append(clazz, that.clazz)
                .append(predicates, that.predicates)
                .append(serialiser, that.serialiser)
                .append(aggregateFunction, that.aggregateFunction)
                .append(description, that.description)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(clazz)
                .append(predicates)
                .append(serialiser)
                .append(aggregateFunction)
                .append(description)
                .toHashCode();
    }

    public static class Builder {
        private TypeDefinition type = new TypeDefinition();

        public Builder() {
        }

        public Builder clazz(final Class clazz) {
            type.setClazz(clazz);
            return this;
        }

        public Builder serialiser(final Serialisation serialiser) {
            type.setSerialiser(serialiser);
            return this;
        }

        public Builder predicates(final List<Predicate> predicates) {
            type.setPredicates(predicates);
            return this;
        }

        public Builder predicates(final Predicate... predicates) {
            return predicates(Lists.newArrayList(predicates));
        }

        public Builder aggregateFunction(final BinaryOperator aggregateFunction) {
            type.setAggregateFunction(aggregateFunction);
            return this;
        }

        public Builder description(final String description) {
            type.setDescription(description);
            return this;
        }

        public TypeDefinition build() {
            return type;
        }
    }
}
