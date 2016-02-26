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

package gaffer.store.schema;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.function.AggregateFunction;
import gaffer.serialisation.Serialisation;
import gaffer.serialisation.implementation.JavaSerialiser;

/**
 * A <code>Type</code> contains the an object's java class along with how to validate and aggregate the object.
 * It is used to deserialise/serialise a {@link gaffer.store.schema.DataSchema} to/from JSON.
 */
public class Type {
    private static final Serialisation DEFAULT_SERIALISER = new JavaSerialiser();

    private Class<?> clazz;
    private ElementFilter validator;
    private Serialisation serialiser = DEFAULT_SERIALISER;
    private String position;
    private AggregateFunction aggregateFunction;

    Type() {
    }

    public Type(final Class<?> clazz) {
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

    public ElementFilter getValidator() {
        return validator;
    }

    public void setValidator(final ElementFilter validator) {
        this.validator = validator;
    }

    /**
     * @return the {@link gaffer.serialisation.Serialisation} for the property. If one has not been explicitly set then
     * it will default to a {@link gaffer.serialisation.implementation.JavaSerialiser}.
     */
    @JsonIgnore
    public Serialisation getSerialiser() {
        return serialiser;
    }

    /**
     * @param serialiser the {@link gaffer.serialisation.Serialisation} for the property. If null then
     *                   a {@link gaffer.serialisation.implementation.JavaSerialiser} will be set instead.
     */
    public void setSerialiser(final Serialisation serialiser) {
        if (null == serialiser) {
            this.serialiser = DEFAULT_SERIALISER;
        } else {
            this.serialiser = serialiser;
        }
    }

    public String getSerialiserClass() {
        final Class<? extends Serialisation> serialiserClass = serialiser.getClass();
        if (!DEFAULT_SERIALISER.getClass().equals(serialiserClass)) {
            return serialiserClass.getName();
        }

        return null;
    }

    public void setSerialiserClass(final String clazz) {
        if (null == clazz) {
            this.serialiser = DEFAULT_SERIALISER;
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

    /**
     * @return the position to store the property. This can be interpreted differently by different
     * {@link gaffer.store.Store} implementations. For example it could refer to the column to store the property in.
     */
    public String getPosition() {
        return position;
    }

    /**
     * @param position the position to store the property. This can be interpreted differently by different
     *                 {@link gaffer.store.Store} implementations. For example it could refer to the column to store the property in.
     */
    public void setPosition(final String position) {
        this.position = position;
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public void setAggregateFunction(final AggregateFunction aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    public String toString() {
        return "Type{"
                + "clazz=" + clazz
                + ", position='" + position + '\''
                + ", validator=" + validator
                + ", aggregateFunction=" + aggregateFunction
                + ", serialiser=" + serialiser
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Type type = (Type) o;

        if (!getClazz().equals(type.getClazz())) {
            return false;
        }
        if (getValidator() != null ? !getValidator().equals(type.getValidator()) : type.getValidator() != null) {
            return false;
        }
        if (getSerialiser() != null ? !getSerialiser().equals(type.getSerialiser()) : type.getSerialiser() != null) {
            return false;
        }
        if (getPosition() != null ? !getPosition().equals(type.getPosition()) : type.getPosition() != null) {
            return false;
        }

        return !(getAggregateFunction() != null ? !getAggregateFunction().equals(type.getAggregateFunction()) : type.getAggregateFunction() != null);
    }

    @Override
    public int hashCode() {
        int result = getClazz().hashCode();
        result = 31 * result + (getValidator() != null ? getValidator().hashCode() : 0);
        result = 31 * result + (getSerialiser() != null ? getSerialiser().hashCode() : 0);
        result = 31 * result + (getPosition() != null ? getPosition().hashCode() : 0);
        result = 31 * result + (getAggregateFunction() != null ? getAggregateFunction().hashCode() : 0);
        return result;
    }

    public static class Builder {
        private Type type = new Type();

        public Builder(final Class<?> clazz) {
            type.setClazz(clazz);
        }

        public Builder clazz(final Class clazz) {
            type.setClazz(clazz);
            return this;
        }

        public Builder position(final String position) {
            type.setPosition(position);
            return this;
        }

        public Builder serialiser(final Serialisation serialiser) {
            type.setSerialiser(serialiser);
            return this;
        }

        public Builder validator(final ElementFilter validator) {
            type.setValidator(validator);
            return this;
        }

        public Builder aggregateFunction(final AggregateFunction aggregateFunction) {
            type.setAggregateFunction(aggregateFunction);
            return this;
        }

        public Type build() {
            if (null == type.getClazz()) {
                throw new IllegalStateException("Type requires a class");
            }
            return type;
        }
    }
}
