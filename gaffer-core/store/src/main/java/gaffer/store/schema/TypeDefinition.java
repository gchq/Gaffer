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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.function.AggregateFunction;
import gaffer.function.FilterFunction;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.serialisation.Serialisation;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>TypeDefinition</code> contains the an object's java class along with how to validate and aggregate the object.
 * It is used to deserialise/serialise a {@link Schema} to/from JSON.
 */
public class TypeDefinition {
    private Class<?> clazz;
    private Serialisation serialiser;
    private ElementFilter validator;
    private AggregateFunction aggregateFunction;

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

    @JsonIgnore
    public ElementFilter getValidator() {
        return validator;
    }

    @JsonSetter("validator")
    public void setValidator(final ElementFilter validator) {
        this.validator = validator;
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null is only returned when the validator is null")
    @JsonGetter("validateFunctions")
    public ConsumerFunctionContext<String, FilterFunction>[] getOriginalValidateFunctions() {
        if (null != validator) {
            final List<ConsumerFunctionContext<String, FilterFunction>> functions = validator.getFunctions();
            return functions.toArray(new ConsumerFunctionContext[functions.size()]);
        }

        return null;
    }

    @JsonSetter("validateFunctions")
    public void addValidateFunctions(final ConsumerFunctionContext<String, FilterFunction>... functions) {
        if (null == validator) {
            validator = new ElementFilter();
        }
        validator.addFunctions(Arrays.asList(functions));
    }

    /**
     * @return the {@link gaffer.serialisation.Serialisation} for the property.
     */
    @JsonIgnore
    public Serialisation getSerialiser() {
        return serialiser;
    }

    /**
     * @param serialiser the {@link gaffer.serialisation.Serialisation} for the property.
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

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public void setAggregateFunction(final AggregateFunction aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
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

        if (null == validator) {
            validator = type.getValidator();
        } else if (null != type.getValidator() && null != type.getValidator().getFunctions()) {
            validator.addFunctions(type.getValidator().getFunctions());
        }

        if (null == aggregateFunction) {
            aggregateFunction = type.getAggregateFunction();
        } else if (null != type.getAggregateFunction() && !aggregateFunction.equals(type.getAggregateFunction())) {
            throw new SchemaException("Unable to merge schemas. Conflict with type (" + clazz + ") aggregate function, options are: "
                    + aggregateFunction + " and " + type.getAggregateFunction());
        }
    }

    @Override
    public String toString() {
        return "TypeDefinition{"
                + "clazz=" + clazz
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

        final TypeDefinition type = (TypeDefinition) o;

        if (!getClazz().equals(type.getClazz())) {
            return false;
        }
        if (getValidator() != null ? !getValidator().equals(type.getValidator()) : type.getValidator() != null) {
            return false;
        }
        if (getSerialiser() != null ? !getSerialiser().equals(type.getSerialiser()) : type.getSerialiser() != null) {
            return false;
        }

        return !(getAggregateFunction() != null ? !getAggregateFunction().equals(type.getAggregateFunction()) : type.getAggregateFunction() != null);
    }

    @Override
    public int hashCode() {
        int result = getClazz().hashCode();
        result = 31 * result + (getValidator() != null ? getValidator().hashCode() : 0);
        result = 31 * result + (getSerialiser() != null ? getSerialiser().hashCode() : 0);
        result = 31 * result + (getAggregateFunction() != null ? getAggregateFunction().hashCode() : 0);
        return result;
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

        public Builder validator(final ElementFilter validator) {
            type.setValidator(validator);
            return this;
        }

        public Builder aggregateFunction(final AggregateFunction aggregateFunction) {
            type.setAggregateFunction(aggregateFunction);
            return this;
        }

        public TypeDefinition build() {
            return type;
        }
    }
}
