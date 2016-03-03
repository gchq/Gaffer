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

package gaffer.data.elementdefinition;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.function.ElementFilter;
import gaffer.function.AggregateFunction;
import gaffer.function.FilterFunction;
import gaffer.function.context.ConsumerFunctionContext;
import java.util.Arrays;
import java.util.List;

/**
 * A <code>Type</code> contains the an object's java class along with how to validate and aggregate the object.
 * It is used to deserialise/serialise a {@link gaffer.data.elementdefinition.schema.DataSchema} to/from JSON.
 */
public class Type {
    private Class<?> clazz;
    private ElementFilter validator;
    private AggregateFunction aggregateFunction;

    Type() {
    }

    public Type(final Class<?> clazz) {
        this.clazz = clazz;
    }

    public Type(final Class<?> clazz, final ElementFilter validator, final AggregateFunction aggregator) {
        this.clazz = clazz;
        this.validator = validator;
        this.aggregateFunction = aggregator;
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
    public ConsumerFunctionContext<ElementComponentKey, FilterFunction>[] getOriginalValidateFunctions() {
        if (null != validator) {
            final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> functions = validator.getFunctions();
            return functions.toArray(new ConsumerFunctionContext[functions.size()]);
        }

        return null;
    }

    @JsonSetter("validateFunctions")
    public void addValidateFunctions(final ConsumerFunctionContext<ElementComponentKey, FilterFunction>... functions) {
        if (null == validator) {
            validator = new ElementFilter();
        }
        validator.addFunctions(Arrays.asList(functions));
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public void setAggregateFunction(final AggregateFunction aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }
}
