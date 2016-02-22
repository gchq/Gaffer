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
package gaffer.function.simple.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gaffer.function.SimpleFilterFunction;
import gaffer.function.annotation.Inputs;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * An <code>IsIn</code> is a {@link SimpleFilterFunction} that checks that the input object is
 * in a set of allowed values.
 */
@Inputs(Object.class)
public class IsIn extends SimpleFilterFunction<Object> {
    private Set<Object> allowedValues;

    public IsIn() {
        // Required for serialisation
    }

    public IsIn(final Collection<Object> controlData) {
        this.allowedValues = new HashSet<>(controlData);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("values")
    public Object[] getAllowedValuesArray() {
        return null != allowedValues ? allowedValues.toArray() : new Object[0];
    }

    @JsonProperty("values")
    public void setAllowedValues(final Object[] allowedValuesArray) {
        if (null != allowedValuesArray) {
            allowedValues = new HashSet<>(Arrays.asList(allowedValuesArray));
        } else {
            allowedValues = new HashSet<>(0);
        }
    }

    @JsonIgnore
    public Set<Object> getAllowedValues() {
        return allowedValues;
    }

    public void setAllowedValues(final Set<Object> allowedValues) {
        this.allowedValues = allowedValues;
    }

    public IsIn statelessClone() {
        return new IsIn(allowedValues);
    }

    @Override
    protected boolean _isValid(final Object input) {
        return allowedValues.contains(input);
    }
}
