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
package uk.gov.gchq.gaffer.function;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import java.util.Map;

/**
 * An <code>MapFilter</code> is a {@link SimpleFilterFunction} that extracts a
 * value from a map using the provided key and passes the value to a provided
 * {@link FilterFunction}.
 */
@Inputs(Map.class)
public class MapFilter extends SimpleFilterFunction<Map> {
    private FilterFunction function;
    private Object key;

    public MapFilter() {
    }

    public MapFilter(final Object key, final FilterFunction function) {
        this.function = function;
        this.key = key;
    }

    @Override
    protected boolean isValid(final Map map) {
        if (null == function) {
            return true;
        }

        if (null == map) {
            return false;
        }

        // If the function is a simple filter function we can avoid having to wrap
        // the map value in an object array.
        if (function instanceof SimpleFilterFunction) {
            try {
                return ((SimpleFilterFunction) function).isValid(map.get(key));
            } catch (final ClassCastException e) {
                throw new IllegalArgumentException("Input does not match parametrised type", e);
            }
        }

        return function.isValid(new Object[]{map.get(key)});
    }

    @Override
    public MapFilter statelessClone() {
        return new MapFilter(key, function.statelessClone());
    }

    public FilterFunction getFunction() {
        return function;
    }

    public void setFunction(final FilterFunction function) {
        this.function = function;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public Object getKey() {
        return key;
    }

    public void setKey(final Object key) {
        this.key = key;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MapFilter mapFilter = (MapFilter) o;

        return new EqualsBuilder()
                .append(inputs, mapFilter.inputs)
                .append(function, mapFilter.function)
                .append(key, mapFilter.key)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(function)
                .append(key)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("function", function)
                .append("key", key)
                .toString();
    }
}
