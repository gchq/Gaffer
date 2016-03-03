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

import gaffer.function.SimpleFilterFunction;
import gaffer.function.annotation.Inputs;

import java.util.Collection;
import java.util.Map;

/**
 * An <code>IsShorterThan</code> is a {@link SimpleFilterFunction} that checks that the input
 * object has a length less than a maximum length. There is also an orEqualTo flag that can be set to allow
 * the input object length to be less than or equal to the maximum length.
 * <p>
 * Allowed object types are {@link java.lang.String}s, arrays, {@link java.util.Collection}s and {@link java.util.Map}s.
 * Additional object types can easily be added by modifying the getLength(Object) method.
 */
@Inputs(Object.class)
public class IsShorterThan extends SimpleFilterFunction<Object> {
    private int maxLength;
    private boolean orEqualTo;

    // Default constructor for serialisation
    public IsShorterThan() {
    }

    public IsShorterThan(final int maxLength) {
        this.maxLength = maxLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(final int maxLength) {
        this.maxLength = maxLength;
    }

    public boolean isOrEqualTo() {
        return orEqualTo;
    }

    public void setOrEqualTo(final boolean orEqualTo) {
        this.orEqualTo = orEqualTo;
    }

    public IsShorterThan statelessClone() {
        IsShorterThan clone = new IsShorterThan(maxLength);
        clone.setOrEqualTo(orEqualTo);

        return clone;
    }

    @Override
    protected boolean _isValid(final Object input) {
        if (null == input) {
            return true;
        }

        if (orEqualTo) {
            return getLength(input) <= maxLength;
        } else {
            return getLength(input) < maxLength;
        }
    }

    private int getLength(final Object value) {
        final int length;
        if (value instanceof String) {
            length = ((String) value).length();
        } else if (value instanceof Object[]) {
            length = ((Object[]) value).length;
        } else if (value instanceof Collection) {
            length = ((Collection) value).size();
        } else if (value instanceof Map) {
            length = ((Map) value).size();
        } else {
            throw new IllegalArgumentException("Could not determine the size of the provided value");
        }

        return length;
    }
}
