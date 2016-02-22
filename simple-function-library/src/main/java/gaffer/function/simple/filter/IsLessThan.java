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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import gaffer.function.SimpleFilterFunction;
import gaffer.function.annotation.Inputs;

/**
 * An <code>IsLessThan</code> is a {@link SimpleFilterFunction} that checks that the input
 * {@link java.lang.Comparable} is less than a control value. There is also an orEqualTo flag that can be set to allow
 * the input value to be less than or equal to the control value.
 */
@Inputs(Comparable.class)
public class IsLessThan extends SimpleFilterFunction<Comparable> {
    private Comparable controlValue;
    private boolean orEqualTo;

    public IsLessThan() {
        // Required for serialisation
    }

    public IsLessThan(final Comparable<?> controlValue) {
        this(controlValue, false);
    }

    public IsLessThan(final Comparable controlValue, final boolean orEqualTo) {
        this.controlValue = controlValue;
        this.orEqualTo = orEqualTo;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    @JsonProperty("value")
    public Comparable getControlValue() {
        return controlValue;
    }

    public void setControlValue(final Comparable controlValue) {
        this.controlValue = controlValue;
    }

    public boolean getOrEqualTo() {
        return orEqualTo;
    }

    public void setOrEqualTo(final boolean orEqualTo) {
        this.orEqualTo = orEqualTo;
    }

    public IsLessThan statelessClone() {
        final IsLessThan clone = new IsLessThan(controlValue);
        clone.setOrEqualTo(orEqualTo);

        return clone;
    }

    @Override
    protected boolean _isValid(final Comparable input) {
        if (null == input || !controlValue.getClass().isAssignableFrom(input.getClass())) {
            return false;
        }

        final int compareVal = controlValue.compareTo(input);
        if (orEqualTo) {
            return compareVal >= 0;
        }

        return compareVal > 0;
    }
}
