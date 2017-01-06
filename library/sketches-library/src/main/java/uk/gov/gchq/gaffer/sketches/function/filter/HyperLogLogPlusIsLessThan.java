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
package uk.gov.gchq.gaffer.sketches.function.filter;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleFilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;

/**
 * An <code>HyperLogLogPlus</code> is a {@link SimpleFilterFunction} that simply checks that the input
 * {@link HyperLogLogPlus} cardinality is less than a control value.
 */
@Inputs(HyperLogLogPlus.class)
public class HyperLogLogPlusIsLessThan extends SimpleFilterFunction<HyperLogLogPlus> {
    private long controlValue;
    private boolean orEqualTo;

    public HyperLogLogPlusIsLessThan() {
        // Required for serialisation
    }

    public HyperLogLogPlusIsLessThan(final long controlValue) {
        this(controlValue, false);
    }

    public HyperLogLogPlusIsLessThan(final long controlValue, final boolean orEqualTo) {
        this.controlValue = controlValue;
        this.orEqualTo = orEqualTo;
    }

    @JsonProperty("value")
    public long getControlValue() {
        return controlValue;
    }

    public void setControlValue(final long controlValue) {
        this.controlValue = controlValue;
    }

    public boolean getOrEqualTo() {
        return orEqualTo;
    }

    public void setOrEqualTo(final boolean orEqualTo) {
        this.orEqualTo = orEqualTo;
    }

    @Override
    public HyperLogLogPlusIsLessThan statelessClone() {
        return new HyperLogLogPlusIsLessThan(controlValue, orEqualTo);
    }

    @Override
    public boolean isValid(final HyperLogLogPlus input) {
        if (input == null) {
            return false;
        }
        long cardinality = input.cardinality();
        if (orEqualTo) {
            if (cardinality <= controlValue) {
                return true;
            }
        } else {
            if (cardinality < controlValue) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final HyperLogLogPlusIsLessThan that = (HyperLogLogPlusIsLessThan) o;

        return new EqualsBuilder()
                .append(inputs, that.inputs)
                .append(controlValue, that.controlValue)
                .append(orEqualTo, that.orEqualTo)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(controlValue)
                .append(orEqualTo)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("controlValue", controlValue)
                .append("orEqualTo", orEqualTo)
                .toString();
    }
}
