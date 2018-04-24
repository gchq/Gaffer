/*
 * Copyright 2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.predicate;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yahoo.sketches.hll.HllSketch;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

/**
 * An {@code HllSketch} is a {@link java.util.function.Predicate} that simply checks that the input
 * {@link HllSketch} cardinality is less than a control value.
 * Before applying the is less than check, it will round the HllSketch estimate
 * to the nearest long.
 */
@Since("1.0.0")
public class HllSketchIsLessThan extends KoryphePredicate<HllSketch> {
    private long controlValue;
    private boolean orEqualTo;

    public HllSketchIsLessThan() {
        // Required for serialisation
    }

    public HllSketchIsLessThan(final long controlValue) {
        this(controlValue, false);
    }

    public HllSketchIsLessThan(final long controlValue, final boolean orEqualTo) {
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
    public boolean test(final HllSketch input) {
        if (null == input) {
            return false;
        }
        final long cardinality = Math.round(input.getEstimate());
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
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final HllSketchIsLessThan that = (HllSketchIsLessThan) obj;

        return new EqualsBuilder()
                .append(controlValue, that.controlValue)
                .append(orEqualTo, that.orEqualTo)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(79, 23)
                .append(controlValue)
                .append(orEqualTo)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("controlValue", controlValue)
                .append("orEqualTo", orEqualTo)
                .toString();
    }
}
