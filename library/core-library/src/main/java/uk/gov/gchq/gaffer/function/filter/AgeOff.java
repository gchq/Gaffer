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
package uk.gov.gchq.gaffer.function.filter;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.function.SimpleFilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;

/**
 * An <code>AgeOff</code> is a {@link SimpleFilterFunction} that ages off old data based on a provided age of time in milliseconds.
 */
@Inputs(Long.class)
public class AgeOff extends SimpleFilterFunction<Long> {
    public static final long HOURS_TO_MILLISECONDS = 60L * 60L * 1000L;
    public static final long DAYS_TO_MILLISECONDS = 24L * HOURS_TO_MILLISECONDS;

    /**
     * The default age of time (1 year) in milliseconds.
     */
    public static final long AGE_OFF_TIME_DEFAULT = 365L * DAYS_TO_MILLISECONDS;

    private long ageOffTime = AGE_OFF_TIME_DEFAULT;

    // Default constructor for serialisation
    public AgeOff() {
    }

    public AgeOff(final long ageOffTime) {
        this.ageOffTime = ageOffTime;
    }

    public AgeOff statelessClone() {
        AgeOff clone = new AgeOff(ageOffTime);
        clone.setAgeOffTime(ageOffTime);

        return clone;
    }

    @Override
    public boolean isValid(final Long input) {
        return null != input && (System.currentTimeMillis() - input) < ageOffTime;
    }

    public long getAgeOffTime() {
        return ageOffTime;
    }

    public void setAgeOffTime(final long ageOffTime) {
        this.ageOffTime = ageOffTime;
    }

    public void setAgeOffDays(final int ageOfDays) {
        setAgeOffTime(DAYS_TO_MILLISECONDS * ageOfDays);
    }

    public void setAgeOffHours(final long ageOfHours) {
        setAgeOffTime(HOURS_TO_MILLISECONDS * ageOfHours);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AgeOff ageOff = (AgeOff) o;

        return new EqualsBuilder()
                .append(inputs, ageOff.inputs)
                .append(ageOffTime, ageOff.ageOffTime)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .append(ageOffTime)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("inputs", inputs)
                .append("ageOffTime", ageOffTime)
                .toString();
    }
}
