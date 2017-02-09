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
import uk.gov.gchq.gaffer.function.FilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;

/**
 * An <code>AgeOffFromDays</code> is a {@link uk.gov.gchq.gaffer.function.processor.Filter}
 * that ages off old data based on a provided age off time in days.
 */
@Inputs({Long.class, Integer.class})
public class AgeOffFromDays extends FilterFunction {
    public static final long DAYS_TO_MILLISECONDS = 24L * 60L * 60L * 1000L;

    // Default constructor for serialisation
    public AgeOffFromDays() {
    }

    @Override
    public boolean isValid(final Object[] input) {
        if (null == input || input.length != 2) {
            return false;
        }

        final Long timestamp = (Long) input[0];
        final Integer days = (Integer) input[1];

        return (null != timestamp && null != days) && (System.currentTimeMillis() - (days * DAYS_TO_MILLISECONDS) < timestamp);
    }

    public AgeOffFromDays statelessClone() {
        return new AgeOffFromDays();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AgeOffFromDays ageOff = (AgeOffFromDays) o;

        return new EqualsBuilder()
                .append(inputs, ageOff.inputs)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(inputs)
                .toHashCode();
    }
}
