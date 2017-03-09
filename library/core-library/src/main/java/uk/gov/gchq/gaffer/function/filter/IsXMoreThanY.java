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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.function.FilterFunction;
import java.util.function.BiPredicate;

/**
 * An <code>IsXMoreThanY</code> is a {@link FilterFunction} that checks that the first input
 * {@link Comparable} is more than the second input {@link Comparable}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public class IsXMoreThanY implements BiPredicate<Comparable, Comparable> {
    @Override
    public boolean test(final Comparable input1, final Comparable input2) {
        return null != input1 && null != input2
                && input1.getClass() == input2.getClass()
                && (input1.compareTo(input2) > 0);
    }

    @Override
    public boolean equals(final Object o) {
        return this == o || o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getClass())
                .toHashCode();
    }
}
