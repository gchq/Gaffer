/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.pair;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.io.Serializable;

/**
 * A simple class to contain a pair of items.
 *
 * @param <F> type of first item in the pair
 * @param <S> type of second item in the pair
 */
public class Pair<F, S> implements Serializable {
    private static final long serialVersionUID = 4769405415756562347L;

    private F first;
    private S second;

    public Pair() {
    }

    public Pair(final F first) {
        this(first, null);
    }

    public Pair(final F first, final S second) {
        this.first = first;
        this.second = second;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public F getFirst() {
        return first;
    }

    public void setFirst(final F first) {
        this.first = first;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public S getSecond() {
        return second;
    }

    public void setSecond(final S second) {
        this.second = second;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final Pair<?, ?> pair = (Pair<?, ?>) obj;

        return new EqualsBuilder().append(first, pair.first)
                .append(second, pair.second)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 29)
                .append(first)
                .append(second)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("first", first)
                .append("second", second)
                .toString();
    }
}
