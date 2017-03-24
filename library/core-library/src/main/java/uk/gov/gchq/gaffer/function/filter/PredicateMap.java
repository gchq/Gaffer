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
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;
import java.util.Map;
import java.util.function.Predicate;

/**
 * An <code>PredicateMap</code> is a {@link Predicate} that extracts a
 * value from a map using the provided key and passes the value to a provided
 * {@link Predicate}.
 */
public class PredicateMap<T> extends KoryphePredicate<Map<?, T>> {
    private Predicate<? super T> predicate;
    private Object key;

    public PredicateMap() {
    }

    public PredicateMap(final Object key, final Predicate<? super T> predicate) {
        this.predicate = predicate;
        this.key = key;
    }

    @Override
    public boolean test(final Map<?, T> map) {
        if (null == predicate) {
            return true;
        }

        if (null == map) {
            return false;
        }

        try {
            return predicate.test(map.get(key));
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Input does not match parametrised type", e);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Predicate<? super T> getPredicate() {
        return predicate;
    }

    public void setPredicate(final Predicate<? super T> predicate) {
        this.predicate = predicate;
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

        if (null == o || !getClass().equals(o.getClass())) {
            return false;
        }

        final PredicateMap predicateMap = (PredicateMap) o;
        return new EqualsBuilder()
                .append(predicate, predicateMap.predicate)
                .append(key, predicateMap.key)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(predicate)
                .append(key)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("predicate", predicate)
                .append("key", key)
                .toString();
    }
}
