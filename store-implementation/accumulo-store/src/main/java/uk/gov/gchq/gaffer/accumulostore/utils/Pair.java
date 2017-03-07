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

package uk.gov.gchq.gaffer.accumulostore.utils;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.io.Serializable;

/**
 * A simple class to contain a pair of items.
 *
 * @param <T> type of items in the pair
 */
public class Pair<T> implements Serializable {
    private static final long serialVersionUID = 4769405415756562347L;
    private T first;
    private T second;

    public Pair() { }

    public Pair(final T first, final T second) {
        this.first = first;
        this.second = second;
    }
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public T getFirst() {
        return first;
    }

    public void setFirst(final T first) {
        this.first = first;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public T getSecond() {
        return second;
    }

    public void setSecond(final T second) {
        this.second = second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        final Pair<?> other = (Pair<?>) obj;
        if (first == null) {
            if (other.first != null) {
                return false;
            }
        } else if (!first.equals(other.first)) {
            return false;
        }

        if (second == null) {
            if (other.second != null) {
                return false;
            }
        } else if (!second.equals(other.second)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "Pair{" + "first=" + first + ", second=" + second + '}';
    }
}
