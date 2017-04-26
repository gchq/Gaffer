/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.utils;

/**
 * A class to represent a pair of objects, potentially of different classes.
 */
public class Pair<T1, T2> {
    private final T1 t1;
    private final T2 t2;

    public Pair(final T1 t1, final T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getFirst() {
        return t1;
    }

    public T2 getSecond() {
        return t2;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pair<?, ?> pair = (Pair<?, ?>) o;

        if (t1 != null ? !t1.equals(pair.t1) : pair.t1 != null) {
            return false;
        }
        return t2 != null ? t2.equals(pair.t2) : pair.t2 == null;

    }

    @Override
    public int hashCode() {
        int result = t1 != null ? t1.hashCode() : 0;
        result = 31 * result + (t2 != null ? t2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Pair{t1=" + t1 + ", t2=" + t2 + '}';
    }

}
