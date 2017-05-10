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
package uk.gov.gchq.gaffer.commonutil.comparison;

import java.io.Serializable;
import java.util.Comparator;

public abstract class Comparison<T> implements Comparator<T>, Serializable {

    private boolean reversed;
    private boolean includeNulls;

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(final boolean reversed) {
        this.reversed = reversed;
    }

    public boolean isIncludeNulls() {
        return includeNulls;
    }

    public void setIncludeNulls(final boolean includeNulls) {
        this.includeNulls = includeNulls;
    }

    public Comparator apply() {
        return includeNulls(reverse(this));
    }

    private Comparator<T> reverse(final Comparator<T> comparison) {
        return reversed ? comparison.reversed() : comparison;
    }

    private Comparator<T> includeNulls(final Comparator<T> comparison) {
        return includeNulls ? Comparator.nullsLast(comparison) : comparison;
    }
}
