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

package uk.gov.gchq.gaffer.data.element.comparison;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Comparator;

/**
 * {@link uk.gov.gchq.gaffer.data.element.comparison.ElementComparator} implementation
 * to use when making comparisons based on {@link uk.gov.gchq.gaffer.data.element.Element}s
 * (e.g. concerning multiple properties or a combination of vertex/group/property etc).
 */
@SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
        justification = "This class should not be serialised")
public class ElementObjectComparator implements ElementComparator {

    /**
     * Reference to a mutated comparator, required since modifying the original
     * comparator leads to a non-JSON serialisable object.
     */
    @JsonIgnore
    private Comparator mutatedComparator;

    private Comparator comparator;
    private boolean reversed;
    private boolean includeNulls;

    public ElementObjectComparator() {
        // Empty
    }

    @Override
    public Comparator getComparator() {
        return comparator;
    }

    @Override
    public void setComparator(final Comparator comparator) {
        this.comparator = comparator;
        mutateComparator();
    }

    @Override
    public boolean isReversed() {
        return reversed;
    }

    @Override
    public void setReversed(final boolean reversed) {
        this.reversed = reversed;
        mutateComparator();
    }

    @Override
    public boolean isIncludeNulls() {
        return includeNulls;
    }

    @Override
    public void setIncludeNulls(final boolean includeNulls) {
        this.includeNulls = includeNulls;
        mutateComparator();
    }

    private void mutateComparator() {
        if (null != comparator) {
            mutatedComparator = comparator;

            if (reversed) {
                mutatedComparator = mutatedComparator.reversed();
            }
            if (includeNulls) {
                mutatedComparator = Comparator.nullsLast(mutatedComparator);
            }
        }
    }

    @Override
    public int compare(final Element obj1, final Element obj2) {
        if (null != mutatedComparator) {
            return mutatedComparator.compare(obj1, obj2);
        }
        throw new IllegalArgumentException("Must provide a comparator instance.");
    }

    public static class Builder extends ElementComparator.Builder<ElementObjectComparator, Builder> {
        public Builder() {
            super(new ElementObjectComparator());
        }
    }
}
