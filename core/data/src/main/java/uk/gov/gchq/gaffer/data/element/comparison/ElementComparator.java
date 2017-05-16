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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

/**
 * Base interface describing {@link uk.gov.gchq.gaffer.data.element.Element}
 * {@link java.util.Comparator} instances.
 */
public interface ElementComparator extends Comparator<Element> {

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    Comparator getComparator();

    void setComparator(final Comparator comparator);

    boolean isReversed();

    void setReversed(final boolean reversed);

    boolean isIncludeNulls();

    void setIncludeNulls(final boolean includeNulls);

    default Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        return Collections.emptySet();
    }

    abstract class Builder<C extends ElementComparator, B extends Builder<C, ?>> {

        private C elementComparator;

        protected Builder(final C elementComparator) {
            this.elementComparator = elementComparator;
        }

        public C build() {
            return _getComparator();
        }

        public C _getComparator() {
            return elementComparator;
        }

        public B _self() {
            return (B) this;
        }

        public ElementComparator.Builder comparator(final Comparator comparator) {
            _getComparator().setComparator(comparator);
            return _self();
        }

        public ElementComparator.Builder reverse(final boolean reverse) {
            _getComparator().setReversed(reverse);
            return _self();
        }

        public ElementComparator.Builder includeNulls(final boolean includeNulls) {
            _getComparator().setIncludeNulls(includeNulls);
            return _self();
        }
    }
}
