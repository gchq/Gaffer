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

import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Comparator;
import java.util.function.Predicate;

public class ElementPropertyComparator extends ElementComparator {
    private static final long serialVersionUID = -4368650085560146741L;

    private String propertyName;
    private String groupName;

    protected ElementPropertyComparator(final String groupName, final String propertyName) {
        this.groupName = groupName;
        this.propertyName = propertyName;
    }

    public ElementPropertyComparator(final Builder builder) {
        this.groupName = builder.groupName;
        this.propertyName = builder.propertyName;
        this.comparator = builder.comparator;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getGroupName() {
        return groupName;
    }

    public Predicate<Element> asPredicate() {
        final Predicate<Element> hasCorrectGroup = e -> groupName == e.getGroup();
        final Predicate<Element> propertyIsNull = e -> null == e.getProperty(propertyName);
        final Predicate<Element> nullsExcluded = e -> !isIncludeNulls();

        return propertyIsNull.and(nullsExcluded).negate().and(hasCorrectGroup);
    }

    @Override
    public int compare(final Element o1, final Element o2) {
        if (null != comparator) {
            return comparator.compare(o1.getProperty(propertyName), o2.getProperty(propertyName));
        }
        return ((Comparable) o1.getProperty(propertyName)).compareTo(o2.getProperty(propertyName));
    }

    public static class Builder {
        private String groupName;
        private String propertyName;
        private Comparator comparator;

        public Builder() {
            // empty
        }

        public ElementPropertyComparator.Builder groupName(final String groupName) {
            this.groupName = groupName;
            return this;
        }

        public ElementPropertyComparator.Builder propertyName(final String propertyName) {
            this.propertyName = propertyName;
            return this;
        }

        public ElementPropertyComparator.Builder comparator(final Comparator comparator) {
            this.comparator = comparator;
            return this;
        }

        public ElementPropertyComparator build() {
            if (null == groupName || null == propertyName) {
                throw new IllegalArgumentException("Must provide non-null groupName and propertyName.");
            }
            return new ElementPropertyComparator(this);
        }
    }
}
