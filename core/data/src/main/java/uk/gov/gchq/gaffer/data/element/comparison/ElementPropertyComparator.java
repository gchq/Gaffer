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
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link uk.gov.gchq.gaffer.data.element.comparison.ElementComparator} implementation
 * to use when making comparisons based on a single element property (e.g. a count
 * field).
 */
@SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
        justification = "This class should not be serialised")
public class ElementPropertyComparator implements ElementComparator {
    private Comparator comparator;

    private String propertyName = null;
    private Set<String> groupNames = Collections.emptySet();
    private boolean reversed;

    @Override
    public int compare(final Element e1, final Element e2) {
        if (e1 == null) {
            if (e2 == null) {
                return 0;
            }
            return 1;
        }
        if (e2 == null) {
            return -1;
        }

        if (!groupNames.contains(e1.getGroup())) {
            if (!groupNames.contains(e2.getGroup())) {
                return 0;
            }
            return 1;
        }
        if (!groupNames.contains(e2.getGroup())) {
            return -1;
        }

        return _compare(e1.getProperty(propertyName), e2.getProperty(propertyName));
    }

    public int _compare(final Object val1, final Object val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return 1;
        }
        if (val2 == null) {
            return -1;
        }

        if (reversed) {
            return ((Comparable) val2).compareTo(val1);
        }

        return ((Comparable) val1).compareTo(val2);
    }

    @Override
    public Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        if (null == comparator) {
            if (1 == groupNames.size()) {
                return Collections.singleton(new Pair<>(groupNames.iterator().next(), propertyName));
            }

            final Set<Pair<String, String>> pairs = new HashSet<>(groupNames.size());
            for (final String groupName : groupNames) {
                pairs.add(new Pair<>(groupName, propertyName));
            }
            return pairs;
        }

        return Collections.emptySet();
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    public Set<String> getGroupNames() {
        return groupNames;
    }

    public void setGroupNames(final Set<String> groupNames) {
        this.groupNames = groupNames;
    }

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(final boolean reversed) {
        this.reversed = reversed;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(final Comparator comparator) {
        this.comparator = comparator;
    }

    public static class Builder {

        private ElementPropertyComparator comparator = new ElementPropertyComparator();

        public ElementPropertyComparator build() {
            return comparator;
        }

        public Builder comparator(final Comparator comparator) {
            this.comparator.setComparator(comparator);
            return this;
        }

        public Builder groupNames(final String... groupName) {
            comparator.setGroupNames(Sets.newHashSet(groupName));
            return this;
        }

        public Builder propertyName(final String propertyName) {
            comparator.setPropertyName(propertyName);
            return this;
        }

        public Builder reverse(final boolean reverse) {
            comparator.setReversed(reverse);
            return this;
        }
    }
}
