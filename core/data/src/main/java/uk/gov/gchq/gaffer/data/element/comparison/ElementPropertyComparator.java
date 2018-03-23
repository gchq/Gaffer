/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
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
 * An {@link uk.gov.gchq.gaffer.data.element.comparison.ElementComparator} implementation
 * to use when making comparisons based on a single element property (e.g. a count
 * field).
 * <p>
 * You must provide the property name and the set of element groups that contain
 * that property.
 * </p>
 * <p>
 * Any elements that are compared that are not in one of the provided groups
 * will be returned 'last' in the result.
 * </p>
 * <p>
 * Any elements that are compared that do not have the provided property
 * will also be returned 'last' in the result.
 * </p>
 * <p>
 * There is a reversed option to allow you to flip the comparison value.
 * </p>
 */
@JsonPropertyOrder(value = {"class", "property", "comparator", "groups"}, alphabetic = true)
@SuppressFBWarnings(value = "SE_COMPARATOR_SHOULD_BE_SERIALIZABLE",
        justification = "This class should not be serialised")
public class ElementPropertyComparator implements ElementComparator {
    private Comparator comparator;
    public static int count = 0;

    private String property = null;
    private Set<String> groups = Collections.emptySet();
    private boolean reversed;

    @Override
    public int compare(final Element e1, final Element e2) {
        count++;

        if (null == e1) {
            if (null == e2) {
                return 0;
            }
            return 1;
        }
        if (null == e2) {
            return -1;
        }

        if (!groups.contains(e1.getGroup())) {
            if (!groups.contains(e2.getGroup())) {
                return 0;
            }
            return 1;
        }
        if (!groups.contains(e2.getGroup())) {
            return -1;
        }

        return _compare(e1.getProperty(property), e2.getProperty(property));
    }

    public int _compare(final Object val1, final Object val2) {
        if (null == val1) {
            if (null == val2) {
                return 0;
            }
            return 1;
        }
        if (null == val2) {
            return -1;
        }

        if (null == comparator) {
            if (reversed) {
                return ((Comparable) val2).compareTo(val1);
            }

            return ((Comparable) val1).compareTo(val2);
        }

        if (reversed) {
            return comparator.compare(val2, val1);
        }

        return comparator.compare(val1, val2);
    }

    @Override
    public Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        if (null == comparator) {
            if (1 == groups.size()) {
                return Collections.singleton(new Pair<>(groups.iterator().next(), property));
            }

            final Set<Pair<String, String>> pairs = new HashSet<>(groups.size());
            for (final String groupName : groups) {
                pairs.add(new Pair<>(groupName, property));
            }
            return pairs;
        }

        return Collections.emptySet();
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(final String property) {
        this.property = property;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(final Set<String> groups) {
        this.groups = groups;
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

        public Builder groups(final String... group) {
            comparator.setGroups(Sets.newHashSet(group));
            return this;
        }

        public Builder property(final String property) {
            comparator.setProperty(property);
            return this;
        }

        public Builder reverse(final boolean reverse) {
            comparator.setReversed(reverse);
            return this;
        }
    }
}
