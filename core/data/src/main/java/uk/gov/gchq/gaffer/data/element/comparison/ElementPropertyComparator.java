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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import java.util.Collections;
import java.util.Comparator;
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
    private String groupName = null;

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

        if (!groupName.equals(e1.getGroup())) {
            if (!groupName.equals(e2.getGroup())) {
                return 0;
            }
            return 1;
        }
        if (!groupName.equals(e2.getGroup())) {
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
        return ((Comparable) val1).compareTo(val2);
    }

    @Override
    public Set<Pair<String, String>> getComparableGroupPropertyPairs() {
        if (null == comparator) {
            return Collections.singleton(new Pair<>(groupName, propertyName));
        }

        return Collections.emptySet();
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyName(final String propertyName) {
        this.propertyName = propertyName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(final String groupName) {
        this.groupName = groupName;
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

        public Builder groupName(final String groupName) {
            comparator.setGroupName(groupName);
            return this;
        }

        public Builder propertyName(final String propertyName) {
            comparator.setPropertyName(propertyName);
            return this;
        }
    }
}
