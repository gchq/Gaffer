/*
 * Copyright 2018 Crown Copyright
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

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.koryphe.tuple.predicate.KoryphePredicate2;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * An {@code ElementJoinComparator} is a {@link KoryphePredicate2} that is
 * used to compare if two {@link Element}s are equal.  Optional {@code GroupBy} properties
 * can be set, and will then be used within the comparison also.
 */
public class ElementJoinComparator extends KoryphePredicate2<Element, Element> {
    private Set<String> groupByProperties = new HashSet<>();


    public ElementJoinComparator() {
    }

    public ElementJoinComparator(final Set<String> groupByProperties) {
        this.groupByProperties.addAll(groupByProperties);
    }

    public ElementJoinComparator(final String... groupByProperties) {
        this.groupByProperties = Sets.newHashSet(groupByProperties);
    }

    public Set<String> getGroupByProperties() {
        return groupByProperties;
    }

    public void setGroupByProperties(final Set<String> groupByProperties) {
        this.groupByProperties = groupByProperties;
    }

    @Override
    public boolean test(final Element element, final Element element2) {
        if (element == element2) {
            return true;
        }

        if (null == element || null == element2) {
            return false;
        }

        if (!element.getClass().equals(element2.getClass())) {
            return false;
        }

        if (!element.getGroup().equals(element2.getGroup())) {
            return false;
        }

        if (element instanceof Entity) {
            if (!((Entity) element).getVertex().equals(((Entity) element2).getVertex())) {
                return false;
            }
        } else {
            if (!((Edge) element).getSource().equals(((Edge) element2).getSource())) {
                return false;
            }
            if (!((Edge) element).getDestination().equals(((Edge) element2).getDestination())) {
                return false;
            }
            if (!((Edge) element).getDirectedType().equals(((Edge) element2).getDirectedType())) {
                return false;
            }
        }

        for (final String key : groupByProperties) {
            if (!Objects.equals(element.getProperty(key), element2.getProperty(key))) {
                return false;
            }
        }

        return true;
    }
}
