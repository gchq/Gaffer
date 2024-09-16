/*
 * Copyright 2018-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.join.match;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementJoinComparator;
import uk.gov.gchq.gaffer.operation.impl.join.match.Match;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tests for matches for Elements within a Join Operation, groupBy properties can be optionally specified.
 */
public class ElementMatch implements Match {
    private final ElementJoinComparator elementJoinComparator;
    private Iterable matchCandidates;

    private static final String NULL_MATCH_CANDIDATES_ERROR_MESSAGE = "ElementMatch must be initialised with non-null match candidates";

    public ElementMatch() {
        elementJoinComparator = new ElementJoinComparator();
    }

    public ElementMatch(final String... groupByProperties) {
        elementJoinComparator = new ElementJoinComparator(groupByProperties);
    }

    public ElementMatch(final Set<String> groupByProperties) {
        elementJoinComparator = new ElementJoinComparator(groupByProperties);
    }

    public void setElementGroupByProperties(final Set<String> groupByProperties) {
        elementJoinComparator.setGroupByProperties(groupByProperties);
    }

    @Override
    public void init(final Iterable matchCandidates) {
        if (matchCandidates == null) {
            throw new IllegalArgumentException(NULL_MATCH_CANDIDATES_ERROR_MESSAGE);
        }
        this.matchCandidates = matchCandidates;
    }

    @Override
    public List matching(final Object testObject) {
        if (matchCandidates == null) {
            throw new IllegalArgumentException(NULL_MATCH_CANDIDATES_ERROR_MESSAGE);
        }

        List matches = new ArrayList<>();


        for (final Object entry : matchCandidates) {
            if (elementJoinComparator.test((Element) entry, (Element) testObject)) {
                matches.add(((Element) entry).shallowClone());
            }
        }
        return matches;
    }
}
