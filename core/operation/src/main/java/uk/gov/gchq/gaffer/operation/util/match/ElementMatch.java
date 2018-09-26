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

package uk.gov.gchq.gaffer.operation.util.match;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.comparison.ElementEquality;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Tests for matches for Elements within a Join Operation, groupBy properties can be optionally specified.
 */
public class ElementMatch implements Match {
    private ElementEquality elementEquality;

    public ElementMatch() {
        elementEquality = new ElementEquality();
    }

    public ElementMatch(final String... groupByProperties) {
        elementEquality = new ElementEquality(groupByProperties);
    }

    public ElementMatch(final Set<String> groupByProperties) {
        elementEquality = new ElementEquality(groupByProperties);
    }

    public void setElementGroupByProperties(final Set<String> groupByProperties) {
        elementEquality.setGroupByProperties(groupByProperties);
    }

    @Override
    public List matching(final Object testObject, final List testList) {
        List matches = new ArrayList<>();

        for (Object entry : testList) {
            if (elementEquality.test((Element) entry, (Element) testObject)) {
                matches.add(((Element) entry).shallowClone());
            }
        }
        return matches;
    }
}
