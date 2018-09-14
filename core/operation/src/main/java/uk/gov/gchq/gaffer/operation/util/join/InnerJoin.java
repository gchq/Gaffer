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

package uk.gov.gchq.gaffer.operation.util.join;

import com.google.common.collect.ImmutableMap;

import uk.gov.gchq.gaffer.operation.util.matcher.Matcher;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchingOnIterable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class InnerJoin implements JoinFunction {
    @Override
    public Iterable join(final List left, final List right, final Matcher matcher, final MatchingOnIterable matchingOnIterable) {
        if (matchingOnIterable.equals(MatchingOnIterable.LEFT)) {
            return getResultSet(left, right, matcher);
        } else if (matchingOnIterable.equals(MatchingOnIterable.RIGHT)) {
            return getResultSet(right, left, matcher);
        } else {
            return new HashSet<>();
        }
    }

    private Set getResultSet(List startingList, List secondaryList, final Matcher matcher) {
        Set resultSet = new HashSet<>();
        for (Object listObject : startingList) {
            List matchingObjects = matcher.matching(listObject, secondaryList);
            if (!matchingObjects.isEmpty()) {
                resultSet.add(ImmutableMap.of(listObject, matchingObjects));
            }
        }
        return resultSet;
    }
}
