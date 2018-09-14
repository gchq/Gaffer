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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import uk.gov.gchq.gaffer.operation.util.matcher.Matcher;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchingOnIterable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FullJoin implements JoinFunction {
    @Override
    public Iterable join(final List left, final List right, final Matcher matcher, final MatchingOnIterable matchOn) {
        // TODO - fix this, currently broken
        Set resultSet = new HashSet<>();

        if (matchOn.equals(MatchingOnIterable.LEFT)) {
            for (Object listObject : left) {
                List matchingObjects = matcher.matching(listObject, right);
                resultSet.add(ImmutableMap.of(listObject, matchingObjects));
                for (Object o : right) {
                    resultSet.add(ImmutableList.of(o, new ArrayList<>()));
                }
            }
        } else {
            for (Object listObject : right) {
                List matchingObjects = matcher.matching(listObject, left);
                resultSet.add(ImmutableMap.of(listObject, matchingObjects));
                for (Object o : left) {
                    resultSet.add(ImmutableList.of(o, new ArrayList<>()));
                }
            }
        }
        return resultSet;
    }
}
