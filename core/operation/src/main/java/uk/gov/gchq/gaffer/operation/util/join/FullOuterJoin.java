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
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.operation.util.matcher.Matcher;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchingOnIterable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FullOuterJoin implements JoinFunction {
    @Override
    public Iterable join(final Iterable left, final Iterable right, final Matcher matcher, final MatchingOnIterable matchingOnIterable) {
        // If found in the right and the left but not in both then return it
        Set resultsSet = new HashSet<>();
        List leftList = Lists.newArrayList(new LimitedCloseableIterable(left, 0, 100000, false));
        List rightList = Lists.newArrayList(new LimitedCloseableIterable(right, 0, 100000, false));

        // If left list does not contain the object from the right list, add it to a results list,
        // otherwise remove from the left list, then at the end add everything remaining in the left list to the results list.
        for (Object listObj : rightList) {
            Map matchingMap = new HashMap<>();
            matchingMap.putAll(matcher.matching(listObj, leftList));
            if (!matchingMap.containsKey(listObj)) {
                resultsSet.add(ImmutableMap.of(listObj, new ArrayList<>()));
            }
        }

        for (Object listObj : leftList) {
            Map matchingMap = new HashMap<>();
            matchingMap.putAll(matcher.matching(listObj, rightList));
            if (!matchingMap.containsKey(listObj)) {
                resultsSet.add(ImmutableMap.of(listObj, new ArrayList<>()));
            }
        }

        return resultsSet;
    }
}
