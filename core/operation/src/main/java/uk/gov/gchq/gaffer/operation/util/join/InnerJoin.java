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

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.operation.util.matcher.Matcher;
import uk.gov.gchq.gaffer.operation.util.matcher.MatchingOnIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InnerJoin implements JoinFunction {
    @Override
    public Iterable join(final Iterable left, final Iterable right, final Matcher matcher, final MatchingOnIterable matchingOnIterable) {
        List results = new ArrayList<>();
        List leftList = Lists.newArrayList(new LimitedCloseableIterable(left, 0, 10000, false));
        List rightList = Lists.newArrayList(new LimitedCloseableIterable(right, 0, 10000, false));

        switch (matchingOnIterable){
            case LEFT: {
                for (Object leftListObject : leftList) {
                    Map<Object, List> matchingObjects = matcher.matching(leftListObject, rightList);
                    for (List o : matchingObjects.values()) {
                        if (!o.isEmpty()) {
                            results.add(matchingObjects);
                        }
                    }
                }
            }
            case RIGHT: {
                for (Object rightListObject : rightList) {
                    Map<Object, List> matchingObjects = matcher.matching(rightListObject, leftList);
                    for (List o : matchingObjects.values()) {
                        if (!o.isEmpty()) {
                            results.add(matchingObjects);
                        }
                    }
                }
            }
        }
        return results;
    }
}
