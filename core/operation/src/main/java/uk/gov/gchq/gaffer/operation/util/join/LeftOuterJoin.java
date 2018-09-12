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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LeftOuterJoin implements JoinFunction {
    @Override
    public Iterable join(final Iterable left, final Iterable right, final Matcher matcher) {
        Set resultsSet = new LinkedHashSet<>();
        List leftList = Lists.newArrayList(new LimitedCloseableIterable(left, 0, 100000, false));
        List rightList = Lists.newArrayList(new LimitedCloseableIterable(right, 0, 100000, false));

        // If right list does not contain the object from the left list, add it to a results list
        for (Object listObj : leftList) {
            resultsSet.add(matcher.matching(rightList, listObj));
        }
        return resultsSet;
    }
}
