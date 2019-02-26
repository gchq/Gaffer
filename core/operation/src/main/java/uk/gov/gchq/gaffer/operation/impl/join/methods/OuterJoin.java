/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.join.methods;

import com.google.common.collect.ImmutableMap;

import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@code OuterJoin} is a Join function which returns values from an iterable (together with an empty list)
 * where they do not match with any value in the list.
 */
public class OuterJoin implements JoinFunction {
    @Override
    public List join(final Iterable left, final List right, final Match match) {
        List resultList = new ArrayList<>();

        for (final Object leftObj : left) {
            List matching = match.matching(leftObj, right);
            if (matching.isEmpty()) {
                resultList.add(ImmutableMap.of(leftObj, matching));
            }
        }

        return resultList;
    }
}
