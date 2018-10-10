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

package uk.gov.gchq.gaffer.operation.impl.join.methods;

import com.google.common.collect.ImmutableMap;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.impl.join.JoinFunctionTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FullJoinTest extends JoinFunctionTest {

    @Override
    protected List<Map<Element, List<Element>>> getExpectedLeftKeyResults() {
        return Arrays.asList(
                ImmutableMap.of(getElement(10), Collections.emptyList()),
                ImmutableMap.of(getElement(1), Collections.singletonList(getElement(1))),
                ImmutableMap.of(getElement(2), Collections.singletonList(getElement(2))),
                ImmutableMap.of(getElement(3), Collections.singletonList(getElement(3))),
                ImmutableMap.of(getElement(4), Collections.singletonList(getElement(4))),
                ImmutableMap.of(getElement(12), Collections.emptyList())
        );
    }

    @Override
    protected List<Map<Element, List<Element>>> getExpectedRightKeyResults() {
        return Arrays.asList(
                ImmutableMap.of(getElement(10), Collections.emptyList()),
                ImmutableMap.of(getElement(1), Collections.singletonList(getElement(1))),
                ImmutableMap.of(getElement(2), Collections.singletonList(getElement(2))),
                ImmutableMap.of(getElement(3), Collections.singletonList(getElement(3))),
                ImmutableMap.of(getElement(4), Collections.singletonList(getElement(4))),
                ImmutableMap.of(getElement(12), Collections.emptyList())
        );
    }

    @Override
    protected JoinFunction getJoinFunction() {
        return new FullJoin();
    }
}