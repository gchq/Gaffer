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
import uk.gov.gchq.gaffer.operation.impl.join.JoinFunctionTest;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OuterJoinTest extends JoinFunctionTest {

    @Override
    protected List<MapTuple> getExpectedLeftKeyResults() {
        return Arrays.asList(
                createMapTuple(getElement(10), Collections.emptyList())
        );
    }

    @Override
    protected List<MapTuple> getExpectedRightKeyResults() {
        return Arrays.asList(
                createMapTuple(Collections.emptyList(), getElement(12))
        );
    }

    @Override
    protected List<MapTuple> getExpectedLeftKeyResultsFlattened() {
        return Arrays.asList(
                createMapTuple(getElement(10), null)
        );
    }

    @Override
    protected List<MapTuple> getExpectedRightKeyResultsFlattened() {
        return Arrays.asList(
                createMapTuple(null, getElement(12))
        );
    }

    @Override
    protected JoinFunction getJoinFunction() {
        return new OuterJoin();
    }
}
