/*
 * Copyright 2016 Crown Copyright
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

package koryphe.function.mock;

import koryphe.function.stateful.aggregator.Aggregator;
import koryphe.tuple.tuplen.Tuple2;
import koryphe.tuple.tuplen.Tuple3;

public class MockComplexInputAggregator implements Aggregator<Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>,
                                                              Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>> {
    @Override
    public Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> execute(
            Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> input,
            Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> state) {
        return input;
    }

    @Override
    public MockComplexInputAggregator copy() {
        return new MockComplexInputAggregator();
    }
}
