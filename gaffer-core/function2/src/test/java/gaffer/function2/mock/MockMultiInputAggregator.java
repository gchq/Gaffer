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

package gaffer.function2.mock;

import gaffer.function2.Aggregator;
import gaffer.tuple.tuplen.Tuple2;
import gaffer.tuple.tuplen.value.Value2;

public class MockMultiInputAggregator implements Aggregator<Tuple2<Integer, Integer>> {
    @Override
    public Tuple2<Integer, Integer> execute(Tuple2<Integer, Integer> input, Tuple2<Integer, Integer> state) {
        if (state == null) {
            return input;
        } else {
            state.put0(input.get0() + state.get0());
            state.put1(input.get1() + state.get1());
            return state;
        }
    }

    @Override
    public MockMultiInputAggregator copy() {
        return new MockMultiInputAggregator();
    }
}
