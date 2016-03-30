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

public class MockMultiInputAggregator extends Aggregator<Tuple2<Integer, Integer>> {
    private int total1 = 0;
    private int total2 = 0;

    @Override
    public void aggregate(Tuple2<Integer, Integer> input) {
        total1 += input.get0();
        total2 += input.get1();
    }

    @Override
    public void init() {
        total1 = 0;
        total2 = 0;
    }

    @Override
    public Tuple2<Integer, Integer> state() {
        Tuple2<Integer, Integer> out = Tuple2.createTuple();
        out.put0(total1);
        out.put1(total2);
        return out;
    }

    @Override
    public MockMultiInputAggregator copy() {
        return new MockMultiInputAggregator();
    }
}
