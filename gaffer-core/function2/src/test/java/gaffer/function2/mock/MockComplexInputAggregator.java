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
import gaffer.tuple.tuplen.Tuple3;
import gaffer.tuple.tuplen.value.Value2;
import gaffer.tuple.tuplen.value.Value3;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MockComplexInputAggregator implements Aggregator<Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>> {
    @Override
    public Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> execute(
            Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> input,
            Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> state) {
        if (state == null) {
            return input;
        } else {
            int in0a = input.get0().get0();
            String in0b = input.get0().get1();
            int in1 = input.get1();
            Iterator<String> in2 = input.get2().iterator();
            String in2a = in2.next();
            String in2b = in2.next();
            String in2c = in2.next();

            int s0a = state.get0().get0();
            String s0b = state.get0().get1();
            int s1 = state.get1();
            Iterator<String> s2 = state.get2().iterator();
            String s2a = s2.next();
            String s2b = s2.next();
            String s2c = s2.next();

            state.get0().put0(in0a + s0a);
            state.get0().put1(in0b + s0b);
            state.put1(in1 + s1);
            state.put2(Arrays.asList(in2a + s2a, in2b + s2b, in2c + s2c));
            return state;
        }
    }

    @Override
    public MockComplexInputAggregator copy() {
        return new MockComplexInputAggregator();
    }
}
