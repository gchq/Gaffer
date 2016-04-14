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

public class MockComplexInputAggregator extends Aggregator<Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>> {
    private int total1 = 0;
    private String concat1 = "";
    private int total2 = 0;
    private String concat2 = "";
    private String concat3 = "";
    private String concat4 = "";

    @Override
    public void aggregate(Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> input) {
        total1 += input.get0().get0();
        concat1 += input.get0().get1();
        total2 += input.get1();
        Iterator<String> in = input.get2().iterator();
        concat2 += in.next();
        concat3 += in.next();
        concat4 += in.next();
    }

    @Override
    public void init() {
        total1 = 0;
        concat1 = "";
        total2 = 0;
        concat2 = "";
        concat3 = "";
        concat4 = "";
    }

    @Override
    public Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> state() {
        Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> result = new Value3<>();
        Tuple2<Integer, String> result0 = new Value2<>();
        result0.put0(total1);
        result0.put1(concat1);
        result.put0(result0);
        result.put1(total2);
        List<String> result2 = Arrays.asList(concat2, concat3, concat4);
        result.put2(result2);
        return result;
    }

    @Override
    public MockComplexInputAggregator copy() {
        return new MockComplexInputAggregator();
    }
}
