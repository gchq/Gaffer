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

package koryphe.tuple.function;

import koryphe.function.Adapter;
import koryphe.function.stateful.aggregator.Aggregator;
import koryphe.function.mock.MockAggregator;
import koryphe.tuple.Tuple;
import koryphe.tuple.tuplen.Tuple2;
import koryphe.tuple.tuplen.value.Value2;
import koryphe.tuple.adapter.TupleAdapter;
import org.junit.Test;
import util.JsonSerialiser;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TupleAggregatorTest {
    @Test
    public void testTupleAggregation() {
        String[] inputs = new String[]{"input1", "input2", "input3"};
        Tuple2<String, String>[] outputs = new Tuple2[]{new Value2<>(inputs[0], null),
                                                        new Value2<>(inputs[1], inputs[0]),
                                                        new Value2<>(inputs[2], inputs[1])};

        TupleAggregator<String, String, Tuple2<String, String>> transformer = new TupleAggregator<>();
        Tuple<String>[] tuples = new Tuple[]{mock(Tuple.class), mock(Tuple.class), mock(Tuple.class)};

        // set up the function
        Aggregator<String, Tuple2<String, String>> function1 = mock(Aggregator.class);
        TupleAdapter<String, String> inputAdapter = mock(TupleAdapter.class);
        TupleAdapter<String, Tuple2<String, String>> outputAdapter = mock(TupleAdapter.class);
        transformer.setFunction(function1);
        transformer.setInputAdapter(inputAdapter);
        transformer.setOutputAdapter(outputAdapter);
        Tuple<String> state = null;
        for (int i = 0; i < tuples.length; i++) {
            Tuple2<String, String> previousOutput = null;
            given(inputAdapter.from(tuples[i])).willReturn(inputs[i]);
            if (i > 0) {
                previousOutput = outputs[i - 1];
                given(outputAdapter.from(state)).willReturn(previousOutput);
            }
            given(function1.execute(inputs[i], previousOutput)).willReturn(outputs[i]);
            given(outputAdapter.to(outputs[i])).willReturn(tuples[0]);
            state = transformer.execute(tuples[i], state);
        }

        // check the expected calls
        verify(outputAdapter, times(1)).from(null);
        verify(outputAdapter, times(2)).from(tuples[0]);
        for (int i = 0; i < tuples.length; i++) {
            String in1 = inputs[i];
            Tuple2<String, String> in2 = null;
            if (i > 0) {
                in2 = outputs[i - 1];
            }
            verify(inputAdapter, times(1)).from(tuples[i]);
            verify(function1, times(1)).execute(in1, in2);
            verify(outputAdapter, times(1)).to(outputs[i]);
        }
    }

    @Test
    public void shouldCopy() {
        TupleAggregator<String, Integer, Integer> aggregator = new TupleAggregator();
        MockAggregator function = new MockAggregator();
        TupleAdapter<String, Integer> inputAdapter = new TupleAdapter<String, Integer>("a");
        TupleAdapter<String, Integer> outputAdapter = new TupleAdapter<String, Integer>("b");
        aggregator.setInputAdapter(inputAdapter);
        aggregator.setOutputAdapter(outputAdapter);
        aggregator.setFunction(function);

        TupleAggregator<String, Integer, Integer> aggregatorCopy = aggregator.copy();
        assertNotSame(aggregator, aggregatorCopy);

        Aggregator<Integer, Integer> functionCopy = aggregatorCopy.getFunction();
        assertNotSame(function, functionCopy);
        assertTrue(functionCopy instanceof MockAggregator);

        Adapter<Tuple<String>, Integer> inputAdapterCopy = aggregatorCopy.getInputAdapter();
        Adapter<Tuple<String>, Integer> outputAdapterCopy = aggregatorCopy.getOutputAdapter();
        assertTrue(inputAdapterCopy instanceof TupleAdapter);
        assertTrue(outputAdapterCopy instanceof TupleAdapter);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleAggregator<String, Integer, Integer> aggregator = new TupleAggregator();
        MockAggregator function = new MockAggregator();
        TupleAdapter<String, Integer> inputAdapter = new TupleAdapter<>("a");
        TupleAdapter<String, Integer> outputAdapter = new TupleAdapter<>("b");
        aggregator.setInputAdapter(inputAdapter);
        aggregator.setOutputAdapter(outputAdapter);
        aggregator.setFunction(function);

        String json = JsonSerialiser.serialise(aggregator);
        TupleAggregator<String, Integer, Integer> deserialisedAggregator = JsonSerialiser.deserialise(json, TupleAggregator.class);

        // check deserialisation
        assertNotSame(aggregator, deserialisedAggregator);

        Aggregator<Integer, Integer> deserialisedFunction = deserialisedAggregator.getFunction();
        assertNotSame(function, deserialisedFunction);
        assertTrue(deserialisedFunction instanceof MockAggregator);

        Adapter<Tuple<String>, Integer> deserialisedInputAdapter = deserialisedAggregator.getInputAdapter();
        Adapter<Tuple<String>, Integer> deserialisedOutputAdapter = deserialisedAggregator.getOutputAdapter();
        assertNotSame(inputAdapter, deserialisedInputAdapter);
        assertTrue(deserialisedInputAdapter instanceof TupleAdapter);
        assertNotSame(outputAdapter, deserialisedOutputAdapter);
        assertTrue(deserialisedOutputAdapter instanceof TupleAdapter);
    }
}
