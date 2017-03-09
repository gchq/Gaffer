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

import koryphe.function.aggregate.Aggregator;
import koryphe.function.mock.MockAggregator;
import koryphe.tuple.Tuple;
import koryphe.tuple.mask.TupleMask;
import org.junit.Test;
import util.JsonSerialiser;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

//TODO: implement test with combiner that converts strings to integers.
public class TupleAggregatorTest {
    @Test
    public void testTupleAggregation() {
        String[] inputs = new String[]{"input1", "input2", "input3"};
        String[] outputs = new String[]{"output1", "output2", "output3"};

        TupleAggregator<String, String> aggregator = new TupleAggregator<>();
        Tuple<String>[] tuples = new Tuple[]{mock(Tuple.class), mock(Tuple.class), mock(Tuple.class)};

        // set up the function
        Aggregator<String> function1 = mock(Aggregator.class);
        TupleMask<String, String> inputMask = mock(TupleMask.class);
        aggregator.setFunction(function1);
        aggregator.setSelection(inputMask);
        Tuple<String> state = null;
        for (int i = 0; i < tuples.length; i++) {
            String previousOutput = null;
            given(inputMask.select(tuples[i])).willReturn(inputs[i]);
            if (i > 0) {
                previousOutput = outputs[i - 1];
                given(inputMask.select(state)).willReturn(previousOutput);
            }
            given(function1.execute(inputs[i], previousOutput)).willReturn(outputs[i]);
            given(inputMask.project(outputs[i])).willReturn(tuples[0]);
            state = aggregator.execute(tuples[i], state);
        }

        // check the expected calls
        verify(inputMask, times(tuples.length)).select(tuples[0]);
        for (int i = 0; i < tuples.length; i++) {
            String in1 = inputs[i];
            String in2 = null;
            if (i > 0) {
                in2 = outputs[i - 1];
                verify(inputMask, times(1)).select(tuples[i]);
            }
            verify(function1, times(1)).execute(in1, in2);
            verify(inputMask, times(1)).project(outputs[i]);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleAggregator<String, Integer> aggregator = new TupleAggregator<>();
        MockAggregator function = new MockAggregator();
        TupleMask<String, Integer> inputMask = new TupleMask<>("a");
        aggregator.setSelection(inputMask);
        aggregator.setFunction(function);

        String json = JsonSerialiser.serialise(aggregator);
        TupleAggregator<String, Integer> deserialisedAggregator = JsonSerialiser.deserialise(json, TupleAggregator.class);

        // check deserialisation
        assertNotSame(aggregator, deserialisedAggregator);

        Aggregator<Integer> deserialisedFunction = deserialisedAggregator.getFunction();
        assertNotSame(function, deserialisedFunction);
        assertTrue(deserialisedFunction instanceof MockAggregator);

        TupleMask<String, Integer> deserialisedInputMask = deserialisedAggregator.getSelection();
        assertNotSame(inputMask, deserialisedInputMask);
        assertTrue(deserialisedInputMask instanceof TupleMask);
    }
}
