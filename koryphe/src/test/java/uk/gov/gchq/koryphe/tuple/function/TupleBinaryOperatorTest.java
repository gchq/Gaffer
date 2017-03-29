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

package uk.gov.gchq.koryphe.tuple.function;

import org.junit.Test;
import uk.gov.gchq.koryphe.binaryoperator.MockBinaryOperator;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleOutputAdapter;
import uk.gov.gchq.koryphe.tuple.TupleReverseOutputAdapter;
import uk.gov.gchq.koryphe.tuple.binaryoperator.TupleAdaptedBinaryOperator;
import uk.gov.gchq.koryphe.util.JsonSerialiser;
import java.io.IOException;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TupleBinaryOperatorTest {

    @Test
    public void testTupleAggregation() {
        String[] inputs = new String[]{"input1", "input2", "input3"};
        String[] outputs = new String[]{"output1", "output2", "output3"};

        TupleAdaptedBinaryOperator<String, String> binaryOperator = new TupleAdaptedBinaryOperator<>();
        Tuple<String>[] tuples = new Tuple[]{mock(Tuple.class), mock(Tuple.class), mock(Tuple.class)};

        // set up the function
        BinaryOperator<String> function = mock(BinaryOperator.class);
        TupleInputAdapter<String, String> inputAdapter = mock(TupleInputAdapter.class);
        TupleOutputAdapter<String, String> outputAdapter = mock(TupleOutputAdapter.class);
        TupleReverseOutputAdapter<String, String> reverseOutputAdapter = mock(TupleReverseOutputAdapter.class);

        binaryOperator.setFunction(function);
        binaryOperator.setInputAdapter(inputAdapter);
        binaryOperator.setReverseOutputAdapter(reverseOutputAdapter);
        binaryOperator.setOutputAdapter(outputAdapter);

        Tuple<String> state = null;

        for (int i = 0; i < tuples.length; i++) {
            String previousOutput = null;
            given(inputAdapter.apply(tuples[i])).willReturn(inputs[i]);
            if (i > 0) {
                previousOutput = outputs[i - 1];
                given(inputAdapter.apply(state)).willReturn(previousOutput);
            }

            if (i > 1) {
                given(outputAdapter.apply(outputs[i], tuples[i-1])).willReturn(tuples[0]);
            }
            given(inputAdapter.apply(tuples[i])).willReturn(inputs[i]);

            given(reverseOutputAdapter.apply(null)).willReturn(previousOutput);
            given(reverseOutputAdapter.apply(tuples[i])).willReturn(outputs[i]);
            given(function.apply(inputs[i], previousOutput)).willReturn(outputs[i]);

            given(outputAdapter.apply(outputs[i], null)).willReturn(tuples[0]);

            state = binaryOperator.apply(tuples[i], state);
        }

        // check the expected calls
        for (int i = 0; i < tuples.length; i++) {
            verify(inputAdapter, times(1)).apply(tuples[i]);
            String in1 = inputs[i];
            String in2 = null;
            if (i > 0) {
                in2 = outputs[i - 1];
                verify(inputAdapter, times(1)).apply(tuples[i]);
            }
            verify(function, times(1)).apply(in1, in2);
            verify(inputAdapter, times(1)).apply(tuples[i]);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleAdaptedBinaryOperator<String, Integer> binaryOperator = new TupleAdaptedBinaryOperator<>();
        MockBinaryOperator function = new MockBinaryOperator();
        TupleInputAdapter<String, Integer> inputAdapter = new TupleInputAdapter();
        binaryOperator.setInputAdapter(inputAdapter);
        binaryOperator.setFunction(function);

        String json = JsonSerialiser.serialise(binaryOperator);
        TupleAdaptedBinaryOperator<String, Integer> deserialisedBinaryOperator = JsonSerialiser.deserialise(json, TupleAdaptedBinaryOperator.class);

        // check deserialisation
        assertNotSame(binaryOperator, deserialisedBinaryOperator);

        BinaryOperator<Integer> deserialisedFunction = deserialisedBinaryOperator.getFunction();
        assertNotSame(function, deserialisedFunction);
        assertTrue(deserialisedFunction instanceof MockBinaryOperator);

        Function<Tuple<String>, Integer> deserialisedInputMask = deserialisedBinaryOperator.getInputAdapter();
        assertNotSame(inputAdapter, deserialisedInputMask);
        assertTrue(deserialisedInputMask instanceof Function);
    }
}
