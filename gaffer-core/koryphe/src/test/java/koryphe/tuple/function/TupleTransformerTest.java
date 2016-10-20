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
import koryphe.function.stateless.StatelessFunction;
import koryphe.function.mock.MockTransformer;
import koryphe.function.stateless.transformer.Transformer;
import koryphe.tuple.Tuple;
import koryphe.tuple.tuplen.Tuple2;
import koryphe.tuple.tuplen.value.Value2;
import koryphe.tuple.adapter.TupleAdapter;
import org.junit.Test;
import util.JsonSerialiser;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TupleTransformerTest {
    @Test
    public void testSingleFunctionTransformation() {
        String input1 = "input1";
        String output1a = "output1a";
        String output1b = "output1b";
        Tuple2<String, String> output1 = new Value2<>(output1a, output1b);

        Tuple<String> tuple = mock(Tuple.class);

        // set up a function
        TupleTransformer<String, String, Tuple2<String, String>> transformer = new TupleTransformer<>();
        Transformer<String, Tuple2<String, String>> function1 = mock(Transformer.class);
        TupleAdapter<String, String> inputAdapter = mock(TupleAdapter.class);
        TupleAdapter<String, Tuple2<String, String>> outputAdapter = mock(TupleAdapter.class);
        given(inputAdapter.from(tuple)).willReturn(input1);
        given(function1.execute(input1)).willReturn(output1);
        transformer.setFunction(function1);
        transformer.setInputAdapter(inputAdapter);
        transformer.setOutputAdapter(outputAdapter);

        // execute it
        transformer.execute(tuple);

        // check it was called as expected
        verify(inputAdapter, times(1)).from(tuple);
        verify(function1, times(1)).execute(input1);
        verify(outputAdapter, times(1)).setContext(tuple);
        verify(outputAdapter, times(1)).to(output1);
    }

    @Test
    public void testMultiTupleTransformation() {
        String input = "input";
        String output = "output";

        TupleTransformer<String, String, String> transformer = new TupleTransformer<>();

        //create some tuples
        int times = 5;
        Tuple<String>[] tuples = new Tuple[times];
        for (int i = 0; i < times; i++) {
            tuples[i] = mock(Tuple.class);
        }

        // set up a function to transform the tuples
        Transformer<String, String> function1 = mock(Transformer.class);
        TupleAdapter<String, String> inputAdapter = mock(TupleAdapter.class);
        TupleAdapter<String, String> outputAdapter = mock(TupleAdapter.class);
        transformer.setFunction(function1);
        transformer.setInputAdapter(inputAdapter);
        transformer.setOutputAdapter(outputAdapter);
        for (int i = 0; i < times; i++) {
            given(inputAdapter.from(tuples[i])).willReturn(input + i);
            given(function1.execute(input + i)).willReturn(output + i);
        }

        // execute transformations
        for (int i = 0; i < times; i++) {
            transformer.execute(tuples[i]);
        }

        // check expected calls
        for (int i = 0; i < times; i++) {
            verify(inputAdapter, times(1)).from(tuples[i]);
            verify(function1, times(1)).execute(input + i);
            verify(outputAdapter, times(1)).setContext(tuples[i]);
            verify(outputAdapter, times(1)).to(output + i);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleTransformer<String, Object, Object> transformer = new TupleTransformer<>();
        MockTransformer function = new MockTransformer();
        transformer.setFunction(function);
        TupleAdapter<String, Object> inputAdapter = new TupleAdapter<>("a");
        TupleAdapter<String, Object> outputAdapter = new TupleAdapter<>("b");
        transformer.setInputAdapter(inputAdapter);
        transformer.setOutputAdapter(outputAdapter);

        String json = JsonSerialiser.serialise(transformer);
        TupleTransformer<String, Object, Object> deserialisedTransformer = JsonSerialiser.deserialise(json, TupleTransformer.class);
        assertNotSame(transformer, deserialisedTransformer);

        Transformer<Object, Object> functionCopy = deserialisedTransformer.getFunction();
        assertNotSame(function, functionCopy);
        assertTrue(functionCopy instanceof MockTransformer);

        Adapter<Tuple<String>, Object> inputAdapterCopy = deserialisedTransformer.getInputAdapter();
        Adapter<Tuple<String>, Object> outputAdapterCopy = deserialisedTransformer.getOutputAdapter();
        assertNotSame(inputAdapter, inputAdapterCopy);
        assertTrue(inputAdapterCopy instanceof TupleAdapter);
        assertNotSame(outputAdapter, outputAdapterCopy);
        assertTrue(outputAdapterCopy instanceof TupleAdapter);
    }
}
