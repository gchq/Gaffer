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

public class TupleBiFunctionTest {
    //TODO:add tests
//    @Test
//    public void testTupleCombination() {
//        String[] inputs = new String[]{"input1", "input2", "input3"};
//        Set<String> output1 = new HashSet<>(Arrays.asList("input1"));
//        Set<String> output2 = new HashSet<>(Arrays.asList("input1", "input2"));
//        Set<String> output3 = new HashSet<>(Arrays.asList("input1", "input2", "input3"));
//        List<Set<String>> outputsArray = new ArrayList<>();
//        outputsArray.addAll(Arrays.asList(output1, output2, output3));
//        Object[] outputs = outputsArray.toArray();
//
//        TupleBiFunction<String, String, Set<String>> combiner = new TupleBiFunction<>();
//        Tuple<String>[] tuples = new Tuple[]{mock(Tuple.class), mock(Tuple.class), mock(Tuple.class)};
//
//        // set up the function
//        BiFunction<String, Set<String>, Set<String>> function1 = mock(BiFunction.class);
//        TupleAdapter<String, String> inputMask = mock(TupleAdapter.class);
//        TupleAdapter<String, Set<String>> outputMask = mock(TupleAdapter.class);
//        combiner.setFunction(function1);
//        combiner.setSelection(inputMask);
//        combiner.setProjection(outputMask);
//        Tuple<String> state = null;
//        for (int i = 0; i < tuples.length; i++) {
//            Set<String> previousOutput = null;
//            given(inputMask.select(tuples[i])).willReturn(inputs[i]);
//            if (i > 0) {
//                previousOutput = (Set<String>) outputs[i - 1];
//                given(outputMask.select(state)).willReturn(previousOutput);
//            }
//            given(function1.apply(inputs[i], previousOutput)).willReturn((Set<String>) outputs[i]);
//            given(outputMask.project(tuples[i], outputs[i])).willReturn(tuples[0]);
//            state = combiner.apply(tuples[i], state);
//        }
//
//        // check the expected calls
//        verify(outputMask, times(tuples.length - 1)).select(tuples[0]);
//        for (int i = 0; i < tuples.length; i++) {
//            String in1 = inputs[i];
//            Set<String> in2 = null;
//            if (i > 0) {
//                in2 = (Set<String>) outputs[i - 1];
//
//            }
//            verify(inputMask, times(1)).select(tuples[i]);
//            verify(function1, times(1)).apply(in1, in2);
//            verify(outputMask, times(1)).project(tuples[i], outputs[i]);
//        }
//    }
//
//    @Test
//    public void shouldJsonSerialiseAndDeserialise() throws IOException {
//        TupleBiFunction<String, String, Set<String>> combiner = new TupleBiFunction<>();
//        MockBiFunction function = new MockBiFunction();
//        TupleAdapter<String, String> inputMask = new TupleAdapter<>("a");
//        TupleAdapter<String, Set<String>> outputMask = new TupleAdapter<>("b");
//        combiner.setSelection(inputMask);
//        combiner.setProjection(outputMask);
//        combiner.setFunction(function);
//
//        String json = JsonSerialiser.serialise(combiner);
//        TupleBiFunction<String, String, Set<String>> deserialisedBiFunction = JsonSerialiser.deserialise(json, TupleBiFunction.class);
//
//        // check deserialisation
//        assertNotSame(combiner, deserialisedBiFunction);
//
//        BiFunction<String, Set<String>, Set<String>> deserialisedFunction = deserialisedBiFunction.getFunction();
//        assertNotSame(function, deserialisedFunction);
//        assertTrue(deserialisedFunction instanceof MockBiFunction);
//
//        TupleAdapter<String, String> deserialisedInputMask = deserialisedBiFunction.getSelection();
//        assertNotSame(inputMask, deserialisedInputMask);
//        assertTrue(deserialisedInputMask instanceof TupleAdapter);
//        TupleAdapter<String, Set<String>> deserialisedOutputMask = deserialisedBiFunction.getProjection();
//        assertNotSame(outputMask, deserialisedOutputMask);
//        assertTrue(deserialisedOutputMask instanceof TupleAdapter);
//    }
}
