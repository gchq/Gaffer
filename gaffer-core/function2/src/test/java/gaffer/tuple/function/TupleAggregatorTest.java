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

package gaffer.tuple.function;

import gaffer.function2.Aggregator;
import gaffer.function2.FunctionTest;
import gaffer.function2.StatefulFunction;
import gaffer.function2.mock.MockComplexInputAggregator;
import gaffer.function2.mock.MockMultiInputAggregator;
import gaffer.function2.mock.MockSingleInputAggregator;
import gaffer.function2.signature.IterableSignature;
import gaffer.function2.signature.Signature;
import gaffer.function2.signature.SingletonSignature;
import gaffer.tuple.MapTuple;
import gaffer.tuple.Tuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.view.Reference;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TupleAggregatorTest extends FunctionTest {
    @Test
    public void testSingleInputAggregation() {
        TupleAggregator<Aggregator, String> tupleAggregator = new TupleAggregator<>();
        FunctionContext<Aggregator, String> aggregatorContext = new FunctionContext<>();
        aggregatorContext.setSelection(new Reference("a"));
        aggregatorContext.setProjection(new Reference("b"));
        aggregatorContext.setFunction(new MockSingleInputAggregator());
        tupleAggregator.addFunction(aggregatorContext);

        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("a", 1);

        int executions = 5;
        for (int i = 0; i < executions; i++) {
            tupleAggregator.aggregate(tuple);
        }
        tupleAggregator.state();

        assertEquals("Did not produce expected output at reference b", 5, tuple.get("b"));
    }

    @Test
    public void testMultiInputAggregation() {
        TupleAggregator<Aggregator, String> tupleAggregator = new TupleAggregator<>();
        FunctionContext<Aggregator, String> aggregatorContext = new FunctionContext<>();
        aggregatorContext.setSelection(new Reference("a", "b"));
        aggregatorContext.setProjection(new Reference("c", "d"));
        aggregatorContext.setFunction(new MockMultiInputAggregator());
        tupleAggregator.addFunction(aggregatorContext);

        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("a", 1);
        tuple.put("b", 2);

        int executions = 5;
        for (int i = 0; i < executions; i++) {
            tupleAggregator.aggregate(tuple);
        }
        tupleAggregator.state();

        assertEquals("Did not produce expected output at reference c", executions, tuple.get("c"));
        assertEquals("Did not produce expected output at reference d", executions * 2, tuple.get("d"));
    }

    @Test
    public void testComplexInputAggregation() {
        TupleAggregator<Aggregator, String> tupleAggregator = new TupleAggregator<>();
        FunctionContext<Aggregator, String> aggregatorContext = new FunctionContext<>();
        Reference<String> selection1 = new Reference("a", "b");
        Reference<String> selection2 = new Reference("c");
        Reference<String> selection3 = new Reference("d", "e", "f");
        Reference<String> selection = new Reference<>();
        selection.setTupleReferences(selection1, selection2, selection3);
        Reference<String> projection1 = new Reference("g", "h");
        Reference<String> projection2 = new Reference("i");
        Reference<String> projection3 = new Reference("j", "k", "l");
        Reference<String> projection = new Reference<>();
        projection.setTupleReferences(projection1, projection2, projection3);

        aggregatorContext.setSelection(selection);
        aggregatorContext.setProjection(projection);
        aggregatorContext.setFunction(new MockComplexInputAggregator());
        tupleAggregator.addFunction(aggregatorContext);

        Integer a = 1;
        String b = "b";
        Integer c = 3;
        String d = "d";
        String e = "e";
        String f = "f";

        MapTuple<String> tuple = new MapTuple<>();
        tuple.put("a", a);
        tuple.put("b", b);
        tuple.put("c", c);
        tuple.put("d", d);
        tuple.put("e", e);
        tuple.put("f", f);

        int executions = 2;
        for (int i = 0; i < executions; i++) {
            tupleAggregator.aggregate(tuple);
        }
        tupleAggregator.state();

        // check aggregated fields
        assertEquals("Did not produce expected output at reference g", executions * a, tuple.get("g"));
        assertEquals("Did not produce expected output at reference h", b + b, tuple.get("h"));
        assertEquals("Did not produce expected output at reference i", executions * c, tuple.get("i"));
        assertEquals("Did not produce expected output at reference j", d + d, tuple.get("j"));
        assertEquals("Did not produce expected output at reference k", e + e, tuple.get("k"));
        assertEquals("Did not produce expected output at reference d", f + f, tuple.get("l"));

        // check original fields are untouched
        assertEquals("Did not produce expected output at reference a", a, tuple.get("a"));
        assertEquals("Did not produce expected output at reference b", b, tuple.get("b"));
        assertEquals("Did not produce expected output at reference c", c, tuple.get("c"));
        assertEquals("Did not produce expected output at reference d", d, tuple.get("d"));
        assertEquals("Did not produce expected output at reference e", e, tuple.get("e"));
        assertEquals("Did not produce expected output at reference f", f, tuple.get("f"));
    }

    @Test
    public void shouldCopy() {
        TupleAggregator<Aggregator, String> aggregator = new TupleAggregator();
        MockSingleInputAggregator function = new MockSingleInputAggregator();
        FunctionContext<Aggregator, String> context = new FunctionContext<Aggregator, String>(new Reference("a"), function, new Reference("b"));
        aggregator.addFunction(context);

        TupleAggregator aggregatorCopy = aggregator.copy();
        List<FunctionContext<Aggregator, String>> functionsCopy = aggregatorCopy.getFunctions();

        assertEquals("Unexpected number of functions in copy", 1, functionsCopy.size());
        Aggregator functionCopy = functionsCopy.get(0).getFunction();
        assertNotSame(aggregator, aggregatorCopy);
        assertNotSame(function, functionCopy);
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        TupleAggregator<Aggregator, String> aggregator = new TupleAggregator();
        MockSingleInputAggregator function = new MockSingleInputAggregator();
        FunctionContext<Aggregator, String> context = new FunctionContext<Aggregator, String>(new Reference("a"), function, new Reference("b"));
        aggregator.addFunction(context);

        String json = serialise(aggregator);
        TupleAggregator<Aggregator, String> deserialisedAggregator = deserialise(json, TupleAggregator.class);

        // check deserialisation
        assertNotNull(deserialisedAggregator);
        List<FunctionContext<Aggregator, String>> functions = deserialisedAggregator.getFunctions();
        assertEquals("Unexpected number of functions in copy", 1, functions.size());
        StatefulFunction deserialisedFunction = functions.get(0).getFunction();
        assertTrue(deserialisedFunction instanceof MockSingleInputAggregator);
        assertNotSame(aggregator, deserialisedAggregator);
        assertNotSame(function, deserialisedFunction);
    }

    @Test
    public void shouldValidateInputAndOutput() {
        TupleAggregator<Aggregator, String> aggregator = new TupleAggregator<>();
        MockMultiInputAggregator function = new MockMultiInputAggregator();
        FunctionContext<Aggregator, String> context = new FunctionContext<>(new Reference("a", "b"), function, new Reference("d", "e"));
        aggregator.addFunction(context);
        MockSingleInputAggregator function2 = new MockSingleInputAggregator();
        context = new FunctionContext<>(new Reference("c"), function2, new Reference("f"));
        aggregator.addFunction(context);

        MapTuple<String> mapTuple = new MapTuple<>();
        mapTuple.put("a", Integer.class);
        mapTuple.put("b", Integer.class);
        mapTuple.put("c", Integer.class);
        mapTuple.put("d", Integer.class);
        mapTuple.put("e", Integer.class);
        mapTuple.put("f", Integer.class);

        Signature inputSignature = aggregator.getInputSignature(); //Tuple is an IterableSignature, not a TupleSignature
        assertTrue(inputSignature instanceof IterableSignature);
        Signature outputSignature = aggregator.getOutputSignature();
        assertTrue(outputSignature instanceof IterableSignature);

        assertTrue(aggregator.assignableFrom(mapTuple));
        assertTrue(aggregator.assignableTo(mapTuple));

        mapTuple.put("b", String.class);
        assertFalse(aggregator.assignableFrom(mapTuple));
        assertTrue(aggregator.assignableTo(mapTuple));

        mapTuple.put("d", String.class);
        assertFalse(aggregator.assignableFrom(mapTuple));
        assertFalse(aggregator.assignableTo(mapTuple));
    }
}
