package gaffer.tuple.function;

import gaffer.function2.Aggregator;
import gaffer.function2.FunctionTest;
import gaffer.function2.StatefulFunction;
import gaffer.function2.mock.MockComplexInputAggregator;
import gaffer.function2.mock.MockMultiInputAggregator;
import gaffer.function2.mock.MockSingleInputAggregator;
import gaffer.tuple.MapTuple;
import gaffer.tuple.function.context.FunctionContext;
import gaffer.tuple.handler.TupleView;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TupleAggregatorTest extends FunctionTest {
    @Test
    public void testSingleInputAggregation() {
        TupleAggregator<Aggregator, String> tupleAggregator = new TupleAggregator<>();
        FunctionContext<Aggregator, String> aggregatorContext = new FunctionContext<>();
        aggregatorContext.setSelectionView(new TupleView<String>().addHandler("a"));
        aggregatorContext.setProjectionView(new TupleView<String>().addHandler("b"));
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
        aggregatorContext.setSelectionView(new TupleView<String>().addHandler("a", "b"));
        aggregatorContext.setProjectionView(new TupleView<String>().addHandler("c", "d"));
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
        List<List<String>> selection = Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c"), Arrays.asList("d", "e", "f"));
        List<List<String>> projection = Arrays.asList(Arrays.asList("g", "h"), Arrays.asList("i"), Arrays.asList("j", "k", "l"));
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
        aggregator.addFunction(new TupleView<String>().addHandler("a"), function, new TupleView<String>().addHandler("b"));

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
        aggregator.addFunction(new TupleView<String>().addHandler("a"), function, new TupleView<String>().addHandler("b"));

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
}
