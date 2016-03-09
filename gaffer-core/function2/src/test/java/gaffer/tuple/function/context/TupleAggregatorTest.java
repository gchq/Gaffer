package gaffer.tuple.function.context;

import gaffer.function2.Aggregator;
import gaffer.function2.FunctionTest;
import gaffer.tuple.MapTuple;
import gaffer.tuple.function.TupleAggregator;
import gaffer.tuple.tuplen.Tuple2;
import gaffer.tuple.tuplen.Tuple3;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TupleAggregatorTest extends FunctionTest {
    @Test
    public void testSingleInputAggregation() {
        TupleAggregator<Aggregator, String> tupleAggregator = new TupleAggregator<>();
        FunctionContext<Aggregator, String> aggregatorContext = new FunctionContext<>();
        aggregatorContext.setSelection(new String[]{"a"});
        aggregatorContext.setProjection(new String[]{"b"});
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
        aggregatorContext.setSelection(new String[]{"a", "b"});
        aggregatorContext.setProjection(new String[]{"c", "d"});
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

    private class MockSingleInputAggregator extends Aggregator<Integer> {
        private int total = 0;

        @Override
        public void aggregate(Integer input) {
            total += input;
        }

        @Override
        public void init() {
            total = 0;
        }

        @Override
        public Integer state() {
            return total;
        }

        @Override
        public MockSingleInputAggregator copy() {
            return new MockSingleInputAggregator();
        }
    }

    private class MockMultiInputAggregator extends Aggregator<Tuple2<Integer, Integer>> {
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

    private class MockComplexInputAggregator extends Aggregator<Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>> {
        Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> last;

        @Override
        public void aggregate(Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> input) {
            last = input;
        }

        @Override
        public void init() {
            last = null;
        }

        @Override
        public Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>> state() {
            return last;
        }

        @Override
        public MockComplexInputAggregator copy() {
            return new MockComplexInputAggregator();
        }
    }
}
