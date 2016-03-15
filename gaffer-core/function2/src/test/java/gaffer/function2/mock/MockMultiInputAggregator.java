package gaffer.function2.mock;

import gaffer.function2.Aggregator;
import gaffer.tuple.tuplen.Tuple2;

public class MockMultiInputAggregator extends Aggregator<Tuple2<Integer, Integer>> {
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
