package gaffer.function2.mock;

import gaffer.function2.Aggregator;

public class MockSingleInputAggregator extends Aggregator<Integer> {
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