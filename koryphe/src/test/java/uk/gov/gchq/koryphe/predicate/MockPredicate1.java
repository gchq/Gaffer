package uk.gov.gchq.koryphe.predicate;

public class MockPredicate1 extends KoryphePredicate<Double> {
    @Override
    public boolean test(final Double aDouble) {
        return false;
    }
}
