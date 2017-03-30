package uk.gov.gchq.koryphe.predicate;

public class MockPredicateTrue extends KoryphePredicate<Double> {
    @Override
    public boolean test(final Double input) {
        return true;
    }
}
