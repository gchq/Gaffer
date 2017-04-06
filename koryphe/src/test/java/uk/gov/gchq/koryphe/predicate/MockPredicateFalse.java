package uk.gov.gchq.koryphe.predicate;

public class MockPredicateFalse extends KoryphePredicate<Double> {
    @Override
    public boolean test(final Double input) {
        return false;
    }
}
