package uk.gov.gchq.koryphe.predicate;

import uk.gov.gchq.koryphe.tuple.predicate.KoryphePredicate2;

public class MockPredicate2True extends KoryphePredicate2<Double, Integer> {
    @Override
    public boolean test(final Double d, final Integer i) {
        return false;
    }
}
