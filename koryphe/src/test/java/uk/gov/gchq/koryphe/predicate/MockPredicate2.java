package uk.gov.gchq.koryphe.predicate;

import uk.gov.gchq.koryphe.tuple.predicate.KoryphePredicate2;

public class MockPredicate2 extends KoryphePredicate2<Double, Integer> {
    @Override
    public boolean test(final Double aDouble, final Integer integer) {
        return false;
    }
}
