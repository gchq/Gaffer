package uk.gov.gchq.koryphe.predicate;

import java.util.function.Predicate;

public class MockPredicate implements Predicate<Double> {
    @Override
    public boolean test(Double input) {
        return true;
    }
}
