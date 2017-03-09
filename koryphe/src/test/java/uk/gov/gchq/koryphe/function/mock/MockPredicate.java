package uk.gov.gchq.koryphe.function.mock;

import java.util.function.Predicate;

public class MockPredicate implements Predicate<Object> {
    @Override
    public boolean test(Object input) {
        return true;
    }
}
