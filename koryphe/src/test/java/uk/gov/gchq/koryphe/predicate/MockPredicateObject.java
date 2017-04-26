package uk.gov.gchq.koryphe.predicate;

public class MockPredicateObject extends KoryphePredicate<Object> {
    @Override
    public boolean test(final Object input) {
        return true;
    }
}
