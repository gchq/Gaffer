package koryphe.function.mock;

import koryphe.function.validate.Validator;

public class MockValidator implements Validator<Object> {
    @Override
    public Boolean execute(Object input) {
        return true;
    }
}
