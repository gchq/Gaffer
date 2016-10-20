package koryphe.function.mock;

import koryphe.function.stateless.validator.Validator;

public class MockValidator implements Validator<Object> {

    @Override
    public Boolean execute(Object input) {
        return true;
    }
}
