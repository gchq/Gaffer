package uk.gov.gchq.gaffer.rest.factory;

import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import static org.mockito.Mockito.mock;

public class MockUserFactory implements UserFactory {

    final User user = mock(User.class);

    @Override
    public User createUser() {
        return user;
    }

    @Override
    public Context createContext() {
        return new Context(user);
    }
}
