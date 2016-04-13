package gaffer.rest.userrole;

import java.util.Arrays;
import java.util.Collection;

public class UserRoleLookupForTest implements UserRoleLookup {
    private static Collection<String> userRoles = Arrays.asList("User", "ReadUser");

    @Override
    public boolean isUserInRole(final String role) {
        return userRoles.contains(role);
    }

    public static void setUserRoles(final String... userRoles) {
        UserRoleLookupForTest.userRoles = Arrays.asList(userRoles);
    }
}
