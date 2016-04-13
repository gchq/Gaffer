/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gaffer.rest.userrole;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import gaffer.commonutil.StreamUtil;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.operation.impl.get.GetRelatedElements;
import gaffer.rest.SystemProperty;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;


public class UserRoleHelperTest {
    @Before
    public void setup() throws IOException {
        System.getProperties().load(StreamUtil.openStream(getClass(), "/rest.properties"));
        System.getProperties().load(StreamUtil.openStream(getClass(), "/roles.properties"));
    }

    @Test
    public void shouldAcceptOperationChainWhenUserHasAllowedRoles() {
        // Given
        final UserRoleHelper userRoleHelper = new UserRoleHelper();
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetRelatedElements())
                .build();

        UserRoleLookupForTest.setUserRoles("ReadUser", "WriteUser");

        // When
        userRoleHelper.checkRoles(opChain);

        // Then - no exceptions
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllowedRolesForAllOperations() {
        // Given
        final UserRoleHelper userRoleHelper = new UserRoleHelper();
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GenerateObjects()) // Requires AdminUser
                .build();

        UserRoleLookupForTest.setUserRoles("ReadUser", "WriteUser");

        // When/Then
        try {
            userRoleHelper.checkRoles(opChain);
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAnyRoles() throws OperationException {
        // Given
        final UserRoleHelper userRoleHelper = new UserRoleHelper();
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetRelatedElements())
                .build();

        UserRoleLookupForTest.setUserRoles();

        // When/Then
        try {
            userRoleHelper.checkRoles(opChain);
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldRejectOperationChainWhenUserDoesntHaveAllowedRole() throws OperationException {
        // Given
        final UserRoleHelper userRoleHelper = new UserRoleHelper();
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetRelatedElements())
                .build();

        UserRoleLookupForTest.setUserRoles("unknownUser");

        // When/Then
        try {
            userRoleHelper.checkRoles(opChain);
            fail("Exception expected");
        } catch (final IllegalAccessError e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldAcceptOperationChainWhenRolesIsDisabled() {
        // Given
        System.clearProperty(SystemProperty.USER_ROLE_LOOKUP_CLASS_NAME);
        final UserRoleHelper userRoleHelper = new UserRoleHelper();
        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAdjacentEntitySeeds())
                .then(new GetRelatedElements())
                .build();

        // Remove all user roles
        UserRoleLookupForTest.setUserRoles();

        // When
        userRoleHelper.checkRoles(opChain);

        // Then - no exceptions
    }
}
