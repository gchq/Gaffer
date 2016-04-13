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

import gaffer.operation.Operation;
import gaffer.operation.OperationChain;
import gaffer.rest.SystemProperty;
import gaffer.rest.service.SimpleGraphConfigurationService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Checks user roles against operation roles to ensure a user is allowed to
 * execute provided operations. The operation role mappings are fetched from system
 * properties
 */
public class UserRoleHelper {
    private static final int ROLES_SUFFIX_LENGTH = SystemProperty.OPERATION_ROLES_SUFFIX.length();

    private final UserRoleLookup userRoleLookup;
    private final boolean rolesEnabled;
    private final Map<Class<? extends Operation>, Set<String>> roleMapping;

    public UserRoleHelper() {
        userRoleLookup = fetchUserRoleService();
        rolesEnabled = null != userRoleLookup;
        roleMapping = fetchRoleMapping();
    }

    /**
     * Checks all {@link Operation}s in the provided {@link OperationChain} are
     * allowed to be executed by the user. This is done by checking the user's roles
     * against the operation roles.
     * If an operation cannot be executed then an {@link IllegalAccessError} is thrown.
     *
     * @param opChain the operation chain.
     * @see UserRoleHelper#checkRoles(Operation)
     */
    public void checkRoles(final OperationChain<?> opChain) {
        if (rolesEnabled && null != opChain) {
            for (Operation operation : opChain.getOperations()) {
                checkRoles(operation);
            }
        }
    }

    /**
     * Checks all provided {@link Operation}s are allowed to be executed by the
     * user. This is done by checking the user's roles against the operation roles.
     * If an operation cannot be executed then an {@link IllegalAccessError} is thrown.
     *
     * @param operation the operation.
     */
    public void checkRoles(final Operation operation) {
        if (rolesEnabled && null != operation) {
            final Set<String> allowedRoles = roleMapping.get(operation.getClass());
            boolean hasRole = false;
            if (null == allowedRoles) {
                throw new IllegalArgumentException(operation.getClass().getName() + " was not recognised. Please ensure the operation package is registered in the REST API properties.");
            }

            for (String allowedRole : allowedRoles) {
                if (userRoleLookup.isUserInRole(allowedRole)) {
                    hasRole = true;
                    break;
                }
            }

            if (!hasRole) {
                throw new IllegalAccessError("User does not have permission to run operation: "
                        + operation.getClass().getName());
            }
        }
    }

    private Map<Class<? extends Operation>, Set<String>> fetchRoleMapping() {
        final Map<Class<? extends Operation>, Set<String>> allOpRoles = new HashMap<>();
        if (rolesEnabled) {
            final Map<Class<? extends Operation>, Set<String>> sysPropsRoles = new HashMap<>();

            for (String propName : System.getProperties().stringPropertyNames()) {
                if (propName.endsWith(SystemProperty.OPERATION_ROLES_SUFFIX)) {
                    final String opClassName = propName.substring(0, propName.length() - ROLES_SUFFIX_LENGTH);
                    try {
                        final Class<? extends Operation> opClazz = Class.forName(opClassName).asSubclass(Operation.class);
                        final Set<String> roles = new HashSet<>();
                        for (String role : System.getProperty(propName).split(",")) {
                            if (null != role && !"".equals(role)) {
                                roles.add(role);
                            }
                        }
                        sysPropsRoles.put(opClazz, roles);
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            // Load all available operations
            final SimpleGraphConfigurationService graphService = new SimpleGraphConfigurationService();
            for (Class<? extends Operation> opClass : graphService.getOperations()) {
                Set<String> roles = allOpRoles.get(opClass);
                if (null == roles) {
                    roles = new HashSet<>();
                    allOpRoles.put(opClass, roles);
                }

                // Check to find all implementations that have roles defined in system properties
                for (Entry<Class<? extends Operation>, Set<String>> entry : sysPropsRoles.entrySet()) {
                    if (entry.getKey().isAssignableFrom(opClass)) {
                        roles.addAll(entry.getValue());
                    }
                }
            }
        }

        return allOpRoles;
    }

    private UserRoleLookup fetchUserRoleService() {
        final String userRoleProviderClassName = System.getProperty(SystemProperty.USER_ROLE_LOOKUP_CLASS_NAME);
        final UserRoleLookup service;
        if (null != userRoleProviderClassName) {
            try {
                service = Class.forName(userRoleProviderClassName).asSubclass(UserRoleLookup.class).newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(userRoleProviderClassName + " class could not be found", e);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException("Could not create and instance of " + userRoleProviderClassName + " using a default constructor", e);
            }
        } else {
            service = null;
        }
        return service;
    }
}
