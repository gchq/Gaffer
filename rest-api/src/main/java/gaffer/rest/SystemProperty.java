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

package gaffer.rest;

/**
 * System property keys and default values.
 */
public abstract class SystemProperty {
    // KEYS
    /**
     * Comma separated list of paths to {@link gaffer.store.schema.Schema} files.
     */
    public static final String SCHEMA_PATHS = "gaffer.schemas";
    public static final String STORE_PROPERTIES_PATH = "gaffer.storeProperties";
    public static final String BASE_URL = "gaffer.rest-api.basePath";
    public static final String VERSION = "gaffer.rest-api.version";

    /**
     * The package prefix to scan where the REST API service classes belong.
     */
    public static final String SERVICES_PACKAGE_PREFIX = "gaffer.rest-api.resourcePackage";

    /**
     * If you wish to include any {@link gaffer.operation.Operation} and
     * {@link gaffer.function.Function} classes that have a different prefix to
     * 'gaffer', use this property to set a comma separated list of package prefixes
     * to scan.
     */
    public static final String PACKAGE_PREFIXES = "gaffer.package.prefixes";

    /**
     * To limit execution of operations based on user roles, use this property to set a user role provider class.
     * You will also need to set the operation role mappings - see {@link SystemProperty#OPERATION_ROLES_SUFFIX}
     *
     * @see gaffer.rest.userrole.UserRoleLookup
     */
    public static final String USER_ROLE_LOOKUP_CLASS_NAME = "gaffer.user.role.lookup.class.name";

    /**
     * To limit execution of operations based on user roles, use this property suffix to set the operation role mappings.
     * There should be a list of operation class names suffixed with .role as property keys and the values are a comma separated list of role names.
     * You will also need to set the operation role mappings - see {@link SystemProperty#USER_ROLE_LOOKUP_CLASS_NAME}
     *
     * @see gaffer.rest.userrole.UserRoleLookup
     */
    public static final String OPERATION_ROLES_SUFFIX = ".roles";

    // DEFAULTS
    public static final String PACKAGE_PREFIXES_DEFAULT = "gaffer";
    public static final String SERVICES_PACKAGE_PREFIX_DEFAULT = "gaffer.rest";
    public static final String BASE_URL_DEFAULT = "gaffer/rest/v1";
    public static final String CORE_VERSION = "1.0.0";
}
