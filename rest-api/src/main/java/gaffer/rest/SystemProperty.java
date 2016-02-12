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
    public static final String SCHEMA_TYPES_PATH = "gaffer.schemaTypes";
    public static final String DATA_SCHEMA_PATH = "gaffer.dataSchema";
    public static final String STORE_SCHEMA_PATH = "gaffer.storeSchema";
    public static final String STORE_PROPERTIES_PATH = "gaffer.storeProperties";
    public static final String BASE_URL = "gaffer.rest-api.basePath";
    public static final String VERSION = "gaffer.rest-api.version";
    public static final String SERVICES_PACKAGE_PREFIX = "gaffer.rest-api.resourcePackage";
    public static final String PACKAGE_PREFIXES = "gaffer.package.prefixes";

    // DEFAULTS
    /**
     * Comma separated list of package prefixes to search for {@link gaffer.function.Function}s and {@link gaffer.operation.Operation}s.
     */
    public static final String PACKAGE_PREFIXES_DEFAULT = "gaffer";
    public static final String SERVICES_PACKAGE_PREFIX_DEFAULT = "gaffer.rest";
    public static final String BASE_URL_DEFAULT = "gaffer/rest/v1";
    public static final String CORE_VERSION = "1.0.0";
}
