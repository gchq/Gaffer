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

package uk.gov.gchq.gaffer.rest;

/**
 * System property keys and default values.
 */
public abstract class SystemProperty {
    // KEYS
    public static final String SCHEMA_PATHS = "uk.gov.gchq.gaffer.schemas";
    public static final String STORE_PROPERTIES_PATH = "uk.gov.gchq.gaffer.storeProperties";
    public static final String BASE_URL = "uk.gov.gchq.gaffer.rest-api.basePath";
    public static final String VERSION = "uk.gov.gchq.gaffer.rest-api.version";
    public static final String GRAPH_FACTORY_CLASS = "uk.gov.gchq.gaffer.graph.factory.class";
    public static final String SERVICES_PACKAGE_PREFIX = "uk.gov.gchq.gaffer.rest-api.resourcePackage";
    public static final String PACKAGE_PREFIXES = "uk.gov.gchq.gaffer.package.prefixes";
    public static final String OP_AUTHS_PATH = "uk.gov.gchq.gaffer.operation.auths.path";

    // DEFAULTS
    /**
     * Comma separated list of package prefixes to search for {@link gaffer.function.Function}s and {@link gaffer.operation.Operation}s.
     */
    public static final String PACKAGE_PREFIXES_DEFAULT = "gaffer";
    public static final String SERVICES_PACKAGE_PREFIX_DEFAULT = "uk.gov.gchq.gaffer.rest";
    public static final String BASE_URL_DEFAULT = "rest/v1";
    public static final String CORE_VERSION = "1.0.0";
    public static final String GRAPH_FACTORY_CLASS_DEFAULT = GraphFactory.class.getName();
}
