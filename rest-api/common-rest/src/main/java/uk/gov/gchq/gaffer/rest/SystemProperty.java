/*
 * Copyright 2016-2023 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.DebugUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.rest.factory.DefaultGraphFactory;
import uk.gov.gchq.gaffer.rest.factory.UnknownUserFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * System property keys and default values.
 */
public final class SystemProperty {
    // KEYS
    public static final String GRAPH_CONFIG_PATH = "gaffer.graph.config";
    public static final String SCHEMA_PATHS = "gaffer.schemas";
    public static final String STORE_PROPERTIES_PATH = "gaffer.storeProperties";
    public static final String GAFFER_VERSION = "gaffer.version";
    public static final String KORYPHE_VERSION = "koryphe.version";
    public static final String BASE_PATH = "gaffer.rest-api.basePath";
    public static final String REST_API_VERSION = "gaffer.rest-api.version";
    public static final String GRAPH_FACTORY_CLASS = "gaffer.graph.factory.class";
    public static final String USER_FACTORY_CLASS = "gaffer.user.factory.class";
    public static final String SERVICES_PACKAGE_PREFIX = "gaffer.rest-api.resourcePackage";
    public static final String PACKAGE_PREFIXES = "gaffer.package.prefixes";
    public static final String JSON_SERIALISER_CLASS = JSONSerialiser.JSON_SERIALISER_CLASS_KEY;
    public static final String JSON_SERIALISER_MODULES = JSONSerialiser.JSON_SERIALISER_MODULES;
    public static final String REST_DEBUG = DebugUtil.DEBUG;

    // Exposed Property Keys
    /**
     * A CSV of properties to expose via the properties endpoint.
     */
    public static final String EXPOSED_PROPERTIES = "gaffer.properties";
    public static final String APP_TITLE = "gaffer.properties.app.title";
    public static final String APP_DESCRIPTION = "gaffer.properties.app.description";
    public static final String APP_BANNER_COLOUR = "gaffer.properties.app.banner.colour";
    public static final String APP_BANNER_DESCRIPTION = "gaffer.properties.app.banner.description";
    public static final String APP_DOCUMENTATION_URL = "gaffer.properties.app.doc.url";
    public static final String LOGO_LINK = "gaffer.properties.app.logo.link";
    public static final String LOGO_IMAGE_URL = "gaffer.properties.app.logo.src";
    public static final String FAVICON_SMALL_URL = "gaffer.properties.app.logo.favicon.small";
    public static final String FAVICON_LARGE_URL = "gaffer.properties.app.logo.favicon.large";

    // DEFAULTS
    /**
     * Comma separated list of package prefixes to search for Functions and {@link uk.gov.gchq.gaffer.operation.Operation}s.
     */
    public static final String PACKAGE_PREFIXES_DEFAULT = "uk.gov.gchq";
    public static final String SERVICES_PACKAGE_PREFIX_DEFAULT = "uk.gov.gchq.gaffer.rest";
    public static final String BASE_PATH_DEFAULT = "rest";
    public static final String CORE_VERSION = "2.0.0";
    public static final String GAFFER_VERSION_DEFAULT = getVersion(GAFFER_VERSION);
    public static final String KORYPHE_VERSION_DEFAULT = getVersion(KORYPHE_VERSION);
    public static final String GRAPH_FACTORY_CLASS_DEFAULT = DefaultGraphFactory.class.getName();
    public static final String USER_FACTORY_CLASS_DEFAULT = UnknownUserFactory.class.getName();
    public static final String REST_DEBUG_DEFAULT = DebugUtil.DEBUG_DEFAULT;
    public static final String APP_TITLE_DEFAULT = "Gaffer REST";
    public static final String APP_DESCRIPTION_DEFAULT = "The Gaffer REST service.";
    public static final String APP_DOCUMENTATION_URL_DEFAULT = "https://gchq.github.io/gaffer-doc/latest/";
    public static final String LOGO_LINK_DEFAULT = "https://github.com/gchq/Gaffer";
    public static final String LOGO_IMAGE_URL_DEFAULT = "images/logo.png";

    private static Properties versionProperties;

    private SystemProperty() {
        // Private constructor to prevent instantiation.
    }

    private static String getVersion(final String propertyKey) {
        if (versionProperties == null) {
            loadVersionProperties();
        }
        return versionProperties.getProperty(propertyKey);
    }

    private static void loadVersionProperties() {
        try {
            Properties prop = new Properties();
            InputStream input = StreamUtil.openStream(SystemProperty.class, "version.properties");
            prop.load(input);
            versionProperties = prop;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
