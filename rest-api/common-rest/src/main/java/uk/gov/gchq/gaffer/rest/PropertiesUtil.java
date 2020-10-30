/*
 * Copyright 2020 Crown Copyright
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

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_BANNER_COLOUR;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_BANNER_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DESCRIPTION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DOCUMENTATION_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_DOCUMENTATION_URL_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE;
import static uk.gov.gchq.gaffer.rest.SystemProperty.APP_TITLE_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.FAVICON_LARGE_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.FAVICON_SMALL_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_VERSION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.GAFFER_VERSION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.KORYPHE_VERSION;
import static uk.gov.gchq.gaffer.rest.SystemProperty.KORYPHE_VERSION_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_IMAGE_URL;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_IMAGE_URL_DEFAULT;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_LINK;
import static uk.gov.gchq.gaffer.rest.SystemProperty.LOGO_LINK_DEFAULT;

/**
 * Utility class used to retrieve the Gaffer System Properties
 */
public final class PropertiesUtil {

    private static final String EXPOSED_PROPERTIES = SystemProperty.EXPOSED_PROPERTIES;
    private static final Map<String, String> CORE_EXPOSED_PROPERTIES = createCoreExposedProperties();

    private static Map<String, String> createCoreExposedProperties() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put(APP_TITLE, APP_TITLE_DEFAULT);
        map.put(APP_DESCRIPTION, APP_DESCRIPTION_DEFAULT);
        map.put(APP_BANNER_DESCRIPTION, "");
        map.put(APP_BANNER_COLOUR, "");
        map.put(APP_DOCUMENTATION_URL, APP_DOCUMENTATION_URL_DEFAULT);
        map.put(LOGO_LINK, LOGO_LINK_DEFAULT);
        map.put(LOGO_IMAGE_URL, LOGO_IMAGE_URL_DEFAULT);
        map.put(FAVICON_SMALL_URL, LOGO_IMAGE_URL_DEFAULT);
        map.put(FAVICON_LARGE_URL, LOGO_IMAGE_URL_DEFAULT);
        map.put(GAFFER_VERSION, GAFFER_VERSION_DEFAULT);
        map.put(KORYPHE_VERSION, KORYPHE_VERSION_DEFAULT);
        return Collections.unmodifiableMap(map);
    }

    private PropertiesUtil() {
    }

    public static Map<String, String> getProperties() {
        final Map<String, String> properties = new LinkedHashMap<>();
        final String customPropNamesCsv = System.getProperty(EXPOSED_PROPERTIES);
        final Stream<String> customPropNames = null != customPropNamesCsv ? Arrays.stream(customPropNamesCsv.split(",")) : Stream.empty();
        Stream.concat(CORE_EXPOSED_PROPERTIES.keySet().stream(), customPropNames)
                .filter(StringUtils::isNotEmpty)
                .forEach(prop -> {
                    String value = System.getProperty(prop);
                    if (null == value) {
                        value = CORE_EXPOSED_PROPERTIES.get(prop);
                    }
                    properties.put(prop, value);
                });

        return Collections.unmodifiableMap(properties);
    }

    public static String getProperty(final String propertyName) {
        return getProperties().get(propertyName);
    }
}
