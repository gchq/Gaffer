/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;

import uk.gov.gchq.gaffer.core.exception.Error;
import uk.gov.gchq.gaffer.core.exception.Status;
import uk.gov.gchq.gaffer.rest.ServiceConstants;
import uk.gov.gchq.gaffer.rest.SystemProperty;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

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

/**
 * An implementation of {@link IPropertiesServiceV2} that gets the configured system properties
 */
public class PropertiesServiceV2 implements IPropertiesServiceV2 {
    public static final String EXPOSED_PROPERTIES = SystemProperty.EXPOSED_PROPERTIES;
    public static final Map<String, String> CORE_EXPOSED_PROPERTIES = createCoreExposedProperties();

    private static Map<String, String> createCoreExposedProperties() {
        final Map<String, String> map = new LinkedHashMap<>();
        map.put(APP_TITLE, APP_TITLE_DEFAULT);
        map.put(APP_DESCRIPTION, APP_DESCRIPTION_DEFAULT);
        map.put(APP_BANNER_DESCRIPTION, "");
        map.put(APP_BANNER_COLOUR, "");
        map.put(APP_DOCUMENTATION_URL, APP_DOCUMENTATION_URL_DEFAULT);
        return Collections.unmodifiableMap(map);
    }

    @Override
    public Response getProperties() {
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

        return Response.ok(Collections.unmodifiableMap(properties))
                .header(ServiceConstants.GAFFER_MEDIA_TYPE_HEADER, ServiceConstants.GAFFER_MEDIA_TYPE)
                .build();
    }

    @Override
    public Response getProperty(final String propertyName) {
        final boolean isCore = CORE_EXPOSED_PROPERTIES.containsKey(propertyName);
        boolean isExposed = isCore;
        if (!isExposed) {
            final String propertiesList = System.getProperty(EXPOSED_PROPERTIES);
            if (null != propertiesList) {
                final String[] props = propertiesList.split(",");
                isExposed = ArrayUtils.contains(props, propertyName);
            }
        }

        String prop;
        if (isExposed) {
            prop = System.getProperty(propertyName);
            if (null == prop && isCore) {
                prop = CORE_EXPOSED_PROPERTIES.get(propertyName);
            }
        } else {
            prop = null;
        }

        final ResponseBuilder builder = null == prop ? Response.status(404)
                .entity(new Error.ErrorBuilder()
                        .status(Status.NOT_FOUND)
                        .statusCode(404)
                        .simpleMessage("Property: " + propertyName + " could not be found.")
                        .build())
                .type(MediaType.APPLICATION_JSON_TYPE)
                : Response.ok(prop)
                .type(MediaType.TEXT_PLAIN_TYPE);
        return builder.header(ServiceConstants.GAFFER_MEDIA_TYPE_HEADER, ServiceConstants.GAFFER_MEDIA_TYPE).build();
    }
}
