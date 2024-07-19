/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class that helps the conversion between Gaffer custom types and
 * standard types that Tinkerpop supports.
 *
 * Tinkerpop only supports a limited number of types so any custom Gaffer
 * ones can only be passed as String representations via Gremlin so need
 * conversion before using in Gaffer Operation chains.
 */
public final class GafferCustomTypeFactory {
    /*
     * List of supported standard types that GraphSON v3 can serialise
     */
    public static final List<Class<?>> GRAPHSONV3_TYPES = Collections.unmodifiableList(
        Arrays.asList(
            Boolean.class,
            Byte[].class,
            Date.class,
            Double.class,
            Float.class,
            Integer.class,
            List.class,
            Long.class,
            Map.class,
            Set.class,
            String.class,
            Timestamp.class,
            UUID.class));

    private static final Logger LOGGER = LoggerFactory.getLogger(GafferCustomTypeFactory.class);
    private static final Pattern TSTV_REGEX = Pattern.compile(".*\\[type=(?<type>.*),\\s*subType=(?<stype>.*),\\s*value=(?<val>.*)\\]$");

    private GafferCustomTypeFactory() {
        // Utility class
    }

    /**
     * Returns a the relevant Object e.g. {@link TypeSubTypeValue} from the
     * supplied value or ID, usually by parsing a specifically formatted string.
     * As a fallback will give back the original value if no relevant type
     * was found.
     *
     * <pre>
     * "TypeSubTypeValue[type=alpha,subType=beta,value=gamma]" // returns TypeSubTypeValue object
     * "[type=alpha,subType=beta,value=gamma]" // returns TypeSubTypeValue object
     * "normalvalue" // returns itself
     * </pre>
     *
     * @param value The value.
     * @return The value as its relevant type.
     */
    public static Object parseAsCustomTypeIfValid(final Object value) {
        if (value instanceof String) {
            Matcher tstvMatcher = TSTV_REGEX.matcher((String) value);
            if (tstvMatcher.matches()) {
                // Split into a TSTV via matcher
                LOGGER.debug("Parsing string as a TSTV: {}", value);
                return new TypeSubTypeValue(
                        tstvMatcher.group("type"),
                        tstvMatcher.group("stype"),
                        tstvMatcher.group("val"));
            }
        }

        // If value is collection e.g. list or set then check the values inside it
        if (value instanceof Collection<?>) {
            List<Object> converted = new ArrayList<>();
            ((Collection<?>) value).forEach(v -> converted.add(parseAsCustomTypeIfValid(v)));
            // Return a set if needed
            if (value instanceof Set<?>) {
                return new HashSet<>(converted);
            }
            return converted;
        }

        return value;
    }

     /**
     * Parses the given value to make sure it can be used with Tinkerpops
     * GraphSONv3 types. Will convert the value to String representation
     * if type is not compatible or if value is a collection it will convert
     * all the values inside it.
     *
     * @param value The value to parse.
     * @return The value in compatible format.
     */
    public static Object parseForGraphSONv3(final Object value) {
        if (value == null) {
            return value;
        }

        // If value is collection e.g. list or set then check the values inside it
        if (value instanceof Collection<?>) {
            List<Object> converted = new ArrayList<>();
            ((Collection<?>) value).forEach(v -> converted.add(parseForGraphSONv3(v)));
            // Return a set if needed
            if (value instanceof Set<?>) {
                return new HashSet<>(converted);
            }
            return converted;
        }

        // Check if the value can be used with GraphSON v3
        if (!GRAPHSONV3_TYPES.contains(value.getClass())) {
            LOGGER.debug("Converting value to string {}", value);
            return value.toString();
        }

        return value;
    }

}
