/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Utility methods for Schema Properties.
 */
public final class PropertiesUtil {
    private static final Pattern PROPERTY_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9|]*");

    private PropertiesUtil() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    /**
     * Checks the input String against the allowed property pattern.
     *
     * @param property String to validate.
     * @throws IllegalArgumentException if property String is invalid.
     */
    public static void validate(final String property) {
        if (property != null) {
            validateValue(property);
        }
    }

    /**
     * Checks all Property value Strings against the allowed property pattern.
     *
     * @param properties Properties to validate.
     * @throws IllegalArgumentException if and property value is invalid.
     */
    public static void validate(final Properties properties) {
        if (properties != null && !properties.isEmpty()) {
            List<String> propertiesValues = (List) properties.values();
            propertiesValues.forEach(val -> validate(val));
        }
    }

    private static void validateValue(final String propertyValue) {
        if (!PROPERTY_ALLOWED_CHARACTERS.matcher(propertyValue).matches()) {
            throw new IllegalArgumentException("Property is invalid: " + propertyValue + ", it must match regex: " + PROPERTY_ALLOWED_CHARACTERS);
        }
    }
}
