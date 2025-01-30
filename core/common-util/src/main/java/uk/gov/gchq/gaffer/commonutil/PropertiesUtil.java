/*
 * Copyright 2017-2023 Crown Copyright
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for Schema Properties.
 */
public final class PropertiesUtil {
    private static final Pattern PROPERTY_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9|\\-]+");

    private PropertiesUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Checks the input property String against the allowed property pattern.
     *
     * @param property String to validate.
     * @throws IllegalArgumentException if property String is invalid.
     */
    public static void validateName(final String property) {
        if (!PROPERTY_ALLOWED_CHARACTERS.matcher(property).matches()) {
            throw new IllegalArgumentException("Property is invalid: " + property + ", it must match regex: " + PROPERTY_ALLOWED_CHARACTERS);
        }
    }
    /**
     * Checks the input property String against the allowed property pattern.
     *
     * @param property String to validate.
     * @return boolean if name is valid
     */
    public static boolean isValidName(final String property) {
        if (!PROPERTY_ALLOWED_CHARACTERS.matcher(property).matches()) {
            return false;
        }
        return true;
    }

     public static String stripInvalidCharacters(final String property) {
        StringBuilder builder = new StringBuilder();
        Matcher matcher = PROPERTY_ALLOWED_CHARACTERS.matcher(property);
        while (matcher.find()) {
            builder.append(matcher.group());
        }
        return builder.toString();
    }
}
