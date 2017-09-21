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

import java.util.regex.Pattern;

/**
 * Utility methods for Schema groups.
 */
public final class GroupUtil {
    private static final Pattern PROPERTY_ALLOWED_CHARACTERS = Pattern.compile("[a-zA-Z0-9|]*");

    private GroupUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Checks the input group String against the allowed group pattern.
     *
     * @param group group name String to validate.
     * @throws IllegalArgumentException if group name String is invalid.
     */
    public static void validateName(final String group) {
        if (!PROPERTY_ALLOWED_CHARACTERS.matcher(group).matches()) {
            throw new IllegalArgumentException("Group is invalid: " + group + ", it must match regex: " + PROPERTY_ALLOWED_CHARACTERS);
        }
    }
}
