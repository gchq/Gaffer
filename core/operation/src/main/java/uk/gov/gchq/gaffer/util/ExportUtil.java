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

package uk.gov.gchq.gaffer.util;

import uk.gov.gchq.gaffer.user.User;
import java.util.regex.Pattern;

public final class ExportUtil {
    public static final String NON_PLAIN_TEXT = "[^a-zA-Z0-9]";
    public static final Pattern KEY_REGEX = Pattern.compile("[a-zA-Z0-9]*");

    private ExportUtil() {
        // Utility class with static methods, so it should not be constructed
    }

    public static void validateKey(final String key) {
        if (!KEY_REGEX.matcher(key).matches()) {
            throw new IllegalArgumentException("The export key '" + key + "' is invalid. Only letters 'a to z' and 'A to Z' are allowed.");
        }
    }

    public static String getPlainTextUserId(final User user) {
        return user.getUserId().replaceAll(NON_PLAIN_TEXT, "");
    }
}
