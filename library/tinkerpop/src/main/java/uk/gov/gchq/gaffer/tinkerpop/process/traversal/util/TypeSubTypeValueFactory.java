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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TypeSubTypeValueFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeSubTypeValueFactory.class);
    private static final Pattern TSTV_REGEX = Pattern.compile("^t:(?<type>.*)\\|st:(?<stype>.*)\\|v:(?<val>.*)$");

    private TypeSubTypeValueFactory() {
        // Utility class
    }

    /**
     * Returns a the relevant Object e.g. {@link TypeSubTypeValue} from the
     * supplied value or ID, usually by parsing a specifically formatted string.
     * As a fallback will give back the original value if no relevant type
     * was found.
     *
     * @param value The value.
     * @return The value as its relevant type.
     */
    public static Object parseStringAsTstvIfValid(final String value) {
        Matcher tstvMatcher = TSTV_REGEX.matcher(value);
        if (tstvMatcher.matches()) {
            // Split into a TSTV via matcher
            LOGGER.debug("Parsing string as a TSTV: {}", value);
            return new TypeSubTypeValue(
                    tstvMatcher.group("type"),
                    tstvMatcher.group("stype"),
                    tstvMatcher.group("val"));
        }

        return value;
    }

}
