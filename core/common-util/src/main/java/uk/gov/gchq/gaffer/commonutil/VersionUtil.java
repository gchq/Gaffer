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

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;

/**
 * Utility class for checking versioning within the Gaffer project.
 */
public final class VersionUtil {

    /**
     * Carry out a simple check to determine if the input string represents a valid
     * Gaffer version.
     *
     * @param versionString the version string to check
     * @return {@code true} if the version string is valid, otherwise {@code false}
     */
    public static boolean validateVersionString(final String versionString) {
        final String[] tokens = versionString.split("\\.");
        return tokens.length == 3 && Arrays.stream(tokens).map(NumberUtils::isNumber).reduce(true, Boolean::logicalAnd);
    }

    private VersionUtil() {
        // empty
    }
}
