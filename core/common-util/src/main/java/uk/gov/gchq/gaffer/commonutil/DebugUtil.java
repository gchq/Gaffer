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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DebugUtil {
    public static final String DEBUG = "gaffer.error-mode.debug";
    public static final String DEBUG_DEFAULT = String.valueOf(false);
    public static boolean isDebug;
    private static final Logger LOGGER = LoggerFactory.getLogger(DebugUtil.class);

    public static boolean checkDebugMode() {
        try {
            isDebug = Boolean.valueOf(System.getProperty(DEBUG, DEBUG_DEFAULT).trim());
            if (isDebug) {
                LOGGER.debug("Debug has been enabled in SystemProperties");
            }
        } catch (Exception e) {
            LOGGER.error("Defaulting Debug flag. Could not assign from System Properties: {}", e.getMessage());
            isDebug = Boolean.valueOf(DEBUG_DEFAULT);
        }

        return isDebug;
    }
}
