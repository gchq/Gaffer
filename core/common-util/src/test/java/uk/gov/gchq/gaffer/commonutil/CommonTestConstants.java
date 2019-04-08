/*
 * Copyright 2016-2019 Crown Copyright
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

import java.io.File;

public final class CommonTestConstants {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonTestConstants.class);
    public static final File TMP_DIRECTORY;

    static {
        final String tmpDirectoryProperty = System.getProperty("java.io.tmpdir");

        if (null != tmpDirectoryProperty) {
            TMP_DIRECTORY = new File(tmpDirectoryProperty);
        } else {
            LOGGER.warn("Could not determine default temporary directory, using current directory.");
            TMP_DIRECTORY = new File(".");
        }
    }

    private CommonTestConstants() {
        // Empty
    }
}
