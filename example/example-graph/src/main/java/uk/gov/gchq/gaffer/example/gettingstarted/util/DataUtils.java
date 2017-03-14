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

package uk.gov.gchq.gaffer.example.gettingstarted.util;

import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public final class DataUtils {

    public static List<String> loadData(final InputStream dataStream) {
        List<String> lines = null;
        try {
            lines = IOUtils.readLines(dataStream);
        } catch (final IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(dataStream);
        }
        return lines;
    }

    private DataUtils() {
        // This utils class should not be constructed.
    }
}
