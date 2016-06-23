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

package gaffer.util;

import gaffer.data.element.Element;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ElementJsonFileWriter {
    /**
     * Writes elements as json to file. Each element is on a separate line in the file.
     * Requires the parent directories to exist.
     *
     * @param elements the elements to be written to file
     * @param fileName the file name
     * @throws IOException when creation of file or writing to file fails
     */
    public void write(final Iterable<Element> elements, final String fileName) throws IOException {
        if (!new File(fileName).exists()) {
            Files.createFile(Paths.get(fileName));
        }

        Files.write(Paths.get(fileName), new ElementJsonSerialiserIterable(elements), StandardOpenOption.APPEND);
    }
}
