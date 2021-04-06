/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.flink.operation;

import uk.gov.gchq.gaffer.data.element.Element;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.util.stream.Collectors.toList;

public interface ElementFileStore {

    String getStorePath();

    default void writeElement(final int fileId, final Element element) throws IOException {
        try (ObjectOutputStream outputStream = new ObjectOutputStream(Files.newOutputStream(getElementPath(fileId)))) {
            outputStream.writeObject(element);
        }
    }

    default Path getElementPath(final int fileId) {
        return getElementPath(Integer.toString(fileId));
    }

    default Path getElementPath(final String fileId) {
        return Paths.get(getStorePath(), fileId);
    }

    default List<String> getFileIds() throws IOException {
        return Files.list(Paths.get(getStorePath())).map(Path::getFileName).map(Path::toString).collect(toList());
    }

    default List<Element> getElements() throws IOException {
        return getFileIds().stream().map(id -> {
            try (ObjectInputStream inputStream = new ObjectInputStream(Files.newInputStream(getElementPath(id)))) {
                return (Element) inputStream.readObject();
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
        }).collect(toList());
    }
}
