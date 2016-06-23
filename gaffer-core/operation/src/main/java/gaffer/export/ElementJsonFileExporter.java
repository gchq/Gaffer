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

package gaffer.export;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.EmptyClosableIterable;
import gaffer.data.element.Element;
import gaffer.user.User;
import gaffer.util.ElementJsonFileReaderIterable;
import gaffer.util.ElementJsonFileWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * An <code>ElementJsonFileExporter</code> is an {@link ElementExporter} that
 * exports {@link Element}s to a simple text file contain all elements serialised
 * into JSON.
 * <p>
 * Files will be named: json-exports/[userId]_[timestamp]/[export-key].txt
 * </p>
 * <p>
 * The export timestamp can be found by calling getTimestamp on
 * </p>
 * <p>
 * A previous file can be loaded in for appending additional elements to or reading, by
 * setting the file timestamp on this class.
 * </p>
 */
public class ElementJsonFileExporter extends ElementExporter {
    public static final String PARENT_DIRECTORY = "json-exports";

    public final ElementJsonFileWriter writer = new ElementJsonFileWriter();

    public ElementJsonFileExporter() {
    }

    /**
     * Constructs a {@link ElementJsonFileExporter} with a a specific timestamp.
     * This can be used to read data from a previously exported file.
     *
     * @param timestamp the user's directory timestamp to read the exported results from
     */
    public ElementJsonFileExporter(final long timestamp) {
        setTimestamp(timestamp);
    }

    @Override
    public void initialise(final String key, final Object config, final User user) {
        super.initialise(key, config, user);
        try {
            if (!new File(getFileName()).exists()) {
                Files.createDirectories(Paths.get(PARENT_DIRECTORY));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to create folder for exporting.", e);
        }
    }

    public String getFileName() {
        return PARENT_DIRECTORY + "/" + getExportName() + ".txt";
    }

    @Override
    protected void addElements(final Iterable<Element> elements, final User user) {
        final String fileName = getFileName();
        try {
            writer.write(elements, fileName);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write elements to file: " + fileName, e);
        }
    }

    @Override
    protected CloseableIterable<Element> getElements(final User user, final int start, final int end) {
        final CloseableIterable<Element> results;
        final String fileName = getFileName();
        if (new File(fileName).exists()) {
            results = new ElementJsonFileReaderIterable(fileName, start, end);
        } else {
            results = new EmptyClosableIterable<>();
        }

        return results;
    }
}
