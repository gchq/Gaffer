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

import gaffer.commonutil.CommonConstants;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.CloseableIterator;
import gaffer.commonutil.iterable.EmptyClosableIterable;
import gaffer.data.TransformIterable;
import gaffer.data.element.Element;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.user.User;
import org.apache.commons.io.IOUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.NoSuchElementException;

public class ElementFileExporter extends ElementExporter {
    private static final JSONSerialiser SERIALISER = new JSONSerialiser();

    public ElementFileExporter() {
    }

    /**
     * Constructs a {@link ElementFileExporter} with a custom directory name.
     * NOTE - this means you cannot write to the directory.
     *
     * @param timestamp the user's directory timestamp to read the exported results from
     */
    public ElementFileExporter(final long timestamp) {
        setTimestamp(timestamp);
    }

    @Override
    public boolean initialise(final Object config, final User user) {
        final boolean isNew = super.initialise(config, user);
        try {
            if (!new File(getDirectory()).exists()) {
                Files.createDirectory(Paths.get(getDirectory()));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to create folder for exporting.", e);
        }

        return isNew;
    }

    public String getDirectory() {
        return getUserTimestampedExportName();
    }

    @Override
    protected void addElements(final String key, final Iterable<Element> elements, final User user) {
        final String fileName = getFileName(key);
        if (!new File(fileName).exists()) {
            try {
                Files.createFile(Paths.get(fileName));
            } catch (IOException e) {
                throw new RuntimeException("Unable to create export file.", e);
            }
        }

        try {
            Files.write(Paths.get(fileName), serialise(elements), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Unable to write to file: " + fileName, e);
        }
    }


    @Override
    protected CloseableIterable<Element> getElements(final String key, final User user, final int start, final int end) {
        final CloseableIterable<Element> results;
        final String fileName = getFileName(key);
        if (new File(fileName).exists()) {
            results = new ElementReader(fileName, start, end);
        } else {
            results = new EmptyClosableIterable<>();
        }

        return results;
    }

    private Iterable<String> serialise(final Iterable<Element> elements) {
        return new ElementSerialiser(elements);
    }

    private String getFileName(final String key) {
        return getDirectory() + "/" + key + ".txt";
    }

    private static final class ElementSerialiser extends TransformIterable<Element, String> {
        private ElementSerialiser(final Iterable<Element> input) {
            super(input);
        }

        @Override
        protected String transform(final Element element) {
            try {
                return new String(SERIALISER.serialise(element), CommonConstants.UTF_8);
            } catch (SerialisationException | UnsupportedEncodingException e) {
                throw new RuntimeException("Unable to serialise element: " + element.toString());
            }
        }
    }

    private static final class ElementReader implements CloseableIterable<Element> {
        private final String fileName;
        private final int start;
        private final int linesToRead;

        private ElementReader(final String fileName, final int start, final int end) {
            this.fileName = fileName;
            this.start = start;
            this.linesToRead = end - start;
        }

        @Override
        public CloseableIterator<Element> iterator() {
            final BufferedReader reader;
            try {
                reader = Files.newBufferedReader(Paths.get(fileName));
                if (start > 0) {
                    int lineNum = 0;
                    while (lineNum < start) {
                        final String line = reader.readLine();
                        if (null == line) {
                            // end of file
                            break;
                        }

                        lineNum++;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Unable to read export from file: " + fileName, e);
            }

            return new CloseableIterator<Element>() {
                private String nextElement = null;
                private int linesRead = 0;
                private boolean isClosed = false;

                @Override
                public boolean hasNext() {
                    if (null == nextElement && !isClosed && linesRead < linesToRead) {
                        try {
                            nextElement = reader.readLine();
                        } catch (IOException e) {
                            throw new RuntimeException("Unable to read line from file", e);
                        }
                    }

                    final boolean hasNext = null != nextElement;
                    if (!hasNext) {
                        close();
                    }

                    return hasNext;
                }

                @Override
                public Element next() {
                    if (hasNext()) {
                        try {
                            return SERIALISER.deserialise(nextElement.getBytes(CommonConstants.UTF_8), Element.class);
                        } catch (SerialisationException | UnsupportedEncodingException e) {
                            throw new RuntimeException("Unable to deserialise element: " + nextElement);
                        } finally {
                            nextElement = null;
                            linesRead++;
                        }
                    } else {
                        throw new NoSuchElementException();
                    }
                }

                @Override
                public void close() {
                    if (!isClosed) {
                        IOUtils.closeQuietly(reader);
                        isClosed = true;
                    }
                }
            };
        }

        @Override
        public void close() {
            // the iterators need to be closed by calling close directly on them.
        }
    }
}
