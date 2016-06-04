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

import gaffer.commonutil.CommonConstants;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.commonutil.iterable.CloseableIterator;
import gaffer.data.element.Element;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import org.apache.commons.io.IOUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.NoSuchElementException;

public class ElementJsonFileReaderIterable implements CloseableIterable<Element> {
    private static final JSONSerialiser SERIALISER = new JSONSerialiser();

    private final String fileName;
    private final int start;
    private final int linesToRead;

    public ElementJsonFileReaderIterable(final String fileName) {
        this(fileName, 0);
    }

    public ElementJsonFileReaderIterable(final String fileName, final int start) {
        this(fileName, start, Integer.MAX_VALUE);
    }

    public ElementJsonFileReaderIterable(final String fileName, final int start, final int end) {
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
                if (!hasNext && !isClosed) {
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
                    nextElement = null;
                }
            }
        };
    }

    @Override
    public void close() {
        // the iterators need to be closed by calling close directly on them.
    }
}
