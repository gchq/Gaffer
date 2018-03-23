/*
 * Copyright 2016-2018 Crown Copyright
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

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Utility methods for opening {@link InputStream}s.
 */
public final class StreamUtil {
    public static final String VIEW = "/view.json";
    public static final String SCHEMA_FOLDER = "/schema/";
    public static final String SCHEMA = SCHEMA_FOLDER + "schema.json";
    public static final String ELEMENTS_SCHEMA = SCHEMA_FOLDER + "elements.json";
    public static final String TYPES_SCHEMA = SCHEMA_FOLDER + "types.json";
    public static final String STORE_PROPERTIES = "/store.properties";
    public static final String GRAPH_CONFIG = "/graphConfig.json";
    public static final String FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH = "Failed to create input stream for path: ";
    public static final String LOG_FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH = FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH + "{}";
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamUtil.class);

    private StreamUtil() {
        // Private constructor to prevent instantiation.
    }

    /**
     * Open the graph config file located at the same location as the provided
     * class.
     *
     * @param clazz the class determining the location of the graph config file
     * @return an {@link InputStream} representing the graph config file
     */
    public static InputStream graphConfig(final Class clazz) {
        return openStream(clazz, GRAPH_CONFIG);
    }

    /**
     * Open the view file located at the same location as the provided class.
     *
     * @param clazz the class determining the location of the view file
     * @return an {@link InputStream} representing the view file
     */
    public static InputStream view(final Class clazz) {
        return openStream(clazz, VIEW);
    }

    /**
     * Open the schema files located at the same location as the provided
     * class.
     *
     * @param clazz the class determining the location of the schema files
     * @return an array of {@link InputStream}s representing the schema files
     */
    public static InputStream[] schemas(final Class clazz) {
        return openStreams(clazz, SCHEMA_FOLDER);
    }

    /**
     * Open the schema file located at the same location as the provided
     * class.
     *
     * @param clazz the class determining the location of the schema file
     * @return an {@link InputStream} representing the schema file
     */
    public static InputStream schema(final Class clazz) {
        return openStream(clazz, SCHEMA);
    }

    /**
     * Open the elements schema file located at the same location as the provided
     * class.
     *
     * @param clazz the class determining the location of the elements schema file
     * @return an {@link InputStream} representing the elements schema file
     */
    public static InputStream elementsSchema(final Class clazz) {
        return openStream(clazz, ELEMENTS_SCHEMA);
    }

    /**
     * Open the types schema file located at the same location as the provided
     * class.
     *
     * @param clazz the class determining the location of the types schema file
     * @return an {@link InputStream} representing the types schema file
     */
    public static InputStream typesSchema(final Class clazz) {
        return openStream(clazz, TYPES_SCHEMA);
    }

    /**
     * Open the store properties file located at the same location as the provided
     * class.
     *
     * @param clazz the class determining the location of the store properties file
     * @return an {@link InputStream} representing the store properties file
     */
    public static InputStream storeProps(final Class clazz) {
        return openStream(clazz, STORE_PROPERTIES);
    }

    /**
     * Open all of the files found in the specified subdirectory of the provided
     * class.
     *
     * @param clazz      the class location
     * @param folderPath the subdirectory in the class location
     * @return an array of {@link InputStream}s representing the files found
     */
    public static InputStream[] openStreams(final Class clazz, final String folderPath) {
        if (null == folderPath) {
            return new InputStream[0];
        }

        String folderPathChecked = getFormattedPath(folderPath);

        final HashSet<InputStream> inputStreams = Sets.newHashSet();

        new Reflections(new ConfigurationBuilder()
                .setScanners(new ResourcesScanner())
                .setUrls(ClasspathHelper.forClass(clazz)))
                .getResources(Pattern.compile(".*"))
                .stream()
                .filter(file -> file.startsWith(folderPathChecked))
                .forEach(file -> {
                            try {
                                inputStreams.add(openStream(clazz, file));
                            } catch (final Exception e) {
                                int closedStreamsCount = closeStreams(inputStreams.toArray(new InputStream[inputStreams.size()]));
                                LOGGER.info(String.format("Closed %s input streams", closedStreamsCount));
                            }
                        }
                );

        if (inputStreams.isEmpty()) {
            throw new IllegalArgumentException("No file could be found in path: " + folderPath);
        }

        return inputStreams.toArray(new InputStream[inputStreams.size()]);
    }

    private static String getFormattedPath(final String folderPath) {
        String folderPathChecked = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        if (folderPathChecked.startsWith("/")) {
            folderPathChecked = folderPathChecked.substring(1);
        }
        return folderPathChecked;
    }

    /**
     * Create an array of {@link InputStream}s from the provided list of {@link URI}s.
     *
     * @param uris the URIs to open as input streams
     * @return an array of input streams
     * @throws IOException if there was an error opening the streams
     */
    public static InputStream[] openStreams(final URI... uris) throws IOException {
        final InputStream[] schemas = new InputStream[uris.length];
        for (int pos = 0; pos < uris.length; pos++) {
            try {
                schemas[pos] = openStream(uris[pos]);
            } catch (final Exception e) {
                int closedStreamsCount = closeStreams(schemas);
                LOGGER.info("Closed {} input streams", closedStreamsCount);
                throw e;
            }
        }
        return schemas;
    }

    /**
     * Create an {@link InputStream}s from the provided {@link URI}.
     *
     * @param uri the URI to open as an input stream
     * @return an input streams
     * @throws IOException if there was an error opening the stream
     */
    public static InputStream openStream(final URI uri) throws IOException {
        try {
            return uri.toURL().openStream();
        } catch (final IOException e) {
            LOGGER.error("Failed to create input stream: {}", uri, e);
            throw e;
        }
    }

    /**
     * Safely close the supplied list of {@link InputStream}s.
     *
     * @param inputStreams the input streams to close
     * @return an integer indicating the number of streams which were successfully closed.
     */
    public static int closeStreams(final InputStream... inputStreams) {
        int closedStreamsCount = 0;
        for (final InputStream stream : inputStreams) {
            try {
                stream.close();
            } catch (final Exception e) {
                LOGGER.debug("Exception while closing input streams", e);
            }
            closedStreamsCount++;
        }
        return closedStreamsCount;
    }

    /**
     * Open the file found at the the specified path under the location of the given
     * class.
     *
     * @param clazz the class location
     * @param path  the path in the class location
     * @return an input stream representating the requested file
     * @throws IllegalArgumentException if there was an error opening the stream
     */
    public static InputStream openStream(final Class clazz, final String path) throws IllegalArgumentException {
        final String checkedPath = formatPathForOpenStream(path);
        final InputStream resourceAsStream = clazz.getResourceAsStream(checkedPath);
        return (null != resourceAsStream) ? resourceAsStream : processException(path);
    }

    private static InputStream processException(final String path) throws IllegalArgumentException {
        LOGGER.error(LOG_FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH, path);
        throw new IllegalArgumentException(FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH + path);
    }

    /**
     * Format a path to ensure that it begins with a '/' character.
     *
     * @param path the path to format
     * @return a correctly formatted path string
     */
    public static String formatPathForOpenStream(final String path) {
        if (StringUtils.isEmpty(path)) {
            processException(path);
        }
        return path.startsWith("/") ? path : "/" + path;
    }
}
