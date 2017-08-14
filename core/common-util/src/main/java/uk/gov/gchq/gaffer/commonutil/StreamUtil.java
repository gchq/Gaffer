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

public abstract class StreamUtil {
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
        // this class should not be instantiated - it contains only util methods and constants.
    }

    public static InputStream graphConfig(final Class clazz) {
        return openStream(clazz, GRAPH_CONFIG);
    }

    public static InputStream view(final Class clazz) {
        return openStream(clazz, VIEW);
    }

    public static InputStream[] schemas(final Class clazz) {
        return openStreams(clazz, SCHEMA_FOLDER);
    }

    public static InputStream schema(final Class clazz) {
        return openStream(clazz, SCHEMA);
    }

    public static InputStream elementsSchema(final Class clazz) {
        return openStream(clazz, ELEMENTS_SCHEMA);
    }

    public static InputStream typesSchema(final Class clazz) {
        return openStream(clazz, TYPES_SCHEMA);
    }

    public static InputStream storeProps(final Class clazz) {
        return openStream(clazz, STORE_PROPERTIES);
    }

    public static InputStream[] openStreams(final Class clazz, final String folderPath) {
        if (null == folderPath) {
            return new InputStream[0];
        }

        String folderPathChecked = getFormattedPath(folderPath);


        final HashSet<InputStream> schemas = Sets.newHashSet();

        new Reflections(new ConfigurationBuilder()
                .setScanners(new ResourcesScanner())
                .setUrls(ClasspathHelper.forClass(clazz)))
                .getResources(Pattern.compile(".*"))
                .stream()
                .filter(schemaFile -> schemaFile.startsWith(folderPathChecked))
                .forEach(schemaFile -> {
                            try {
                                schemas.add(openStream(clazz, schemaFile));
                            } catch (Exception e) {
                                int closedStreamsCount = closeStreams(schemas.toArray(new InputStream[schemas.size()]));
                                LOGGER.info(String.format("Closed %s input streams", closedStreamsCount));
                            }
                        }
                );

        if (schemas.isEmpty()) {
            throw new IllegalArgumentException("No schemas could be found in path: " + folderPath);
        }

        return schemas.toArray(new InputStream[schemas.size()]);
    }

    private static String getFormattedPath(final String folderPath) {
        String folderPathChecked = folderPath.endsWith("/") ? folderPath : folderPath + "/";
        if (folderPathChecked.startsWith("/")) {
            folderPathChecked = folderPathChecked.substring(1);
        }
        return folderPathChecked;
    }

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

    public static InputStream openStream(final URI uri) throws IOException {
        try {
            return uri.toURL().openStream();
        } catch (final IOException e) {
            LOGGER.error("Failed to create input stream: {}", uri, e);
            throw e;
        }
    }

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

    public static InputStream openStream(final Class clazz, final String path) throws IllegalArgumentException {
        final String checkedPath = formatPathForOpenStream(path);
        final InputStream resourceAsStream = clazz.getResourceAsStream(checkedPath);
        return (resourceAsStream != null) ? resourceAsStream : processException(path);
    }

    private static InputStream processException(final String path) throws IllegalArgumentException {
        LOGGER.error(LOG_FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH, path);
        throw new IllegalArgumentException(FAILED_TO_CREATE_INPUT_STREAM_FOR_PATH + path);
    }

    public static String formatPathForOpenStream(final String path) {
        if (StringUtils.isEmpty(path)) {
            processException(path);
        }
        return path.startsWith("/") ? path : "/" + path;
    }
}
