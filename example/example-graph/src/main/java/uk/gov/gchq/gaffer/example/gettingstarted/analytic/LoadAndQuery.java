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
package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.example.gettingstarted.walkthrough.WalkthroughStrSubstitutor;
import uk.gov.gchq.gaffer.operation.OperationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public abstract class LoadAndQuery {
    public static final String DESCRIPTION_LOG_KEY = "description";
    private final Map<String, StringBuilder> logCache = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private boolean cacheLogs;
    private final int exampleId;
    private final String header;

    private final String dataFileLocation;
    private final String schemaFolderLocation;
    private final String storePropertiesLocation;

    public LoadAndQuery(final String header) {

        exampleId = Integer.parseInt(getClass().getSimpleName().replace(LoadAndQuery.class.getSimpleName(), ""));
        this.header = header;
        dataFileLocation = "/example/gettingstarted/" + exampleId + "/data.txt";
        schemaFolderLocation = "/example/gettingstarted/" + exampleId + "/schema";
        storePropertiesLocation = "/example/gettingstarted/mockaccumulostore.properties";
    }

    public abstract Object run() throws OperationException;

    public void log(final String message) {
        log(DESCRIPTION_LOG_KEY, message);
    }

    public void log(final String key, final String message) {
        if (cacheLogs) {
            StringBuilder logs = logCache.get(key);
            if (null == logs) {
                logs = new StringBuilder();
                logCache.put(key, logs);
            } else {
                logs.append("\n");
            }
            logs.append(message);
        } else {
            logger.info(message);
        }
    }

    public Map<String, StringBuilder> getLogCache() {
        return logCache;
    }

    public String walkthrough() throws OperationException {
        cacheLogs = true;
        final String walkthrough;
        try (final InputStream stream = StreamUtil.openStream(getClass(), "/example/gettingstarted/" + exampleId + "/walkthrough.md")) {
            if (null == stream) {
                throw new RuntimeException("Missing walkthrough file");
            }
            walkthrough = new String(IOUtils.toByteArray(stream), CommonConstants.UTF_8);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final String formattedWalkthrough = WalkthroughStrSubstitutor.substitute(walkthrough, this, exampleId, header);
        cacheLogs = false;

        return formattedWalkthrough;
    }

    public String getHeader() {
        return header;
    }

    protected InputStream getData() {
        return StreamUtil.openStream(LoadAndQuery.class, dataFileLocation, true);
    }

    protected InputStream[] getSchemas() {
        return StreamUtil.openStreams(LoadAndQuery.class, schemaFolderLocation, true);
    }

    protected InputStream getStoreProperties() {
        return StreamUtil.openStream(LoadAndQuery.class, storePropertiesLocation, true);
    }
}
