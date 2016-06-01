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
package gaffer.example.gettingstarted.analytic;

import gaffer.commonutil.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;

public class LoadAndQuery {
    private String dataFileLocation;
    private String schemaFolderLocation;
    private String storePropertiesLocation;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected InputStream getData() {
        return StreamUtil.openStream(LoadAndQuery.class, dataFileLocation, true);
    }

    protected InputStream[] getSchemas() {
        return StreamUtil.openStreams(LoadAndQuery.class, schemaFolderLocation, true);
    }

    protected InputStream getStoreProperties() {
        return StreamUtil.openStream(LoadAndQuery.class, storePropertiesLocation, true);
    }

    public void setDataFileLocation(final String dataFileLocation) {
        this.dataFileLocation = dataFileLocation;
    }

    public void setSchemaFolderLocation(final String schemaFolderLocation) {
        this.schemaFolderLocation = schemaFolderLocation;
    }

    public void setStorePropertiesLocation(final String storePropertiesLocation) {
        this.storePropertiesLocation = storePropertiesLocation;
    }

    public void log(final String message) {
        logger.info(message);
    }
}
