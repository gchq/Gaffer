/*
 * Copyright 2017. Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

/**
 *
 */
public class SingleUseParquetStore extends ParquetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUseParquetStore.class);

    @Override
    public void initialise(final Schema schema, final StoreProperties properties) throws StoreException {
        cleanUp();
        super.initialise(schema, properties);
    }

    private void cleanUp() throws StoreException {
        try {
            final FileSystem fs = FileSystem.get(new Configuration());
            ParquetStoreProperties props = getProperties();
            if (props == null) {
                props = new ParquetStoreProperties();
            }
            deleteFolder(props.getDataDir(), fs);
        } catch (IOException e) {
            throw new StoreException(e.getMessage(), e);
        }
    }

    private void deleteFolder(final String path, final FileSystem fs) throws IOException {
        LOGGER.debug("path: " + path);
        Path dataDir = new Path(path);
        if (fs.exists(dataDir)) {
            fs.delete(dataDir, true);
            while (fs.listStatus(dataDir.getParent()).length == 0) {
                dataDir = dataDir.getParent();
                fs.delete(dataDir, true);
            }
        }
    }
}
