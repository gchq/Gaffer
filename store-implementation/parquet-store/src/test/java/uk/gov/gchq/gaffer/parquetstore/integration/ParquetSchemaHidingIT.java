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
package uk.gov.gchq.gaffer.parquetstore.integration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.graph.SchemaHidingIT;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.store.Store;

import java.io.IOException;

public class ParquetSchemaHidingIT extends SchemaHidingIT {
    public ParquetSchemaHidingIT() {
        super("parquetStore.properties");
    }

    @Override
    protected void cleanUp() {
        final Store store = Store.createStore("graphId", createFullSchema(), ParquetStoreProperties.loadStoreProperties(StreamUtil.openStream(getClass(), storePropertiesPath)));
        String dataDir = "";
        try {
            dataDir = ((ParquetStore) store).getDataDir();
            deleteFolder(dataDir, ((ParquetStore) store).getFS());
        } catch (final IOException e) {
            throw new RuntimeException("Exception deleting folder: " + dataDir, e);
        }
    }

    private void deleteFolder(final String path, final FileSystem fs) throws IOException {
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
