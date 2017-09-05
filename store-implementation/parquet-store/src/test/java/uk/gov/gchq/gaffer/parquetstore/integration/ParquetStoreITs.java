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
 * limitations under the License
 */

package uk.gov.gchq.gaffer.parquetstore.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.integration.impl.GetAdjacentIdsIT;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.IOException;

public class ParquetStoreITs extends AbstractStoreITs {
    private static final StoreProperties STORE_PROPERTIES = StoreProperties
            .loadStoreProperties(StreamUtil.storeProps(ParquetStoreITs.class));

    @AfterClass
    public static void cleanUp() throws IOException {
        final FileSystem fs = FileSystem.get(new Configuration());
        final ParquetStoreProperties props = new ParquetStoreProperties();
        Path dataDir = new Path(props.getDataDir());
        fs.delete(dataDir, true);
        while (fs.listStatus(dataDir.getParent()).length == 0) {
            dataDir = dataDir.getParent();
            fs.delete(dataDir, true);
        }
    }

    public ParquetStoreITs() {
        super(STORE_PROPERTIES);
        skipTest(GetAdjacentIdsIT.class, "GetAdjacentIds is not implemented yet");
    }
}
