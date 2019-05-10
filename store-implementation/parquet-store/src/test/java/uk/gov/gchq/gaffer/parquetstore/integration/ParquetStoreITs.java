/*
 * Copyright 2017-2018 Crown Copyright
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

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.integration.impl.GetAdjacentIdsIT;
import uk.gov.gchq.gaffer.integration.impl.PartAggregationIT;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;

import java.io.IOException;

public class ParquetStoreITs extends AbstractStoreITs {
    private static final ParquetStoreProperties STORE_PROPERTIES =
            ParquetStoreProperties.loadStoreProperties(StreamUtil.storeProps(ParquetStoreITs.class));
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public ParquetStoreITs() throws IOException {
        super(STORE_PROPERTIES);
        testFolder.create();
        final String testFolderPath = testFolder.newFolder().getAbsolutePath();
        ((ParquetStoreProperties) getStoreProperties()).setDataDir(testFolderPath + "/data");
        ((ParquetStoreProperties) getStoreProperties()).setTempFilesDir(testFolderPath + "/tmpdata");
        skipTest(GetAdjacentIdsIT.class, "GetAdjacentIds is not implemented yet");
        skipTest(PartAggregationIT.class, "known bug with ParquetStore");
    }
}
