///*
// * Copyright 2016-2018 Crown Copyright
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package uk.gov.gchq.gaffer.parquetstore.integration;
//
//import org.junit.Rule;
//import org.junit.rules.TemporaryFolder;
//
//import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
//import uk.gov.gchq.gaffer.integration.graph.SchemaHidingIT;
//import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;
//import uk.gov.gchq.gaffer.store.Store;
//import uk.gov.gchq.gaffer.store.schema.Schema;
//
//import java.io.IOException;
//
//public class ParquetSchemaHidingIT extends SchemaHidingIT {
//    @Rule
//    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
//    private static ParquetStoreProperties parquetStoreProperties;
//
//    public ParquetSchemaHidingIT() {
//        super("parquetStore.properties");
//    }
//
//    @Override
//    protected void cleanUp() {
//    }
//
//    @Override
//    protected Store createStore(final Schema schema) throws IOException {
//        if (null == parquetStoreProperties) {
//            parquetStoreProperties = ParquetStoreProperties
//                    .loadStoreProperties(storePropertiesPath);
//            testFolder.create();
//            final String path = testFolder.newFolder().getAbsolutePath();
//            parquetStoreProperties.setDataDir(path + "/data");
//            parquetStoreProperties.setTempFilesDir(path + "/tmpdata");
//        }
//        return Store.createStore("graphId", schema, parquetStoreProperties);
//    }
//}
