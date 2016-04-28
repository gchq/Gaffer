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
import java.io.InputStream;

public class LoadAndQuery {

    public String dataFileLocation;
    public String dataSchemaLocation;
    public String dataTypesLocation;
    public String storeTypesLocation;
    public String storePropertiesLocation;

    protected InputStream getData() {
        return StreamUtil.openStream(LoadAndQuery.class, dataFileLocation, true);
    }

    protected InputStream getDataSchema() {
        return StreamUtil.openStream(LoadAndQuery.class, dataSchemaLocation, true);
    }

    protected InputStream getDataTypes() {
        return StreamUtil.openStream(LoadAndQuery.class, dataTypesLocation, true);
    }

    protected InputStream getStoreTypes() {
        return StreamUtil.openStream(LoadAndQuery.class, storeTypesLocation, true);
    }

    protected InputStream getStoreProperties() {
        return StreamUtil.openStream(LoadAndQuery.class, storePropertiesLocation, true);
    }

    public void setDataFileLocation(final String dataFileLocation) {
        this.dataFileLocation = dataFileLocation;
    }

    public void setDataSchemaLocation(final String dataSchemaLocation) {
        this.dataSchemaLocation = dataSchemaLocation;
    }

    public void setDataTypesLocation(final String dataTypesLocation) {
        this.dataTypesLocation = dataTypesLocation;
    }

    public void setStoreTypesLocation(final String storeTypesLocation) {
        this.storeTypesLocation = storeTypesLocation;
    }

    public void setStorePropertiesLocation(final String storePropertiesLocation) {
        this.storePropertiesLocation = storePropertiesLocation;
    }
}
