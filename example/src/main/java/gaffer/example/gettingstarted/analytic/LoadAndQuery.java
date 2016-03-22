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
    protected InputStream getData(final int id) {
        return StreamUtil.openStream(LoadAndQuery.class, "/example/gettingstarted/data/data" + id + ".txt", true);
    }

    protected InputStream getDataSchema(final int id) {
        return StreamUtil.openStream(LoadAndQuery.class, "/example/gettingstarted/schema" + id + "/dataSchema.json", true);
    }

    protected InputStream getDataTypes(final int id) {
        return StreamUtil.openStream(LoadAndQuery.class, "/example/gettingstarted/schema" + id + "/dataTypes.json", true);
    }

    protected InputStream getStoreTypes(final int id) {
        return StreamUtil.openStream(LoadAndQuery.class, "/example/gettingstarted/schema" + id + "/storeTypes.json", true);
    }

    protected InputStream getStoreProperties() {
        return StreamUtil.openStream(LoadAndQuery.class, "/example/gettingstarted/properties/mockaccumulostore.properties", true);
    }
}
