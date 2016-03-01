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

package gaffer.commonutil;

import java.io.InputStream;

public class StreamUtil {
    public static final String VIEW = "/view.json";
    public static final String DATA_SCHEMA = "/dataSchema.json";
    public static final String STORE_SCHEMA = "/storeSchema.json";
    public static final String SCHEMA_TYPES = "/schemaTypes.json";
    public static final String STORE_PROPERTIES = "/store.properties";

    public static InputStream view(final Class clazz) {
        return openStream(clazz, VIEW);
    }

    public static InputStream dataSchema(final Class clazz) {
        return openStream(clazz, DATA_SCHEMA);
    }

    public static InputStream storeSchema(final Class clazz) {
        return openStream(clazz, STORE_SCHEMA);
    }

    public static InputStream schemaTypes(final Class clazz) {
        return openStream(clazz, SCHEMA_TYPES);
    }

    public static InputStream storeProps(final Class clazz) {
        return openStream(clazz, STORE_PROPERTIES);
    }

    public static InputStream openStream(final Class clazz, final String path) {
        return clazz.getResourceAsStream(path);
    }

}
