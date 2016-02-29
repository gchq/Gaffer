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

import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtil {
    public static final String VIEW = "/view.json";
    public static final String SCHEMA = "/schema/schema.json";
    public static final String DATA_SCHEMA = "/schema/dataSchema.json";
    public static final String DATA_TYPES = "/schema/dataTypes.json";
    public static final String STORE_SCHEMA = "/schema/storeSchema.json";
    public static final String STORE_TYPES = "/schema/storeTypes.json";
    public static final String STORE_PROPERTIES = "/store.properties";

    public static Path view(final Class clazz) {
        return path(clazz, VIEW);
    }

    public static Path schema(final Class clazz) {
        return path(clazz, SCHEMA);
    }

    public static Path dataSchema(final Class clazz) {
        return path(clazz, DATA_SCHEMA);
    }

    public static Path dataTypes(final Class clazz) {
        return path(clazz, DATA_TYPES);
    }

    public static Path storeSchema(final Class clazz) {
        return path(clazz, STORE_SCHEMA);
    }

    public static Path storeTypes(final Class clazz) {
        return path(clazz, STORE_TYPES);
    }

    public static Path storeProps(final Class clazz) {
        return path(clazz, STORE_PROPERTIES);
    }

    public static Path path(final Class clazz, final String path) {
        return Paths.get(clazz.getResource(path).getPath());
    }

}
