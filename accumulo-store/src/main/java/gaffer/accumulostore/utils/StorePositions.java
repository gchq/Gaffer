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

package gaffer.accumulostore.utils;

import java.util.HashSet;
import java.util.Set;

/**
 * These positions are used to determine where to store {@link gaffer.data.element.Element} properties in Accumulo.
 * <p>
 * These positions relate to accumulo key parts as follows:
 * <ul>
 * <li>COLUMN_QUALIFIER - column qualifier</li>
 * <li>VISIBILITY - column visibility</li>
 * <li>TIMESTAMP - timestamp</li>
 * </ul>
 * VALUE simply maps to Accumulo's Value
 * <p>
 * The positions should be assigned to properties in the {@link gaffer.store.schema.StoreSchema} as follows:
 * <ul>
 * <li>VISIBILITY is optional and if used you will need to supply a custom
 * serialiser to convert the property value into a value Accumulo understands.
 * </li>
 * <li>TIMESTAMP is optional. If it is not specified accumulo will add the time
 * at which an element is stored, this is then used for age of.</li>
 * <li>COLUMN_QUALIFIER is optional and multiple Element properties can be
 * assigned to it - this allows properties to be included in the key and can
 * then optionally be aggregated at query time using the summarise flag on
 * {@link gaffer.operation.GetOperation}.</li>
 * <li>All remaining Element properties should be assigned to the VALUE
 * position.</li>
 * </ul>
 */
public enum StorePositions {
    COLUMN_QUALIFIER, VISIBILITY, TIMESTAMP, VALUE;

    public static final Set<String> NAMES = getNames();

    private static Set<String> getNames() {
        final Set<String> names = new HashSet<>(StorePositions.values().length);
        for (final StorePositions storePositions : StorePositions.values()) {
            names.add(storePositions.name());
        }

        return names;
    }

    public boolean isEqual(final String name) {
        return null != name && name().equals(name);
    }

    public static boolean isValidName(final String name) {
        return NAMES.contains(name);
    }
}
