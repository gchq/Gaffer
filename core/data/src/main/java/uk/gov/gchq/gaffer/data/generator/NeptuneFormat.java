/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.data.element.IdentifierType;

import java.util.LinkedHashMap;

public class NeptuneFormat implements CsvFormat {

    public static final String NEPTUNE_VERTEX = ":ID";

    public static final String NEPTUNE_ENTITY_GROUP = ":LABEL";

    public static final String NEPTUNE_SOURCE = ":START_ID";

    public static final String NEPTUNE_DESTINATION = ":END_ID";

    public static final String NEPTUNE_EDGE_GROUP = ":TYPE";
    public static final LinkedHashMap<String, String> IDENTIFIERS = new LinkedHashMap<String, String>() { {
        put(String.valueOf(IdentifierType.VERTEX), NEPTUNE_VERTEX);
        put("NEPTUNE_ENTITY_GROUP", NEPTUNE_ENTITY_GROUP);
        put("NEPTUNE_EDGE_GROUP", NEPTUNE_EDGE_GROUP);
        put(String.valueOf(IdentifierType.SOURCE), NEPTUNE_SOURCE);
        put(String.valueOf(IdentifierType.DESTINATION), NEPTUNE_DESTINATION);
    } };

    @Override
    public LinkedHashMap<String, String> getIdentifiers() {
        return IDENTIFIERS;
    }
}
