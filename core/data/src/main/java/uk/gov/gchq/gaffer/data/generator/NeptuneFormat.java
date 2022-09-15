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

    protected LinkedHashMap<String, String> identifiers = new LinkedHashMap<String, String>();

    public static final String NEPTUNE_VERTEX = ":ID";

    public static final String NEPTUNE_ENTITY_GROUP = ":LABEL";

    public static final String NEPTUNE_SOURCE = ":START_ID";

    public static final String NEPTUNE_DESTINATION = ":END_ID";

    public static final String NEPTUNE_EDGE_GROUP = ":TYPE";
    private static final String ENTITY_GROUP = "NEPTUNE_ENTITY_GROUP";
    private static final String EDGE_GROUP = "NEPTUNE_EDGE_GROUP";

    public NeptuneFormat() {
        setIdentifiers();
    }

    @Override
    public void setIdentifiers() {
        this.identifiers.put(String.valueOf(IdentifierType.VERTEX), NEPTUNE_VERTEX);
        this.identifiers.put("NEPTUNE_ENTITY_GROUP", NEPTUNE_ENTITY_GROUP);
        this.identifiers.put("NEPTUNE_EDGE_GROUP", NEPTUNE_EDGE_GROUP);
        this.identifiers.put(String.valueOf(IdentifierType.SOURCE), NEPTUNE_SOURCE);
        this.identifiers.put(String.valueOf(IdentifierType.DESTINATION), NEPTUNE_DESTINATION);
    }

    @Override
    public String getEntityGroup() {
        return ENTITY_GROUP;
    }

    @Override
    public String getEdgeGroup() {
        return EDGE_GROUP;
    }

    @Override
    public LinkedHashMap<String, String> getIdentifiers() {
        return identifiers;
    }
}
