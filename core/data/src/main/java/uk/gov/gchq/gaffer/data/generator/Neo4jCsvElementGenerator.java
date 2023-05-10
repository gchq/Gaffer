/*
 * Copyright 2023 Crown Copyright
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

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.LinkedHashMap;

/**
 * An {@link OpenCypherCsvElementGenerator}s that will generate
 * Gaffer {@link Element}s from Neo4j CSV strings.
 */
@Since("2.0.0")
@Summary("Generates elements from a Neo4j CSV")
public class Neo4jCsvElementGenerator extends OpenCypherCsvElementGenerator {
    @Override
    protected LinkedHashMap<String, String> getFields() {
        return new Neo4jCsvGenerator().getFields();
    }
}
