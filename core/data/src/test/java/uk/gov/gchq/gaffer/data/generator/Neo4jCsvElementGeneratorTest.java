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

public class Neo4jCsvElementGeneratorTest extends OpenCypherCsvElementGeneratorTest<Neo4jCsvElementGenerator> {
    protected Neo4jCsvElementGenerator getGenerator(final boolean trim, final char delimiter, final String nullString) {
        final Neo4jCsvElementGenerator generator = new Neo4jCsvElementGenerator();
        generator.setTrim(trim);
        generator.setDelimiter(delimiter);
        generator.setNullString(nullString);
        return generator;
    }

    protected String getResourcePath() {
        return "Neo4j";
    }
}
