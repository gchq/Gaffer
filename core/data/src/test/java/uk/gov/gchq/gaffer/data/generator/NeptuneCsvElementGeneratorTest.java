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

public class NeptuneCsvElementGeneratorTest extends OpenCypherCsvElementGeneratorTest<NeptuneCsvElementGenerator> {
    protected NeptuneCsvElementGenerator getGenerator(final boolean trim, final char delimiter, final String nullString) {
        final NeptuneCsvElementGenerator generator = new NeptuneCsvElementGenerator();
        generator.setTrim(trim);
        generator.setDelimiter(delimiter);
        generator.setNullString(nullString);
        return generator;
    }

    protected String getResourcePath() {
        return "Neptune";
    }
}
