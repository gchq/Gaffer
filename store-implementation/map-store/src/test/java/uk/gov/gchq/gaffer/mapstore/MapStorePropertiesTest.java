/*
 * Copyright 2021-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StorePropertiesTest.TestCustomJsonModules1;
import uk.gov.gchq.gaffer.store.StorePropertiesTest.TestCustomJsonModules2;

import static org.assertj.core.api.Assertions.assertThat;

class MapStorePropertiesTest {

    @Test
    void shouldMergeJsonModules() {
        // Given
        final MapStoreProperties props = new MapStoreProperties();
        props.setJsonSerialiserModules(TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName());

        // When
        final String modules = props.getJsonSerialiserModules();

        // Then
        assertThat(modules)
            .isEqualTo(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName() + "," + TestCustomJsonModules2.class.getName());
    }

    @Test
    void shouldMergeJsonModulesAndDeduplicate() {
        // Given
        final MapStoreProperties props = new MapStoreProperties();
        props.setJsonSerialiserModules(TestCustomJsonModules1.class.getName() + "," + SketchesJsonModules.class.getName());

        // When
        final String modules = props.getJsonSerialiserModules();

        // Then
        assertThat(modules).isEqualTo(SketchesJsonModules.class.getName() + "," + TestCustomJsonModules1.class.getName());
    }
}
