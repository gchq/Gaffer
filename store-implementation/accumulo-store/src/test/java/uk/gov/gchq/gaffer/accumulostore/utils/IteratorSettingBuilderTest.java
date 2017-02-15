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

package uk.gov.gchq.gaffer.accumulostore.utils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.junit.Test;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IteratorSettingBuilderTest {

    @Test
    public void shouldAddCompactSchemaToIteratorSetting() throws Exception {
        // Given
        final IteratorSetting setting = mock(IteratorSetting.class);
        final Schema schema = mock(Schema.class);
        final String compactSchemaJson = "CompactSchema";

        given(schema.toCompactJson()).willReturn(compactSchemaJson.getBytes());

        // When
        new IteratorSettingBuilder(setting).schema(schema);

        // Then
        verify(setting).addOption(AccumuloStoreConstants.SCHEMA, compactSchemaJson);
    }

    @Test
    public void shouldAddCompactViewToIteratorSetting() throws Exception {
        // Given
        final IteratorSetting setting = mock(IteratorSetting.class);
        final View view = mock(View.class);
        final String compactSchemaJson = "CompactSchema";

        given(view.toCompactJson()).willReturn(compactSchemaJson.getBytes());

        // When
        new IteratorSettingBuilder(setting).view(view);

        // Then
        verify(setting).addOption(AccumuloStoreConstants.VIEW, compactSchemaJson);
    }
}
