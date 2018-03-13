/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.export.set;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.SetExporter;
import uk.gov.gchq.gaffer.store.Context;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ExportToSetHandlerTest {

    @Test
    public void shouldHandleNullInput() throws Exception {
        // Given
        final ExportToSet<Integer> exportToSet = new ExportToSet.Builder<Integer>()
                .input(null)
                .key("elements")
                .build();

        final Context context = new Context();
        context.addExporter(new SetExporter());

        final ExportToSetHandler handler = new ExportToSetHandler();

        // When
        final Object result = handler.doOperation(exportToSet, context, null);

        // Then
        assertThat(result, is(nullValue()));
    }
}
