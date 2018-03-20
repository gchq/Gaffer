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

package uk.gov.gchq.gaffer.operation.export.graph.handler;

import uk.gov.gchq.gaffer.operation.export.graph.AuthorisedGraphForExportDelegate;
import uk.gov.gchq.gaffer.operation.export.graph.ExportToOtherAuthorisedGraph;
import uk.gov.gchq.gaffer.operation.export.graph.OtherGraphExporter;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.export.ExportToHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExportToOtherAuthorisedGraphHandler extends ExportToHandler<ExportToOtherAuthorisedGraph, OtherGraphExporter> {

    private Map<String, List<String>> idAuths = new HashMap<>();

    public Map<String, List<String>> getIdAuths() {
        return idAuths;
    }

    public void setIdAuths(final Map<String, List<String>> idAuths) {
        if (null == idAuths) {
            this.idAuths = new HashMap<>();
        } else {
            this.idAuths = idAuths;
        }
    }

    @Override
    protected Class<OtherGraphExporter> getExporterClass() {
        return OtherGraphExporter.class;
    }

    @Override
    protected OtherGraphExporter createExporter(final ExportToOtherAuthorisedGraph export, final Context context, final Store store) {
        return new OtherGraphExporter(context, new AuthorisedGraphForExportDelegate.Builder()
                .store(store)
                .graphId(export.getGraphId())
                .parentSchemaIds(export.getParentSchemaIds())
                .parentStorePropertiesId(export.getParentStorePropertiesId())
                .idAuths(idAuths)
                .user(context.getUser())
                .build());
    }
}
