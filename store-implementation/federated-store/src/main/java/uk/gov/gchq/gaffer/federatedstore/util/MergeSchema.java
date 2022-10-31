/*
 * Copyright 2017-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

public class MergeSchema implements BiFunction<Schema, Schema, Schema>, ContextSpecificMergeFunction<Schema, Schema, Schema> {
    public static final String WIPE_VERTEX_SERIALISERS = "wipe_vertex_serialisers";
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeSchema.class);
    private HashMap<String, Object> context;

    public MergeSchema() {
    }

    public MergeSchema(final HashMap<String, Object> context) {
        this();
        this.context = new HashMap<>(validate(context));
    }

    public Schema apply(final Schema update, final Schema state) {
        if (state == null) {
            return update;
        } else {
            Schema.Builder mergeSchema = new Schema.Builder(state);
            //Check if Vertex Serialiser needs to be wiped due to previous clash with merging.
            final Serialiser vertexSerialiser = (boolean) context.getOrDefault(WIPE_VERTEX_SERIALISERS, false)
                    ? null
                    : update.getVertexSerialiser();
            try {
                mergeSchema.merge(new Schema.Builder(update).vertexSerialiser(vertexSerialiser).build());
            } catch (final SchemaException e) {
                if (e.getMessage().contains(Schema.UNABLE_TO_MERGE_SCHEMAS_CONFLICT_WITH_VERTEX_SERIALISER_OPTIONS_ARE)) {
                    LOGGER.error(String.format("Caught SchemaException, attempting to re-merge but without vertex serialisers. Error message:%s", e.getMessage()));
                    //Clashing Vertex Serialiser is possibly a recoverable state, continue without using Vertex Serialisers, retain this state.
                    context.put(WIPE_VERTEX_SERIALISERS, true);
                    mergeSchema.merge(new Schema.Builder(update).vertexSerialiser(null).build());
                    mergeSchema.vertexSerialiser(null);
                }
            }
            return mergeSchema.build();
        }
    }

    @Override
    public ContextSpecificMergeFunction<Schema, Schema, Schema> createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException {
        return new MergeSchema(context);
    }


    @Override
    public Set<String> getRequiredContextValues() {
        return Collections.emptySet();
    }

    private static HashMap<String, Object> validate(final HashMap<String, Object> context) {
        return context;
    }
}
