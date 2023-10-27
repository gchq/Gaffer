/*
 * Copyright 2022-2023 Crown Copyright
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
import uk.gov.gchq.gaffer.store.schema.exception.SplitElementGroupDefSchemaException;
import uk.gov.gchq.gaffer.store.schema.exception.VertexSerialiserSchemaException;
import uk.gov.gchq.gaffer.store.schema.exception.VisibilityPropertySchemaException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.store.schema.exception.VertexSerialiserSchemaException.VERTEX_SERIALISER;
import static uk.gov.gchq.gaffer.store.schema.exception.VisibilityPropertySchemaException.VISIBILITY_PROPERTY;

public class MergeSchema implements BiFunction<Schema, Schema, Schema>, ContextSpecificMergeFunction<Schema, Schema, Schema> {
    public static final String WIPE_VERTEX_SERIALISERS = "Wipe_Vertex_Serialisers";
    public static final String WIPE_VISIBILITY_PROPERTY = "Wipe_Visibility_Property";
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeSchema.class);
    public static final String FORMAT_CAUGHT_SCHEMA_EXCEPTION_ATTEMPTING_TO_RE_MERGE_BUT_WITHOUT_S_ERROR_MESSAGE_S = "Caught SchemaException, attempting to re-merge but without %s. Error message:%s";
    public static final String MERGE_FUNCTION_UNABLE_TO_RECOVER_FROM_ERROR_DUE_TO = MergeSchema.class.getSimpleName() + " function unable to recover from error, due to: ";
    public static final String MATCHING_ELEMENT_GROUPS_HAVING_NO_SHARED_PROPERTIES_CAUSED_BY = "Matching element groups having no shared properties, caused by:";
    private HashMap<String, Object> context;

    public MergeSchema() {
    }

    public MergeSchema(final HashMap<String, Object> context) {
        this();
        this.context = new HashMap<>(validate(context));
    }

    public Schema apply(final Schema update, final Schema state) {
        if (state == null) {
            return nonNull(update) ? update : new Schema();
        } else {
            //Check if Vertex Serialiser needs to be wiped due to previous clash with merging.
            final Serialiser vertexSerialiser = (boolean) context.getOrDefault(WIPE_VERTEX_SERIALISERS, false)
                    ? null
                    : update.getVertexSerialiser();

            //Check if visibility property needs to be wiped due to previous clash with merging.
            final String visibilityProperty = (boolean) context.getOrDefault(WIPE_VISIBILITY_PROPERTY, false)
                    ? null
                    : update.getVisibilityProperty();

            Schema.Builder mergeSchema = new Schema.Builder(state);

            try {
                mergeSchema.merge(new Schema.Builder(update)
                        .vertexSerialiser(vertexSerialiser)
                        .visibilityProperty(visibilityProperty)
                        .build());

            } catch (final VertexSerialiserSchemaException e) {
                LOGGER.error(String.format(FORMAT_CAUGHT_SCHEMA_EXCEPTION_ATTEMPTING_TO_RE_MERGE_BUT_WITHOUT_S_ERROR_MESSAGE_S, VERTEX_SERIALISER, e.getMessage()));
                //Clashing Vertex Serialiser is possibly a recoverable state, continue without using Vertex Serialisers, retain this state.
                context.put(WIPE_VERTEX_SERIALISERS, true);
                mergeSchema.merge(new Schema.Builder(update)
                        .vertexSerialiser(null)
                        .build());
                mergeSchema.vertexSerialiser(null);
            } catch (final VisibilityPropertySchemaException e) {
                LOGGER.error(String.format(FORMAT_CAUGHT_SCHEMA_EXCEPTION_ATTEMPTING_TO_RE_MERGE_BUT_WITHOUT_S_ERROR_MESSAGE_S, VISIBILITY_PROPERTY, e.getMessage()));
                //Clashing visibility property is possibly a recoverable state, continue without using visibility property, retain this state.
                context.put(WIPE_VISIBILITY_PROPERTY, true);
                mergeSchema.merge(new Schema.Builder(update)
                        .visibilityProperty(null)
                        .build());
                mergeSchema.visibilityProperty(null);

            } catch (final SplitElementGroupDefSchemaException e) {
                // Will require significant effort in Federation to resolve this.
                throw e.preAppendToMessage(MATCHING_ELEMENT_GROUPS_HAVING_NO_SHARED_PROPERTIES_CAUSED_BY)
                        .preAppendToMessage(MERGE_FUNCTION_UNABLE_TO_RECOVER_FROM_ERROR_DUE_TO);
            } catch (final SchemaException e) {
                // Not possible to resolve from these collisions.
                throw e.preAppendToMessage(MERGE_FUNCTION_UNABLE_TO_RECOVER_FROM_ERROR_DUE_TO);
            } catch (final Exception e) {
                // all other errors.
                throw new SchemaException(MERGE_FUNCTION_UNABLE_TO_RECOVER_FROM_ERROR_DUE_TO + e.getMessage(), e);
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
