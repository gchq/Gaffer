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
package uk.gov.gchq.gaffer.federatedstore.util;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.nonNull;

public class ApplyViewToElementsFunction implements BiFunction<Object, Iterable<Object>, Iterable<Object>>, ContextSpecificMergeFunction<Object, Iterable<Object>, Iterable<Object>> {

    public static final String MISSING_S = "Context is not complete for %s missing: %s";
    public static final String VIEW = "view";
    public static final String SCHEMA = "schema";
    public static final String TEMP_RESULTS_GRAPH = "tempResultsGraph";
    HashMap<String, Object> context;

    public ApplyViewToElementsFunction() {
    }

    public ApplyViewToElementsFunction(final HashMap<String, Object> context) throws GafferCheckedException {
        this();
        this.context = validate(context);

        try {
            final Store resultsGraph = new MapStore();
            resultsGraph.initialise("TemporaryResultsGraph" + ApplyViewToElementsFunction.class.getSimpleName(), (Schema) context.get(SCHEMA), new MapStoreProperties());
            context.put(TEMP_RESULTS_GRAPH, resultsGraph); //change to putIfAbsent if you want a given store to persist.
        } catch (final Exception e) {
            throw new GafferCheckedException("Unable to create temporary MapStore for results", e);
        }

    }

    @Override
    public ApplyViewToElementsFunction createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException {
        return new ApplyViewToElementsFunction(context);
    }

    private static HashMap<String, Object> validate(final HashMap<String, Object> context) {
        View view = (View) context.get(VIEW);
        if (view == null) {
            context.put(VIEW, new View()); //TODO FS CAN/CAN'T HAVE EMPTY VIEW!?!?!?!
            // throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), VIEW));
        } else if (view.hasTransform()) {
            throw new UnsupportedOperationException("Error: can not use the default merge function with a POST AGGREGATION TRANSFORM VIEW, " +
                    "because transformation may have created items that does not exist in the schema. " +
                    "The re-applying of the View to the collected federated results would not be be possible. " +
                    "Try a simple concat merge that doesn't require the re-application of view");
            //Solution is to derive and use the "Transformed schema" from the uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition.
        }
        Schema schema = (Schema) context.get(SCHEMA);
        if (schema == null) {
            context.put(SCHEMA, new Schema());
            // throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), SCHEMA));
        }
        return context;
    }

    @Override
    public Set<String> getRequiredContextValues() {
        //TODO FS get Required/optional ContextValues too similar to Maestro
        return ImmutableSet.copyOf(new String[]{VIEW, SCHEMA});
    }

    @Override
    public Iterable<Object> apply(final Object first, final Iterable<Object> next) {
        //TODO FS Test
        if (next instanceof Closeable) {
            Closeable closeable = (Closeable) next;
            try {
                closeable.close();
            } catch (final IOException e) {
                throw new GafferRuntimeException("Error closing looped iterable", e);
            }
        }

        final Store resultsGraph = (Store) context.get(TEMP_RESULTS_GRAPH);
        final Context userContext = new Context(new User()/*blankUser for temp graph*/);
        try {
            //TODO FS attention to Input
            resultsGraph.execute(new AddElements.Builder().input((Iterable<Element>) first).build(), userContext);
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error adding elements to temporary graph", e);
        }

        try {
            //TODO FS attention to VIEW
            return (Iterable) resultsGraph.execute(new GetAllElements.Builder().view((View) context.get(VIEW)).build(), userContext);
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error getting all elements from temporary graph", e);
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 47)
                .append(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return nonNull(obj) && this.getClass().equals(obj.getClass());
    }
}
