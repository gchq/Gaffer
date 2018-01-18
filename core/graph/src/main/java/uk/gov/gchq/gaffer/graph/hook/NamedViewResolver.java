/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook;

import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;

/**
 * A {@link GraphHook} to resolve {@link NamedView}s.
 */
public class NamedViewResolver implements GraphHook {
    private final NamedViewCache cache;

    public NamedViewResolver() {
        cache = new NamedViewCache();
    }

    public NamedViewResolver(final NamedViewCache cache) {
        this.cache = cache;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        resolveViewsInOperations(opChain);
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }

    private void resolveViewsInOperations(final Operations<?> operations) {
        for (final Operation operation : operations.getOperations()) {
            if (operation instanceof OperationView) {
                if (((OperationView) operation).getView() instanceof NamedView) {
                    final View resolvedView = resolveViewInOperation((NamedView) ((OperationView) operation).getView());
                    ((NamedView) resolvedView).setName(null);
                    ((NamedView) ((OperationView) operation).getView()).setName(null);
                    final View viewMergedWithOriginalView = new View.Builder()
                            .merge(resolvedView)
                            .merge(((OperationView) operation).getView())
                            .build();

                    ((OperationView) operation).setView(viewMergedWithOriginalView);
                }
            } else {
                if (operation instanceof Operations) {
                    resolveViewsInOperations((Operations<?>) operation);
                }
            }
        }
    }

    private NamedView resolveViewInOperation(final NamedView namedView) {
        NamedView.Builder newNamedView;
        try {
            NamedViewDetail cachedNamedView = cache.getNamedView(namedView.getName());
            View resolvedCachedView = cachedNamedView.getView(namedView.getParameters());
            newNamedView = new NamedView.Builder()
                    .name(namedView.getName())
                    .merge(resolvedCachedView);

            if (resolvedCachedView instanceof NamedView && null != ((NamedView) resolvedCachedView).getMergedNamedViewNames()) {
                for (final String name : ((NamedView) resolvedCachedView).getMergedNamedViewNames()) {
                    final NamedViewDetail nestedCachedNamedView = cache.getNamedView(name);
                    final NamedView resolvedNestedCacheNamedView = new NamedView.Builder().name(null).merge(nestedCachedNamedView.getView(namedView.getParameters())).build();
                    newNamedView.merge(resolvedNestedCacheNamedView);
                }
            }
        } catch (final CacheOperationFailedException e) {
            // failed to find the namedView in the cache
            throw new RuntimeException(e);
        }
        return newNamedView.build();
    }
}
