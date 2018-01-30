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

import org.apache.commons.lang.StringUtils;

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

import java.util.Map;

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

    private View resolveViewInOperation(final NamedView namedView) {
        View.Builder viewBuilder = new View.Builder();

        viewBuilder.merge(resolveNamedView(namedView.getName(), namedView.getParameters()));

        if (null != namedView.getMergedNamedViewNames()) {
            for (String name : namedView.getMergedNamedViewNames()) {
                viewBuilder.merge(resolveNamedView(name, namedView.getParameters()));
            }
        }
        return viewBuilder.build();
    }

    private View resolveNamedView(final String namedViewName, final Map<String, Object> parameters) {
        View.Builder fullyResolvedView = new View.Builder();
        try {
            NamedViewDetail cachedNamedView = cache.getNamedView(namedViewName);
            if (null != cachedNamedView) {
                View resolvedCachedView = cachedNamedView.getView(parameters);

                if (resolvedCachedView instanceof NamedView) {
                    ((NamedView) resolvedCachedView).setName(null);
                    fullyResolvedView.merge(resolvedCachedView);
                    if (null != ((NamedView) resolvedCachedView).getMergedNamedViewNames()
                            && !((NamedView) resolvedCachedView).getMergedNamedViewNames().isEmpty()) {
                        for (String name : ((NamedView) resolvedCachedView).getMergedNamedViewNames()) {
                            if (StringUtils.isNotEmpty(name)) {
                                fullyResolvedView.merge(resolveNamedView(name, parameters));
                            }
                        }
                    }
                } else {
                    fullyResolvedView.merge(resolvedCachedView);
                }
            }
        } catch (final CacheOperationFailedException e) {
            // failed to find the namedView in the cache.
            // ignore this as users might know it isn't in there.
        }

        return fullyResolvedView.build();
    }
}
