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

package uk.gov.gchq.gaffer.store.operation.handler.named;

import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;

import java.util.Map;

/**
 * Operation handler for {@link AddNamedView} which adds a NamedViewDetail to the cache.
 */
public class AddNamedViewHandler implements OperationHandler<AddNamedView> {
    private final NamedViewCache cache;

    public AddNamedViewHandler() {
        this(new NamedViewCache());
    }

    public AddNamedViewHandler(final NamedViewCache cache) {
        this.cache = cache;
    }

    /**
     * Adds a {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} to the NamedViewCache.  If no cache is specified it will created a new {@link NamedViewCache}.
     * The {@link AddNamedView} name field must be set and cannot be left empty, or the build() method will fail and a runtime exception will be
     * thrown. The handler then adds/overwrites the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail} according to an overwrite flag.
     *
     * @param operation the {@link uk.gov.gchq.gaffer.operation.Operation} containing the {@link uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView} to be added to cache
     * @param context   the {@link Context}
     * @param store     the {@link Store} the operation should be run on
     * @return null (since the output is void)
     * @throws OperationException if the addition to the cache fails
     */
    @Override
    public Object doOperation(final AddNamedView operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getName() || operation.getName().isEmpty()) {
            throw new IllegalArgumentException("NamedView name must be set and not empty");
        }

        final NamedViewDetail namedViewDetail = new NamedViewDetail.Builder()
                .name(operation.getName())
                .view(operation.getViewAsString())
                .description(operation.getDescription())
                .creatorId(context.getUser().getUserId())
                .writers(operation.getWriteAccessRoles())
                .parameters(operation.getParameters())
                .build();

        validate(namedViewDetail.getViewWithDefaultParams(), namedViewDetail);

        try {
            cache.addNamedView(namedViewDetail, operation.isOverwriteFlag(), context.getUser(), store.getProperties().getAdminAuth());
        } catch (final CacheOperationFailedException e) {
            throw new OperationException(e.getMessage(), e);
        }
        return null;
    }

    private void validate(final View namedViewWithDefaultParams, final NamedViewDetail namedViewDetail) throws OperationException {
        if (namedViewWithDefaultParams instanceof NamedView) {
            throw new OperationException("NamedView can not be nested within NamedView");
        }

        if (null != namedViewDetail.getParameters()) {
            String viewString = namedViewDetail.getView();
            for (final Map.Entry<String, ViewParameterDetail> parameterDetail : namedViewDetail.getParameters().entrySet()) {
                String varName = "${" + parameterDetail.getKey() + "}";
                if (!viewString.contains(varName)) {
                    throw new OperationException("Parameter specified in NamedView doesn't occur in View string for " + varName);
                }
            }
        }
    }
}
