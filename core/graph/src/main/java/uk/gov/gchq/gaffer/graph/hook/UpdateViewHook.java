/*
 * Copyright 2018 Crown Copyright
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

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View.Builder;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class UpdateViewHook implements GraphHook {
    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (validateUser(context.getUser())) {
            updateView(opChain);
        }
    }

    public void updateView(final OperationChain<?> opChain) {
        for (Operation operation : opChain.getOperations()) {
            if (operation instanceof OperationView) {
                OperationView operationView = (OperationView) operation;

                View viewToMerge = getViewToMerge();

                View opView = new Builder()
                        .merge(operationView.getView())
                        .merge(viewToMerge)
                        .build();

                opView.expandGlobalDefinitions();
                Map<String, ViewElementDefinition> entities = opView.getEntities();
                entities.entrySet().removeIf(entry -> getGetRestrictedGroups().contains(entry.getKey()));
                Map<String, ViewElementDefinition> edges = opView.getEdges();
                edges.entrySet().removeIf(entry -> getGetRestrictedGroups().contains(entry.getKey()));
                operationView.setView(opView);
            }
        }
    }

    protected abstract List<String> getGetRestrictedGroups();

    protected abstract View getViewToMerge();

    protected abstract Set<String> getValidDataAuths();

    protected abstract Set<String> getValidOpAuths();

    private boolean validateUser(final User user) {
        return isDataAuthsValid(user) && isOpsAuthsValid(user);
    }

    private boolean isDataAuthsValid(final User user) {
        Set<String> dataAuths = user.getDataAuths();
        Set<String> validDataAuths = getValidDataAuths();

        boolean rtn = true;
        if (null != validDataAuths) {
            validDataAuths.retainAll(dataAuths);
            rtn = !validDataAuths.isEmpty();
        }
        return rtn;
    }

    private boolean isOpsAuthsValid(final User user) {
        Set<String> opAuths = user.getOpAuths();
        Set<String> validOpAuths = getValidOpAuths();

        boolean rtn = true;
        if (null != validOpAuths) {
            validOpAuths.retainAll(opAuths);
            rtn = !validOpAuths.isEmpty();
        }
        return rtn;
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
    }
}