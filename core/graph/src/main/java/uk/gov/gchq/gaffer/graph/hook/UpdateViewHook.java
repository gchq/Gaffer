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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class UpdateViewHook implements GraphHook {

    private Set<String> opAuths;
    private Set<String> dataAuths;
    private byte[] viewToMerge;
    private List<String> restrictedGroups;

    public UpdateViewHook() {
    }

    @JsonGetter("opAuths")
    public Set<String> getOpAuths() {
        return opAuths;
    }

    @JsonGetter("dataAuths")
    public Set<String> getDataAuths() {
        return dataAuths;
    }

    @JsonGetter("viewToMerge")
    public View getViewToMerge() {
        try {
            return JSONSerialiser.deserialise(viewToMerge, View.class);
        } catch (SerialisationException e) {
            throw new RuntimeException("Could not deserialise viewToMerge", e);
        }
    }

    @JsonGetter("restrictedGroups")
    public List<String> getRestrictedGroups() {
        return restrictedGroups;
    }

    @JsonSetter("opAuths")
    public UpdateViewHook setOpAuths(final Set<String> opAuths) {
        this.opAuths = opAuths;
        return this;
    }

    @JsonSetter("dataAuths")
    public UpdateViewHook setDataAuths(final Set<String> dataAuths) {
        this.dataAuths = dataAuths;
        return this;
    }

    @JsonSetter("viewToMerge")
    public UpdateViewHook setViewToMerge(final View viewToMerge) {
        try {
            this.viewToMerge = JSONSerialiser.serialise(viewToMerge, true);
        } catch (SerialisationException e) {
            throw new RuntimeException("Could not serialise the viewToMerge", e);
        }
        return this;
    }

    @JsonSetter("restrictedGroups")
    public UpdateViewHook setRestrictedGroups(final List<String> restrictedGroups) {
        this.restrictedGroups = restrictedGroups;
        return this;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (validateUser(context.getUser())) {
            updateView(opChain);
        }
    }

    public void updateView(final OperationChain<?> opChain) {
        for (Operation operation : opChain.flatten()) {
            if (operation instanceof OperationView) {
                OperationView operationView = (OperationView) operation;

                View viewToMerge = getViewToMerge();

                View opView = new View.Builder()
                        .merge(operationView.getView())
                        .merge(viewToMerge.clone())
                        .build();

                opView.expandGlobalDefinitions();
                Map<String, ViewElementDefinition> entities = opView.getEntities();
                entities.entrySet().removeIf(entry -> restrictedGroups.contains(entry.getKey()));
                Map<String, ViewElementDefinition> edges = opView.getEdges();
                edges.entrySet().removeIf(entry -> restrictedGroups.contains(entry.getKey()));
                operationView.setView(opView);
            }
        }
    }

    final protected boolean validateUser(final User user) {
        boolean dataAuthsValid = isAuthsValid(user.getDataAuths(), dataAuths);
        boolean opAuthsValid = isAuthsValid(user.getOpAuths(), opAuths);
        return dataAuthsValid && opAuthsValid;
    }

    protected static boolean isAuthsValid(final Set<String> userAuths, final Set<String> validAuths) {
        boolean rtn = true;
        if (null != validAuths) {
            if (null == userAuths) {
                rtn = validAuths.isEmpty();
            } else {
                HashSet<String> temp = Sets.newHashSet(validAuths);
                temp.retainAll(userAuths);
                rtn = !temp.isEmpty();
            }
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

    public static class Builder {
        private Set<String> opAuths;
        private Set<String> dataAuths;
        private View viewToMerge;
        private List<String> restrictedGroups;


        public Builder restrictedGroups(final List<String> restrictedGroups) {
            this.restrictedGroups = restrictedGroups;
            return this;
        }

        public Builder viewToMerge(final View viewToMerge) {
            this.viewToMerge = viewToMerge;
            return this;
        }

        public Builder dataAuths(final Set<String> dataAuths) {
            this.dataAuths = dataAuths;
            return this;
        }

        public Builder opAuths(final Set<String> opAuths) {
            this.opAuths = opAuths;
            return this;
        }

        public UpdateViewHook build() {
            return new UpdateViewHook()
                    .setOpAuths(opAuths)
                    .setDataAuths(dataAuths)
                    .setRestrictedGroups(restrictedGroups)
                    .setViewToMerge(viewToMerge);
        }
    }

}