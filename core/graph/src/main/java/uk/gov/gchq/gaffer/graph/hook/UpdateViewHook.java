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
import java.util.Map.Entry;
import java.util.Set;


public class UpdateViewHook implements GraphHook {

    private Set<String> withOpAuth;
    private Set<String> withoutOpAuth;
    private Set<String> withDataAuth;
    private Set<String> withoutDataAuth;
    private List<String> whiteListElementGroups;
    private List<String> blackListElementGroups;
    private byte[] viewToMerge;

    public UpdateViewHook() {
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (applyToUser(context.getUser())) {
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
                entities.entrySet().removeIf(this::removeElementGroups);

                Map<String, ViewElementDefinition> edges = opView.getEdges();
                edges.entrySet().removeIf(this::removeElementGroups);
                operationView.setView(opView);
            }
        }
    }

    protected boolean removeElementGroups(final Entry<String, ViewElementDefinition> entry) {
        boolean remove;

        if (null != whiteListElementGroups) {
            remove = !whiteListElementGroups.contains(entry.getKey());
        } else {
            remove = false;
        }

        if (!remove) {
            if (null != blackListElementGroups) {
                remove = blackListElementGroups.contains(entry.getKey());
            }
        }
        return remove;
    }

    final protected boolean applyToUser(final User user) {
        boolean withDataAuth = validateAuths(user.getDataAuths(), this.withDataAuth, true);
        boolean withOpAuth = validateAuths(user.getOpAuths(), this.withOpAuth, true);
        boolean withoutDataAuth = validateAuths(user.getDataAuths(), this.withoutDataAuth, false);
        boolean withoutOpAuth = validateAuths(user.getOpAuths(), this.withoutOpAuth, false);

        return withDataAuth && withOpAuth && !withoutDataAuth && !withoutOpAuth;
    }

    protected static boolean validateAuths(final Set<String> userAuths, final Set<String> validAuth, final boolean ifValidAuthIsNull) {
        boolean rtn = ifValidAuthIsNull;
        if (null != validAuth) {
            if (null == userAuths) {
                rtn = validAuth.isEmpty();
            } else {
                HashSet<String> temp = Sets.newHashSet(validAuth);
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

    @JsonGetter("withOpAuth")
    public Set<String> getWithOpAuth() {
        return withOpAuth;
    }

    @JsonSetter("withOpAuth")
    public UpdateViewHook setWithOpAuth(final Set<String> withOpAuth) {
        this.withOpAuth = withOpAuth;
        return this;
    }

    @JsonGetter("withoutOpAuth")
    public Set<String> getWithoutOpAuth() {
        return withoutOpAuth;
    }

    @JsonSetter("withoutOpAuth")
    public UpdateViewHook setWithoutOpAuth(final Set<String> withoutOpAuth) {
        this.withoutOpAuth = withoutOpAuth;
        return this;
    }

    @JsonGetter("withDataAuth")
    public Set<String> getWithDataAuth() {
        return withDataAuth;
    }

    @JsonSetter("withDataAuth")
    public UpdateViewHook setWithDataAuth(final Set<String> withDataAuth) {
        this.withDataAuth = withDataAuth;
        return this;
    }

    @JsonGetter("withoutDataAuth")
    public Set<String> getWithoutDataAuth() {
        return withoutDataAuth;
    }

    @JsonSetter("withoutDataAuth")
    public UpdateViewHook setWithoutDataAuth(final Set<String> withoutDataAuth) {
        this.withoutDataAuth = withoutDataAuth;
        return this;
    }

    @JsonGetter("whiteListElementGroups")
    public List<String> getWhiteListElementGroups() {
        return whiteListElementGroups;
    }

    @JsonSetter("whiteListElementGroups")
    public UpdateViewHook setWhiteListElementGroups(final List<String> whiteListElementGroups) {
        this.whiteListElementGroups = whiteListElementGroups;
        return this;
    }

    @JsonGetter("blackListElementGroups")
    public List<String> getBlackListElementGroups() {
        return blackListElementGroups;
    }

    @JsonSetter("blackListElementGroups")
    public UpdateViewHook setBlackListElementGroups(final List<String> blackListElementGroups) {
        this.blackListElementGroups = blackListElementGroups;
        return this;
    }


    @JsonSetter("viewToMerge")
    public UpdateViewHook setViewToMerge(final View viewToMerge) {
        this.viewToMerge = getViewToMerge(viewToMerge);
        return this;
    }

    private static byte[] getViewToMerge(final View viewToMerge) {
        byte[] serialise = null;
        if (null != viewToMerge) {
            try {
                serialise = JSONSerialiser.serialise(viewToMerge, true);
            } catch (SerialisationException e) {
                throw new RuntimeException("Could not serialise the viewToMerge", e);
            }
        }
        return serialise;
    }

    @JsonGetter("viewToMerge")
    public View getViewToMerge() {
        return getViewToMerge(viewToMerge);
    }

    private static View getViewToMerge(final byte[] viewToMerge) {
        View deserialise;
        if (null != viewToMerge) {
            try {
                deserialise = JSONSerialiser.deserialise(viewToMerge, View.class);
            } catch (SerialisationException e) {
                throw new RuntimeException("Could not deserialise viewToMerge", e);
            }
        } else {
            deserialise = null;
        }
        return deserialise;
    }

    public static class Builder {
        private Set<String> withOpAuth;
        private Set<String> withoutOpAuth;
        private Set<String> withDataAuths;
        private Set<String> withoutDataAuth;
        private List<String> whiteListElementGroups;
        private List<String> blackListElementGroups;
        private byte[] viewToMerge;

        public Builder withOpAuth(final Set<String> withOpAuth) {
            this.withOpAuth = withOpAuth;
            return this;
        }

        public Builder withoutOpAuth(final Set<String> withoutOpAuth) {
            this.withoutOpAuth = withoutOpAuth;
            return this;
        }

        public Builder withDataAuths(final Set<String> withDataAuths) {
            this.withDataAuths = withDataAuths;
            return this;
        }

        public Builder withoutDataAuth(final Set<String> withoutDataAuth) {
            this.withoutDataAuth = withoutDataAuth;
            return this;
        }

        public Builder whiteListElementGroups(final List<String> whiteListElementGroups) {
            this.whiteListElementGroups = whiteListElementGroups;
            return this;
        }

        public Builder blackListElementGroups(final List<String> blackListElementGroups) {
            this.blackListElementGroups = blackListElementGroups;
            return this;
        }

        public Builder setViewToMerge(final View viewToMerge) {
            this.viewToMerge = UpdateViewHook.getViewToMerge(viewToMerge);
            return this;
        }

        private View getViewToMerge() {
            return UpdateViewHook.getViewToMerge(viewToMerge);

        }

        public UpdateViewHook build() {
            return new UpdateViewHook()
                    .setWithOpAuth(withOpAuth)
                    .setWithoutOpAuth(withoutOpAuth)
                    .setWithDataAuth(withDataAuths)
                    .setWithoutDataAuth(withoutDataAuth)
                    .setWhiteListElementGroups(whiteListElementGroups)
                    .setBlackListElementGroups(blackListElementGroups)
                    .setViewToMerge(getViewToMerge());
        }
    }

}