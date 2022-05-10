/*
 * Copyright 2018-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

@JsonPropertyOrder(
        value = {"class", "graphId"},
        alphabetic = true
)
@Since("1.4.0")
@Summary("Adds a new Graph with hooks to the federated store")
public class AddGraphWithHooks extends AddGraph {
    private GraphHook[] hooks;

    @Override
    public AddGraphWithHooks shallowClone() throws CloneFailedException {
        Builder builder = new Builder()
                .graphId(getGraphId())
                .schema(getSchema())
                .storeProperties(getStoreProperties())
                .parentSchemaIds(getParentSchemaIds())
                .parentPropertiesId(getParentPropertiesId())
                .options(getOptions())
                .disabledByDefault(isDisabledByDefault())
                .isPublic(getIsPublic())
                .readAccessPredicate(getReadAccessPredicate())
                .writeAccessPredicate(getWriteAccessPredicate())
                .userRequestingAdminUsage(isUserRequestingAdminUsage())
                .hooks(hooks);

        if (null != getGraphAuths()) {
            builder.graphAuths(getGraphAuths().toArray(new String[getGraphAuths().size()]));
        }

        return builder.build();
    }

    public GraphHook[] getHooks() {
        return hooks;
    }

    public void setHooks(final GraphHook[] hooks) {
        this.hooks = hooks;
    }

    public static class Builder extends GraphBuilder<AddGraphWithHooks, Builder> {
        public Builder() {
            super(new AddGraphWithHooks());
        }

        public Builder hooks(final GraphHook... hooks) {
            _getOp().setHooks(hooks);
            return _self();
        }
    }
}
