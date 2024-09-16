/*
 * Copyright 2020-2022 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;

@JsonPropertyOrder(value = {"class", "graphId", "graphAuths", "isPublic"}, alphabetic = true)
@Since("1.11.0")
@Summary("Changes the protection used for accessing graphs")
@JsonInclude(Include.NON_DEFAULT)
public class ChangeGraphAccess implements Output<Boolean>, IFederationOperation {
    @Required
    private String graphId;
    private Set<String> graphAuths = new HashSet<>();
    private Map<String, String> options = new HashMap<>();
    private boolean isPublic = false;
    private String ownerUserId;
    private boolean userRequestingAdminUsage;

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    @Override
    public ChangeGraphAccess shallowClone() throws CloneFailedException {
        final Builder builder = new Builder()
                .graphId(graphId)
                .options(this.options)
                .isPublic(this.isPublic)
                .ownerUserId(this.ownerUserId)
                .setUserRequestingAdminUsage(this.userRequestingAdminUsage);

        if (null != graphAuths) {
            builder.graphAuths(graphAuths.toArray(new String[graphAuths.size()]));
        }

        return builder.build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public Set<String> getGraphAuths() {
        return graphAuths;
    }

    public void setGraphAuths(final Set<String> graphAuths) {
        this.graphAuths = graphAuths;
    }

    public void setIsPublic(final boolean isPublic) {
        this.isPublic = isPublic;
    }

    @Override
    public boolean isUserRequestingAdminUsage() {
        return userRequestingAdminUsage;
    }

    @Override
    public ChangeGraphAccess setUserRequestingAdminUsage(final boolean adminRequest) {
        userRequestingAdminUsage = adminRequest;
        return this;
    }

    public boolean getIsPublic() {
        return isPublic;
    }

    public void setOwnerUserId(final String ownerUserId) {
        this.ownerUserId = ownerUserId;
    }

    public String getOwnerUserId() {
        return this.ownerUserId;
    }

    @Override
    public TypeReference<Boolean> getOutputTypeReference() {
        return new TypeReferenceImpl.Boolean();
    }

    public static class Builder extends IFederationOperation.BaseBuilder<ChangeGraphAccess, ChangeGraphAccess.Builder> {

        public Builder() {
            super(new ChangeGraphAccess());
        }

        protected Builder(final ChangeGraphAccess addGraph) {
            super(addGraph);
        }

        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        public Builder isPublic(final boolean isPublic) {
            _getOp().setIsPublic(isPublic);
            return _self();
        }

        public Builder graphAuths(final String... graphAuths) {
            if (null == graphAuths) {
                _getOp().setGraphAuths(null);
            } else {
                _getOp().setGraphAuths(new HashSet<>(asList(graphAuths)));
            }
            return _self();
        }

        public Builder ownerUserId(final String ownerUserId) {
            _getOp().setOwnerUserId(ownerUserId);
            return _self();
        }
    }
}
