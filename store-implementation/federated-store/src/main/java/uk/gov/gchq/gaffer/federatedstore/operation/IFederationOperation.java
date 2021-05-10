/*
 * Copyright 2021 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;

/**
 * {@link IFederationOperation} interface is for special operations used to configure/manipulate/control federation.
 * It has no inteded function outside of federation and should only be handled by the {@link uk.gov.gchq.gaffer.federatedstore.FederatedStore}.
 */
@Since("2.0.0")
public interface IFederationOperation extends Operation {
    boolean userRequestingAdminUsage();

    @JsonProperty("userRequestingAdminUsage")
    default Boolean _userRequestingAdminUsageOrNull() {
        return userRequestingAdminUsage() ? true : null;
    }

    Operation setUserRequestingAdminUsage(final boolean adminRequest);

    abstract class BaseBuilder<OP extends IFederationOperation, B extends Operation.BaseBuilder<OP, ?>> extends Operation.BaseBuilder<OP, B> {
        protected BaseBuilder(final OP op) {
            super(op);
        }

        public B setUserRequestingAdminUsage(final boolean adminRequest) {
            this._getOp().setUserRequestingAdminUsage(adminRequest);
            return _self();
        }
    }

}
