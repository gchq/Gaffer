/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.doc.user.walkthrough;

import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;

public abstract class UserWalkthrough extends AbstractWalkthrough {
    public UserWalkthrough(final String header, final String resourcePrefix, final Class<? extends ElementGenerator> generatorClass) {
        super(header, resourcePrefix + "/data.txt", resourcePrefix + "/schema", generatorClass, "doc", "user");
    }

    @Override
    protected String substituteParameters(final String walkthrough) {
        final String walkthroughFormatted = UserWalkthroughStrSubstitutor.substitute(super.substituteParameters(walkthrough, true), this);
        UserWalkthroughStrSubstitutor.validateSubstitution(walkthroughFormatted);
        return walkthroughFormatted;
    }
}
