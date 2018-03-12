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

package uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;

public class VisibilityParseException extends ParseException {
    private static final long serialVersionUID = -6515226652254225554L;
    private String visibility;

    public VisibilityParseException(final String reason, final byte[] visibility, final int errorOffset) {
        super(reason, errorOffset);
        this.visibility = new String(visibility, StandardCharsets.UTF_8);
    }

    public String getMessage() {
        return super.getMessage() + " in string \'" + this.visibility + "\' at position " + super.getErrorOffset();
    }
}
