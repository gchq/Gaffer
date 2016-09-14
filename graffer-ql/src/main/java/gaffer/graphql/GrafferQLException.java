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

package gaffer.graphql;

/**
 * General purpose exception for errors encountered during Gaffer to GraphQL conversion.
 */
public class GrafferQLException extends Exception {
    public GrafferQLException(final String s) {
        super(s);
    }

    public GrafferQLException(final String message,
                              final Throwable cause,
                              final boolean enableSuppression,
                              final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public GrafferQLException(final String message,
                              final Throwable cause) {
        super(message, cause);
    }

    public GrafferQLException(final Throwable cause) {
        super(cause);
    }
}
