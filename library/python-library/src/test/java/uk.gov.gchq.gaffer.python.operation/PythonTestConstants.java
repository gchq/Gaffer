/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.python.operation;

public final class PythonTestConstants {

    private PythonTestConstants() {
        // Private constructor to hide default public one
    }

    public static final String LOCALHOST = "127.0.0.1";
    public static final String REPOURI = "https://github.com/g609bmsma/test";
    public static final String REPONAME = "test";
    public static final String CURRENTWORKINGDIRECTORY = "/src/main/resources/.PythonBin";
    public static final Integer MAXPORT = 65535;
    public static final Integer MINPORT = 50000;
    public static final Integer TESTSERVERPORT1 = 7788;
    public static final Integer TESTSERVERPORT2 = 7789;
    public static final Integer TESTSERVERPORT3 = 7790;
}
