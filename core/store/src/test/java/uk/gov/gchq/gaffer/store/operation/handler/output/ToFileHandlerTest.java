/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.output;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToFile;
import uk.gov.gchq.gaffer.store.Context;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ToFileHandlerTest {
    @Test
    public void shouldOutputToFile(@TempDir Path sharedTempDir) throws OperationException, IOException {
        //Given
        String filePath = sharedTempDir.toString() + "/lines.csv";
        final List<String> linesIn = Arrays.asList(
                ":ID,:LABEL,:TYPE,:START_ID,:END_ID,count:int,DIRECTED:boolean",
                "vertex1,Foo,,,,1,",
                "vertex2,Foo,,,,,",
                ",,Bar,source1,dest1,1,true",
                ",,Bar,source2,dest2,,true"
        );
        final ToFile operation = new ToFile.Builder()
                .input(linesIn)
                .filePath(filePath)
                .build();

        final ToFileHandler handler = new ToFileHandler();

        //When
        handler.doOperation(operation, new Context(), null);

        //Then
        List<String> linesOut = new ArrayList<String>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                linesOut.add(line);
            }
        }
        assertThat(linesIn.containsAll(linesOut));
    }
}
