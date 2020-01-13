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
package uk.gov.gchq.gaffer.script.operation.provider;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.script.operation.DockerFileUtils;
import uk.gov.gchq.gaffer.script.operation.GitServerUtils;
import uk.gov.gchq.gaffer.script.operation.ScriptTestConstants;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.lang.Thread.sleep;

public class GitScriptProviderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GitScriptProviderTest.class);
    private static GitServerUtils gitServerUtils;

    private final GitScriptProvider gsp = new GitScriptProvider();
    private static final String CURRENT_WORKING_DIRECTORY = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
    private static final String DIRECTORY_PATH = CURRENT_WORKING_DIRECTORY.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
    private static final Path PATH_ABSOLUTE_SCRIPT_REPO = DockerFileUtils.getPathAbsoluteScriptRepo(DIRECTORY_PATH, ScriptTestConstants.REPO_DIR);
    private static final Path PATH_ABSOLUTE_CLONED_SCRIPT_REPO = Paths.get(DockerFileUtils.getPathAbsoluteScriptRepo(DIRECTORY_PATH, ScriptTestConstants.REPO_DIR).toString() + "2/" + ScriptTestConstants.REPO_NAME);

    @Test
    @Order(1)
    public void shouldCloneIfNotAlreadyCloned() {
        //Given
        try {
            gitServerUtils = new GitServerUtils(null);
            gitServerUtils.start();
            sleep(4000);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        // When
        gsp.retrieveScripts(PATH_ABSOLUTE_CLONED_SCRIPT_REPO.toString(), ScriptTestConstants.REPO_URI);
        final String[] files = PATH_ABSOLUTE_CLONED_SCRIPT_REPO.toFile().list();

        // Then
        Assert.assertNotNull(files);

        try {
            gitServerUtils.stopServer();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {
            gitServerUtils = null;
        }
    }

    @Test (expected = Test.None.class /* no exception expected */)
    @Order(2)
    public void shouldPullIfAlreadyCloned() {
        //Given
        try {
            gitServerUtils = new GitServerUtils(Paths.get(PATH_ABSOLUTE_SCRIPT_REPO.toString() + "2/test"));
            gitServerUtils.start();
            sleep(4000);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }

        //When
        gsp.retrieveScripts(PATH_ABSOLUTE_SCRIPT_REPO.toString() + "2", ScriptTestConstants.REPO_URI);
        final String[] files = PATH_ABSOLUTE_CLONED_SCRIPT_REPO.toFile().list();

        // Then
        Assert.assertNotNull(files);

        try {
            gitServerUtils.stopServer();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            Path pathAbsoluteScriptRepo2 = Paths.get(PATH_ABSOLUTE_SCRIPT_REPO.toString() + "2");
            File fileName = pathAbsoluteScriptRepo2.toFile();

            deleteDirectory(fileName);

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    private static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }

        directoryToBeDeleted.delete();
    }
}

