package uk.gov.gchq.gaffer.python.operation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.FileSystems;
import java.nio.file.Path;

public class LocalDockerPlatformTest {

    @Before
    public void setup() {
        GitScriptProvider scriptProvider = new GitScriptProvider();
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
        Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);
        scriptProvider.getScripts(pathAbsoluteScriptRepo.toString(), ScriptTestConstants.REPO_URI);
    }

    @Test
    public void shouldCreateAContainer() {
        // Given
        LocalDockerPlatform platform = new LocalDockerPlatform();
        String scriptName = "script1";
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/" + ".ScriptBin");

        // When
        Container container = platform.createContainer(scriptName, null, directoryPath, ScriptTestConstants.LOCALHOST);

        // Then
        Assert.assertTrue(container instanceof LocalDockerContainer);
        Assert.assertNotNull(container);
    }
}
