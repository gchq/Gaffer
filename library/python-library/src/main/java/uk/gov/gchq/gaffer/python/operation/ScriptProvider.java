package uk.gov.gchq.gaffer.python.operation;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

import java.io.IOException;

public interface ScriptProvider {
    void pullRepo(final Git git, final String pathAbsolutePythonRepo,
                  final String repoURI) throws GitAPIException;
    void cloneRepo(final Git git, final String pathAbsolutePythonRepo,
                   final String repoURI) throws GitAPIException, IOException;
}
