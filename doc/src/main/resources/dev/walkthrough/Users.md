${HEADER}

${CODE_LINK}

When interacting with Gaffer you need to use a ${USER_JAVADOC}. The user object
contains:
- userId - unique identifier for the user
- dataAuths - user authorisations for accessing data
- opAuths - user authorisations for running different Gaffer operations

The execute methods on Graph requires you to pass in an instance of User.
This relies on you creating an instance that represents you, with your unique id
and authorisations.

For users of Gaffer, interacting in Java is insecure as you are able to set
any authorisations you want to. However, as you are writing Java it is assumed
that you have access to the store.properties file so you have access to the
connection details (including password) for your Accumulo/HBase cluster. This
means that if you wanted to you could by-pass Gaffer and read all the data
directly on disk if you wanted to.

The security layer for Gaffer is currently only enforced by a REST API. We
recommend restricting users so they do not have access to the cluster (Accumulo, HBase, etc.)
and only allowing users to connect to Gaffer via the REST API. In the REST API
the User object is constructed via a UserFactory. Currently we only provide one
implementation of this, the UnknownUserFactory. This UnknownUserFactory will
always just return 'new User()'.

To authenticate your users, you will need to extend the REST API and add your
chosen authentication mechanism. This will probably involve updating the web.xml to include
your security roles. Based on that mechanism, you will then need to
implement your own UserFactory class that creates a new User instance based on
the user making the REST API request. This could may involve making a call to your LDAP server.
For example you could use the authorization header in the request:

```java
public class LdapUserFactory implements UserFactory {

    @Context
    private HttpHeaders httpHeaders;

    public User createUser() {
        final String authHeaderValue = httpHeaders.getHeaderString(HttpHeaders.AUTHORIZATION); // add logic to fetch userId
        final String userId = null; // extract from authHeaderValue
        final List<String> opAuths = null; // fetch op auths for userId
        final List<String> dataAuths = null; // fetch op auths for userId
        return new User.Builder()
                .userId(userId)
                .opAuths(opAuths)
                .dataAuths(dataAuths)
                .build();
    }
}
```
