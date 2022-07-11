// Use default configuration to connect to a Gremlin server on localhost
// and start a session (to persist variables)
:remote connect tinkerpop.server conf/remote.yaml session
// Open the remote console so all following commands are sent to the remote
:remote console
