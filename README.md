# HSQLDB deadlock test

Demonstrates HSQLDB deadlock running CHECKPOINT when using MVCC.

```shell
# Build the jar
./mvnw -DskipTests clean package

# The following command will never terminate due to the deadlock
java -jar target/hsqldb-deadlock-1.0-SNAPSHOT-exec.jar MVCC

# The following command does not deadlock and terminates normally
java -jar target/hsqldb-deadlock-1.0-SNAPSHOT-exec.jar LOCKS
```