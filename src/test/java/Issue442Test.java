import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Issue442Test {

  private static final int PORT = 27018;
  private static final String USERNAME_ADMIN = "admin-user";
  private static final String PASSWORD_ADMIN = "admin-password";
  private static final String USERNAME_NORMAL_USER = "test-db-user";
  private static final String PASSWORD_NORMAL_USER = "test-db-user-password";
  private static final String DB_ADMIN = "admin";
  private static final String DB_TEST = "test-db";
  private static final String COLL_TEST = "test-coll";

  @Test
  public void customRole() throws Exception {
    final var roleName = "listColls";
    final var net = Net.of("localhost", PORT, false);
    final var args = MongodArguments.defaults().withAuth(true);
    try (final var running = startMongod(net, args)) {
      final var address = getServerAddress(running);
      try (final var clientWithoutCredentials = new MongoClient(address)) {
        // Create admin user.
        clientWithoutCredentials.getDatabase(DB_ADMIN)
          .runCommand(commandCreateUser(USERNAME_ADMIN, PASSWORD_ADMIN, "root"));
      }
      final var credentialAdmin =
        MongoCredential.createCredential(USERNAME_ADMIN, DB_ADMIN, PASSWORD_ADMIN.toCharArray());
      try (final var clientAdmin = new MongoClient(address, credentialAdmin, MongoClientOptions.builder().build())) {
        final var db = clientAdmin.getDatabase(DB_TEST);
        // Create role allowing the "listCollections" action.
        db.runCommand(commandCreateRole(DB_TEST, COLL_TEST, roleName, List.of("listCollections")));
        // Create normal user and grant them the custom role.
        db.runCommand(commandCreateUser(USERNAME_NORMAL_USER, PASSWORD_NORMAL_USER, roleName));
        // Create collection.
        db.getCollection(COLL_TEST).insertOne(new Document(Map.of("key", "value")));
      }
      final var credentialNormalUser =
        MongoCredential.createCredential(USERNAME_NORMAL_USER, DB_TEST, PASSWORD_NORMAL_USER.toCharArray());
      try (final var clientNormalUser =
             new MongoClient(address, credentialNormalUser, MongoClientOptions.builder().build())) {
        final var expected = List.of(COLL_TEST);
        // FIXME This unexpectedly fails with "not authorized on test-db to execute command { listCollections: 1, ...}".
        final var actual = clientNormalUser.getDatabase(DB_TEST).listCollectionNames().into(new ArrayList<>());
        Assertions.assertIterableEquals(expected, actual);
      }
    }
  }

  // FIXME The assertions in this test work, but the test throws a de.flapdoodle.reverse.TearDownException at the end.
  @Test
  public void readAnyDatabaseRole() throws Exception {
    final var net = Net.of("localhost", PORT, false);
    final var args = MongodArguments.defaults().withAuth(true);
    try (final var running = startMongod(net, args)) {
      final var address = getServerAddress(running);
      try (final var clientWithoutCredentials = new MongoClient(address)) {
        // Create admin user.
        clientWithoutCredentials.getDatabase(DB_ADMIN)
          .runCommand(commandCreateUser(USERNAME_ADMIN, PASSWORD_ADMIN, "root"));
      }
      final var credentialAdmin =
        MongoCredential.createCredential(USERNAME_ADMIN, DB_ADMIN, PASSWORD_ADMIN.toCharArray());
      try (final var clientAdmin = new MongoClient(address, credentialAdmin, MongoClientOptions.builder().build())) {
        // Create normal user and grant them the builtin "readAnyDatabase" role.
        final var dbAdmin = clientAdmin.getDatabase(DB_ADMIN);
        dbAdmin.runCommand(commandCreateUser(USERNAME_NORMAL_USER, PASSWORD_NORMAL_USER, "readAnyDatabase"));
        // Create collection.
        final var dbTest = clientAdmin.getDatabase(DB_TEST);
        dbTest.getCollection(COLL_TEST).insertOne(new Document(Map.of("key", "value")));
      }
      final var credentialNormalUser =
        MongoCredential.createCredential(USERNAME_NORMAL_USER, DB_ADMIN, PASSWORD_NORMAL_USER.toCharArray());
      try (final var clientNormalUser =
             new MongoClient(address, credentialNormalUser, MongoClientOptions.builder().build())) {
        final var expected = List.of(COLL_TEST);
        final var actual = clientNormalUser.getDatabase(DB_TEST).listCollectionNames().into(new ArrayList<>());
        Assertions.assertIterableEquals(expected, actual);
      }
    }
  }

  // FIXME The assertions in this test work, but the test throws a de.flapdoodle.reverse.TearDownException at the end.
  @Test
  public void readRole() throws Exception {
    final var net = Net.of("localhost", PORT, false);
    final var args = MongodArguments.defaults().withAuth(true);
    try (final var running = startMongod(net, args)) {
      final var address = getServerAddress(running);
      try (final var clientWithoutCredentials = new MongoClient(address)) {
        // Create admin user.
        clientWithoutCredentials.getDatabase(DB_ADMIN)
          .runCommand(commandCreateUser(USERNAME_ADMIN, PASSWORD_ADMIN, "root"));
      }
      final var credentialAdmin =
        MongoCredential.createCredential(USERNAME_ADMIN, DB_ADMIN, PASSWORD_ADMIN.toCharArray());
      try (final var clientAdmin = new MongoClient(address, credentialAdmin, MongoClientOptions.builder().build())) {
        final var db = clientAdmin.getDatabase(DB_TEST);
        // Create normal user and grant them the builtin "read" role.
        db.runCommand(commandCreateUser(USERNAME_NORMAL_USER, PASSWORD_NORMAL_USER, "read"));
        // Create collection.
        db.getCollection(COLL_TEST).insertOne(new Document(Map.of("key", "value")));
      }
      final var credentialNormalUser =
        MongoCredential.createCredential(USERNAME_NORMAL_USER, DB_TEST, PASSWORD_NORMAL_USER.toCharArray());
      try (final var clientNormalUser =
             new MongoClient(address, credentialNormalUser, MongoClientOptions.builder().build())) {
        final var expected = List.of(COLL_TEST);
        final var actual = clientNormalUser.getDatabase(DB_TEST).listCollectionNames().into(new ArrayList<>());
        Assertions.assertIterableEquals(expected, actual);
      }
    }
  }

  private static TransitionWalker.ReachedState<RunningMongodProcess> startMongod(
    final Net net,
    final MongodArguments arguments
  ) {
    return Mongod.builder()
      .net(Start.to(Net.class).initializedWith(net))
      .mongodArguments(Start.to(MongodArguments.class).initializedWith(arguments))
      .build()
      .start(Version.Main.V4_4);
  }

  private static ServerAddress getServerAddress(
    final TransitionWalker.ReachedState<RunningMongodProcess> running
  ) throws Exception {
    final var address = running.current().getServerAddress();
    return new ServerAddress(address.getHost(), address.getPort());
  }

  private static Document commandCreateRole(String database, String collection, String roleName, List<String> actions) {
    return new Document("createRole", roleName).append(
      "privileges", List.of(
        new Document("resource", new Document("db", database).append("collection", collection))
          .append("actions", actions)
      )
    ).append("roles", Collections.emptyList());
  }

  private static Document commandCreateUser(
    final String username,
    final String password,
    final String... roles
  ) {
    return new Document("createUser", username)
      .append("pwd", password)
      .append("roles", Arrays.asList(roles));
  }
}
