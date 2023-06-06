package com.pwinckles;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final List<String> MODES = List.of("LOCKS", "MVLOCKS", "MVCC");

    public static void main(String[] args) throws SQLException {
        if (args.length != 1 || !MODES.contains(args[0].toUpperCase())) {
            throw new IllegalArgumentException("Must specify transaction control mode: " + MODES);
        }

        runTest(args[0]);
    }

    private static void runTest(String mode) throws SQLException {
        var executor = Executors.newFixedThreadPool(2);

        try (var conn = newConnection()) {
            setupDb(mode);
            var phaser = new Phaser(2);

            executor.submit(() -> {
                log.info("Before query 1");
                try (var c = newConnection();
                        var statement = c.createStatement()) {
                    c.setAutoCommit(false);
                    selectId("abc", c);

                    phaser.arriveAndAwaitAdvance();
                    TimeUnit.SECONDS.sleep(1);

                    var future = executor.submit(() -> {
                        log.info("Before query 2");
                        try (var c2 = newConnection()) {
                            selectId("123", c2);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        log.info("After query 2");
                    });

                    future.get();
                    statement.execute("COMMIT");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                log.info("After query 1");
            });

            phaser.arriveAndAwaitAdvance();
            log.info("Before checkpoint");
            checkpoint();
            log.info("After checkpoint");
        } finally {
            executor.shutdownNow();
        }
    }

    private static void checkpoint() throws SQLException {
        try (var conn = newConnection();
                var statement = conn.createStatement()) {
            statement.execute("CHECKPOINT");
        }
    }

    private static Long selectId(String value, Connection conn) throws SQLException {
        try (var statement = conn.prepareStatement("SELECT * FROM test WHERE value = ?")) {
            statement.setString(1, value);
            var rs = statement.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return null;
    }

    private static void setupDb(String mode) throws SQLException {
        try (var conn = newConnection();
                var statement = conn.createStatement()) {
            statement.execute("SET DATABASE TRANSACTION CONTROL " + mode);
            statement.execute("CREATE TABLE test (id IDENTITY PRIMARY KEY, value VARCHAR(255))");
            statement.execute("INSERT INTO test (value) VALUES ('abc')");
            statement.execute("INSERT INTO test (value) VALUES ('123')");
        }
    }

    private static Connection newConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:hsqldb:mem:testdb", "SA", "");
    }
}
