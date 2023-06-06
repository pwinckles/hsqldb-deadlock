# List available commands
default:
    just --list

# Build
build:
    ./mvnw -DskipTests clean package

# Install into local M2
install: build
    ./mvnw -DskipTests clean install

# Run tests
test:
    ./mvnw clean test

# Run tests that match pattern
test-filter PATTERN:
    ./mvnw clean test -Dtest={{PATTERN}}

# Apply code formatter
format:
    ./mvnw spotless:apply
