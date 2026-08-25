// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.client;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Confirms {@link LaunchLocationParser} tokenizes {@code launch:} payloads the same
 * way the C++ VGI extension's {@code ParseLaunchArgv} does — this is what makes a
 * Trino-launched worker shareable with a DuckDB process pointed at the identical
 * {@code launch:} location (both sides feed the same tuple hash only if they agree
 * on the resulting argv list).
 */
final class LaunchLocationParserTest {

    @Test
    void plainWhitespaceSeparatedWords() {
        assertEquals(List.of("uv", "run", "--project", "/opt/worker", "vgi-fixture-worker"),
                LaunchLocationParser.parseArgv("uv run --project /opt/worker vgi-fixture-worker"));
    }

    @Test
    void doubleQuotedArgumentWithSpaces() {
        assertEquals(List.of("python", "/path with spaces/foo.py"),
                LaunchLocationParser.parseArgv("python \"/path with spaces/foo.py\""));
    }

    @Test
    void singleQuotedArgumentIsRawNoEscapes() {
        assertEquals(List.of("echo", "a\\b"),
                LaunchLocationParser.parseArgv("echo 'a\\b'"));
    }

    @Test
    void doubleQuoteBackslashEscapes() {
        assertEquals(List.of("echo", "a\"b\\c"),
                LaunchLocationParser.parseArgv("echo \"a\\\"b\\\\c\""));
    }

    @Test
    void emptyQuotedStringIsAnEmptyToken() {
        assertEquals(List.of("cmd", ""), LaunchLocationParser.parseArgv("cmd \"\""));
    }

    @Test
    void bareBackslashOutsideQuotesEscapesNextChar() {
        assertEquals(List.of("a b"), LaunchLocationParser.parseArgv("a\\ b"));
    }

    @Test
    void unterminatedDoubleQuoteThrows() {
        assertThrows(IllegalArgumentException.class, () -> LaunchLocationParser.parseArgv("cmd \"unterminated"));
    }

    @Test
    void unterminatedSingleQuoteThrows() {
        assertThrows(IllegalArgumentException.class, () -> LaunchLocationParser.parseArgv("cmd 'unterminated"));
    }

    @Test
    void trailingBareBackslashThrows() {
        assertThrows(IllegalArgumentException.class, () -> LaunchLocationParser.parseArgv("cmd \\"));
    }

    @Test
    void emptyPayloadThrows() {
        assertThrows(IllegalArgumentException.class, () -> LaunchLocationParser.parseArgv("   "));
    }
}
