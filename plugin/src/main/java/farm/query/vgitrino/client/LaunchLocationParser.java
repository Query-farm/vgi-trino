// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgitrino.client;

import java.util.ArrayList;
import java.util.List;

/**
 * Tokenizes the {@code <argv>} portion of a {@code launch:<argv>} {@code vgi.location}
 * value into the argv list {@code LauncherClient} hashes and spawns — POSIX
 * shell-quote semantics, byte-for-byte matching the C++ VGI extension's own {@code
 * ParseLaunchArgv} ({@code src/vgi_launcher_internal.cpp} in the {@code vgi} repo),
 * which is what makes a Trino-launched worker shareable with a DuckDB process
 * pointed at the identical {@code launch:} location: the resulting argv list feeds
 * the same tuple hash on both sides, so they only agree on which worker to share if
 * they tokenize the location string identically.
 *
 * <p>Supports plain whitespace-separated words, double-quoted strings (backslash
 * escapes for {@code "}, backslash itself, {@code $}, and a backtick), single-quoted
 * strings (no escapes, fully literal), and a bare backslash outside quotes escaping
 * the next character. The Windows variant of the C++ parser (where backslash is a
 * literal path separator, not an escape) is intentionally not replicated — {@code
 * launch:} isn't implemented for Windows in this connector at all (see the README's
 * Scope section), so there is no Windows behavior to match.
 */
final class LaunchLocationParser {

    private LaunchLocationParser() {}

    private enum State { DEFAULT, IN_DOUBLE, IN_SINGLE }

    /**
     * @param payload the part of the location string after the {@code launch:} prefix
     * @return the tokenized argv list
     * @throws IllegalArgumentException on an unterminated quote, a trailing bare backslash,
     *         or an empty argv (mirrors the C++ parser's own {@code std::invalid_argument} cases)
     */
    static List<String> parseArgv(String payload) {
        List<String> out = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        boolean hasToken = false;
        State state = State.DEFAULT;

        for (int i = 0; i < payload.length(); i++) {
            char c = payload.charAt(i);
            switch (state) {
                case DEFAULT -> {
                    if (c == ' ' || c == '\t' || c == '\n') {
                        if (hasToken) {
                            out.add(token.toString());
                            token.setLength(0);
                            hasToken = false;
                        }
                    } else if (c == '"') {
                        state = State.IN_DOUBLE;
                        hasToken = true; // empty "" is still a token
                    } else if (c == '\'') {
                        state = State.IN_SINGLE;
                        hasToken = true;
                    } else if (c == '\\') {
                        if (i + 1 >= payload.length()) {
                            throw new IllegalArgumentException(
                                    "vgi launcher: trailing backslash in launch: argv");
                        }
                        token.append(payload.charAt(++i));
                        hasToken = true;
                    } else {
                        token.append(c);
                        hasToken = true;
                    }
                }
                case IN_DOUBLE -> {
                    if (c == '"') {
                        state = State.DEFAULT;
                    } else if (c == '\\' && i + 1 < payload.length()) {
                        char next = payload.charAt(i + 1);
                        // Only a few sequences are special inside double quotes per POSIX;
                        // everything else preserves the backslash literally.
                        if (next == '"' || next == '\\' || next == '$' || next == '`' || next == '\n') {
                            token.append(next);
                            i++;
                        } else {
                            token.append('\\');
                        }
                    } else {
                        token.append(c);
                    }
                }
                case IN_SINGLE -> {
                    if (c == '\'') {
                        state = State.DEFAULT;
                    } else {
                        token.append(c);
                    }
                }
            }
        }
        if (state != State.DEFAULT) {
            throw new IllegalArgumentException("vgi launcher: unterminated quote in launch: argv");
        }
        if (hasToken) {
            out.add(token.toString());
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("vgi launcher: launch: location has empty argv");
        }
        return out;
    }
}
