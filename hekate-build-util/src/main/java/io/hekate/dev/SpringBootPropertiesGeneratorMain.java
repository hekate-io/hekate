/*
 * Copyright 2021 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.dev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility to generate Spring Boot properties file based on JSON meta-data.
 */
public final class SpringBootPropertiesGeneratorMain {
    private static final Pattern NEW_LINE = Pattern.compile("\\n");

    private SpringBootPropertiesGeneratorMain() {
        // No-op.
    }

    /**
     * Runs this utility.
     *
     * @param args First arg - path to Spring Boot metadata JSON file; second arg - path where to generate the properties file.
     *
     * @throws IOException File reading error.
     * @throws IllegalArgumentException Invalid command line arguments.
     */
    public static void main(String[] args) throws IOException, IllegalArgumentException {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected <json_input_path> <properties_output_path>, but got: " + Arrays.toString(args));
        }

        Path source = Paths.get(args[0]);

        if (!source.toFile().isFile()) {
            throw new IllegalArgumentException("Source doesn't exist or is not a file [path=" + source + ']');
        }

        Path target = Paths.get(args[1]);

        mkDirs(target);

        generate(source, target);
    }

    private static void generate(Path source, Path target) throws IOException {
        try (
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            OutputStreamWriter bufOut = new OutputStreamWriter(buf, UTF_8);
            PrintWriter out = new PrintWriter(bufOut, true)
        ) {
            generate(source, out);

            Files.write(target, buf.toByteArray());
        }
    }

    private static void generate(Path source, PrintWriter out) throws IOException {
        JsonNode root = readJson(source);

        Set<String> duplicates = new HashSet<>();
        Set<String> prefixes = new HashSet<>();

        ArrayNode props = (ArrayNode)root.get("properties");

        String currentPrefix = null;

        for (int i = 0; i < props.size(); i++) {
            JsonNode prop = props.get(i);

            if (i > 0) {
                out.println();
            }

            String name = prop.get("name").asText();

            // Property name must be unique.
            if (!duplicates.add(name)) {
                throw new IllegalStateException("Duplicated property [name=" + name + ", file=" + source + ']');
            }

            // Properties must be ordered by their prefixes ([a.1, a.2, b.1, b.2] <- good; [a.1, b.1, a.2, b.2] <- bad).
            String prefix = name.substring(0, name.lastIndexOf('.'));

            if (!prefix.equals(currentPrefix)) {
                currentPrefix = prefix;

                if (!prefixes.add(currentPrefix)) {
                    throw new IllegalStateException("Property is out of the prefix order [name=" + name + ", file=" + source + ']');
                }

                findHeader(currentPrefix, root).ifPresent(header -> {
                    out.println("########################################################################################################");
                    out.println("# " + header);
                    out.println("########################################################################################################");
                });
            }

            String description = prop.get("description").asText();
            String defaultValue = prop.get("defaultValue").asText();
            List<String> hints = findHints(name, root);

            // Write description as a comment.
            out.println("# " + NEW_LINE.matcher(description).replaceAll("\n#"));

            // Write hints as a comment.
            if (!hints.isEmpty()) {
                out.println("#");

                for (String hint : hints) {
                    out.println("#   " + hint);
                }

                out.println("#");
            }

            // Write the property and its default value.
            out.println(name + "=" + ("null".equals(defaultValue) ? "" : defaultValue));
        }
    }

    private static Optional<String> findHeader(String section, JsonNode json) {
        ArrayNode groups = (ArrayNode)json.get("groups");

        return StreamSupport.stream(groups.spliterator(), false)
            .filter(group -> section.equals(group.get("name").asText()))
            .map(group -> group.get("description").asText())
            .findFirst();
    }

    private static List<String> findHints(String property, JsonNode json) throws IOException {
        List<String> result = new ArrayList<>();

        ArrayNode hints = (ArrayNode)json.get("hints");

        for (JsonNode hint : hints) {
            if (property.equals(hint.get("name").asText())) {
                ArrayNode values = (ArrayNode)hint.get("values");

                Map<String, String> hintMap = new LinkedHashMap<>();

                int maxLen = 0;

                for (JsonNode value : values) {
                    String val = value.get("value").asText();
                    String desc = value.get("description").asText();

                    if (val.length() > maxLen) {
                        maxLen = val.length();
                    }

                    hintMap.put(val, desc);
                }

                int finalMaxLen = maxLen;

                hintMap.forEach((k, v) -> {
                        String paddedKey = pad(k, finalMaxLen - k.length());

                        // Split multi-line descriptions and pad with whitespaces if needed.
                        String[] lines = NEW_LINE.split(v);

                        for (int i = 0; i < lines.length; i++) {
                            if (i == 0) {
                                result.add(paddedKey + " - " + lines[i]);
                            } else {
                                result.add(pad("", finalMaxLen + 2 /* '- ' */) + lines[i]);
                            }
                        }
                    }
                );

                ArrayNode providers = (ArrayNode)hint.get("providers");

                if (providers != null) {
                    StreamSupport.stream(providers.spliterator(), false)
                        .filter(provider -> "any".equals(provider.get("name").asText()))
                        .map(provider -> provider.get("description"))
                        .filter(Objects::nonNull)
                        .map(JsonNode::asText)
                        .findFirst()
                        .ifPresent(description -> {
                            result.add(""); // <-- Empty line as a separator.
                            result.add(description);
                        });
                }
            }
        }

        return result;
    }

    private static JsonNode readJson(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return new ObjectMapper().reader().readTree(in);
        }
    }

    private static void mkDirs(Path path) throws IOException {
        File dir = path.toFile().getParentFile();

        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed to create directories: " + path);
        }
    }

    private static String pad(String s, int spaces) {
        StringBuilder buf = new StringBuilder(s);

        for (int i = 0; i < spaces; i++) {
            buf.append(' ');
        }

        return buf.toString();
    }
}
