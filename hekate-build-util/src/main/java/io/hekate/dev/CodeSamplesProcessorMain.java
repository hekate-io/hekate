/*
 * Copyright 2019 The Hekate Project
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

/**
 * Utility for source code examples inclusion into javadocs.
 */
public final class CodeSamplesProcessorMain {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final String NL = System.lineSeparator();

    private CodeSamplesProcessorMain() {
        // No-op.
    }

    /**
     * Runs this utility.
     *
     * @param args First arg - path to javadocs folder; second arg - list of folders where to
     *
     * @throws IOException File reading error.
     */
    public static void main(String[] args) throws IOException {
        String docPath = args[0];
        String sourcePath = args[1];

        process(docPath, sourcePath);
    }

    private static void process(String javadocSourcePath, String samplesSourcePath) throws IOException {
        File javadocSource = new File(javadocSourcePath);
        List<File> samplesSources = getSampleSources(samplesSourcePath);

        say("Processing sample code:");
        say("   samples - " + samplesSources.stream().map(File::getAbsolutePath).collect(toList()));
        say("    target - " + javadocSource.getCanonicalPath());

        if (!javadocSource.exists()) {
            say("Skipped processing since javadoc source path doesn't exist [path=" + javadocSourcePath + ']');

            return;
        }

        if (!javadocSource.isDirectory()) {
            throw new IllegalArgumentException("Javadoc source path is not a directory [path=" + javadocSource.getAbsolutePath() + ']');
        }

        StringBuilder buf = new StringBuilder();

        processDir(javadocSource, samplesSources, buf);
    }

    private static void processDir(File javadocSrcDir, List<File> samplesSrcDirs, StringBuilder buf) throws IOException {
        File[] files = javadocSrcDir.listFiles(pathname -> pathname.isDirectory() || pathname.getName().endsWith(".html"));

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    processDir(file, samplesSrcDirs, buf);
                } else {
                    processFile(file, samplesSrcDirs, buf);

                    buf.setLength(0);
                }
            }
        }
    }

    private static void processFile(File javadocSource, List<File> samplesSrcDirs, StringBuilder buf) throws IOException {
        boolean rewrite = false;

        try (BufferedReader reader = Files.newBufferedReader(javadocSource.toPath(), UTF_8)) {
            for (String s = reader.readLine(); s != null; s = reader.readLine()) {
                int start;
                int end = -1;

                String pattern = "${source:";

                start = s.indexOf(pattern);

                if (start >= 0) {
                    start += pattern.length();

                    end = s.indexOf('}', start);
                }

                if (start >= 0 && end >= 0) {
                    String path = s.substring(start, end).trim();
                    String section = null;

                    int splitIdx = path.indexOf('#');

                    if (splitIdx >= 0) {
                        section = path.substring(splitIdx + 1).trim();
                        path = path.substring(0, splitIdx).trim();
                    }

                    if (path.isEmpty() || (section != null && section.isEmpty())) {
                        throw new IllegalStateException("Failed to parse 'source' directive: " + s);
                    }

                    String brush = resolveBrush(path);

                    if (brush == null) {
                        throw new IllegalArgumentException("Failed to resolve brush from path '" + path + "'.");
                    }

                    File code = null;

                    for (File dir : samplesSrcDirs) {
                        code = new File(dir, path);

                        if (code.isFile()) {
                            break;
                        } else {
                            code = null;
                        }
                    }

                    if (code == null) {
                        throw new FileNotFoundException("Failed to find sample source file: " + path);
                    }

                    buf.append("<div class=\"doc_source\">").append(NL);
                    buf.append("<pre><code class=\"").append(brush).append("\">").append(NL);

                    writeSourceCode(code, section, buf, javadocSource);

                    buf.append("</code></pre>").append(NL);
                    buf.append("</div>").append(NL);

                    rewrite = true;
                } else {
                    buf.append(s).append(NL);
                }
            }
        }

        if (rewrite) {
            write(buf, javadocSource);
        }
    }

    private static void writeSourceCode(File src, String section, StringBuilder out, File requester) throws IOException {
        List<String> sectionLines = new ArrayList<>(256);

        Integer minOffset = null;

        boolean sectionFound = section == null;

        Pattern start = Pattern.compile("\\s*((//)|(<!--)|(#)).*Start:\\s*" + section + ".*");
        Pattern end = Pattern.compile("\\s*((//)|(<!--)|(#)).*End:\\s*" + section + ".*");

        for (String line : Files.readAllLines(Paths.get(src.getAbsolutePath()))) {
            if (sectionFound) {
                if (end.matcher(line).matches()) {
                    break;
                }

                String realLine = line.trim();

                if (realLine.isEmpty()) {
                    sectionLines.add(realLine);
                } else {
                    int offset = getWhitespacesOffset(line);

                    if (minOffset == null || offset < minOffset) {
                        minOffset = offset;
                    }

                    sectionLines.add(line);
                }
            } else {
                if (start.matcher(line).matches()) {
                    sectionFound = true;
                }
            }
        }

        if (!sectionFound) {
            throw new IllegalStateException("Couldn't find '" + section + "' section in file "
                + src.getAbsolutePath() + " (required for (" + requester.getAbsolutePath() + ')');
        }

        Pattern lt = Pattern.compile("<");
        Pattern gt = Pattern.compile(">");
        Pattern publicStaticClass = Pattern.compile("public static class");

        for (String line : sectionLines) {
            String trimmed;

            if (!line.isEmpty() && minOffset != null && minOffset > 0) {
                trimmed = line.substring(minOffset);
            } else {
                trimmed = line;
            }

            // Replace 'public static class' with 'public class' since many code examples are implemented as inner classes.
            trimmed = publicStaticClass.matcher(trimmed).replaceAll("public class");

            out.append(lt.matcher(gt.matcher(trimmed).replaceAll("&gt;")).replaceAll("&lt;")).append(NL);
        }
    }

    private static void write(StringBuilder src, File target) throws IOException {
        try (BufferedWriter out = Files.newBufferedWriter(target.toPath(), UTF_8)) {
            out.append(src);

            out.flush();
        }
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private static void say(String msg) {
        System.out.println(msg);
    }

    private static List<File> getSampleSources(String samplesSourcePath) {
        String[] tokens = samplesSourcePath.split(";", 0);

        List<File> files = new ArrayList<>();

        for (String token : tokens) {
            File src = new File(token.trim());

            if (src.isDirectory()) {
                files.add(src);
            }
        }

        return files;
    }

    private static String resolveBrush(String path) {
        int dot = path.lastIndexOf('.');

        if (dot >= 0 && dot < path.length() - 1) {
            return path.substring(dot + 1);
        }

        return null;
    }

    private static int getWhitespacesOffset(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isSpaceChar(s.charAt(i))) {
                return i;
            }
        }

        return 0;
    }
}
