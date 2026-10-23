package io.github.alex59.pgtasklease;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;

class DocumentationTest {
    @Test void everyGuideSnippetCompilesOnTheJava17Api() throws Exception {
        var en = snippets(Path.of("docs/0.3.0/en.md"));
        assertEquals(en, snippets(Path.of("docs/0.3.0/ru.md")), "EN/RU code examples must agree");
        assertEquals(8, en.size());
        String setup = en.get(0);
        String imports = setup.lines().filter(line -> line.startsWith("import ")).collect(Collectors.joining("\n"));
        String initialization = setup.lines().filter(line -> !line.startsWith("import ")).collect(Collectors.joining("\n"));
        Path directory = Files.createDirectories(Path.of("build/docs-snippets"));
        var sources = new ArrayList<File>();
        for (int i = 1; i < en.size(); i++) {
            Path file = directory.resolve("GuideSnippet" + i + ".java");
            Files.writeString(file, imports + "\npublic class GuideSnippet" + i + " { static void example() {\n"
                    + initialization + "\nTaskLease lease = null;\n" + en.get(i) + "\n} }\n");
            sources.add(file.toFile());
        }
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "Run with a JDK");
        String classpath = Stream.of(PgTaskQueue.class, reactor.core.publisher.Mono.class,
                        io.r2dbc.spi.ConnectionFactory.class, org.reactivestreams.Publisher.class)
                .map(type -> {
                    try { return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toString(); }
                    catch (Exception error) { throw new IllegalStateException(error); }
                }).collect(Collectors.joining(File.pathSeparator));
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var files = compiler.getStandardFileManager(diagnostics, null, null)) {
            assertTrue(compiler.getTask(null, files, diagnostics,
                    List.of("--release", "17", "-classpath", classpath, "-d", directory.toString()), null,
                    files.getJavaFileObjectsFromFiles(sources)).call(), diagnostics.getDiagnostics().toString());
        }
    }

    @Test void downloadableMigrationMatchesThePackagedMigration() throws Exception {
        assertEquals(Files.readString(Path.of("src/main/resources/pg-task-lease.sql")),
                Files.readString(Path.of("docs/0.3.0/examples/schema.sql")));
    }

    private static List<String> snippets(Path file) throws Exception {
        var matcher = Pattern.compile("```java\\n(.*?)\\n```", Pattern.DOTALL).matcher(Files.readString(file));
        var result = new ArrayList<String>();
        while (matcher.find()) result.add(matcher.group(1));
        return result;
    }
}
