plugins {
    `java-library`
    `maven-publish`
}

val testJavaVersion = providers.gradleProperty("testJavaVersion").map(String::toInt)
    .getOrElse(JavaVersion.current().majorVersion.toInt())

group = "io.github.59alex"
version = "0.3.0"

repositories { mavenCentral() }

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(testJavaVersion))
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api("io.projectreactor:reactor-core:3.8.8")
    api("io.r2dbc:r2dbc-spi:1.0.0.RELEASE")
    testImplementation("org.postgresql:r2dbc-postgresql:1.1.3.RELEASE")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
}
tasks.withType<Jar>().configureEach {
    from(rootProject.file("LICENSE")) { into("META-INF") }
}
tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    // This suite deliberately fails if its disposable PostgreSQL is unavailable.
    System.getenv("PG_TASK_TEST_URL")?.let { environment("PG_TASK_TEST_URL", it) }
    testLogging { events("failed", "skipped") }
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("pg-task-lease")
                description.set("Reactive PostgreSQL task leases with fenced acknowledgements and bounded retries")
                url.set("https://github.com/59Alex/pg-task-lease")
                licenses { license { name.set("MIT License"); url.set("https://opensource.org/licenses/MIT") } }
                developers { developer { id.set("59Alex"); name.set("Aleksandr Kashevarov") } }
                scm { url.set("https://github.com/59Alex/pg-task-lease"); connection.set("scm:git:https://github.com/59Alex/pg-task-lease.git") }
            }
        }
    }
}
