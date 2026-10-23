plugins {
    `java-library`
    `maven-publish`
}

group = "io.github.59alex"
version = "0.1.0"

repositories { mavenCentral() }

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api("io.projectreactor:reactor-core:3.7.0")
    api("io.r2dbc:r2dbc-spi:1.0.0.RELEASE")
    testImplementation("org.postgresql:r2dbc-postgresql:1.1.3.RELEASE")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
}
tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    // This suite deliberately fails if its disposable PostgreSQL is unavailable.
    environment("PG_TASK_TEST_URL", System.getenv("PG_TASK_TEST_URL")
        ?: "r2dbc:postgresql://localhost:5432/pg_task_lease_test")
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
