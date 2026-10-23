# pg-task-lease 0.3.0 — Java: compatibility and adopted features

[English](java.en.md) · [Русский](java.ru.md)<br>
[Usage guide](en.md) · [Library versions](../README.md)

[Java 17](#java-17) · [Java 21](#java-21) · [Java 25](#java-25) · [Java 27](#java-27)

Library and Java versions are different. The base 0.3.0 artifacts target Java 17. Running on a newer JVM does not mean the library adopts every new API or provides a measured performance improvement.

## Java 17

Minimum core version. Compiled with --release 17, class-file 61.0, without preview. All baseline capabilities in the usage guide are available here.

```sh
./gradlew build -PtestJavaVersion=17
```

The build needs a disposable PostgreSQL test database; see README for its configuration. To install without executing tests: ./gradlew publishToMavenLocal.

## Java 21

The base artifact adds no Java 21-specific APIs. It continues to use Java 17 APIs and is tested on JVM 21. Switching runtime does not change its public API or contract.

```sh
./gradlew build -PtestJavaVersion=21
```

## Java 25

JVM 25 is supported. ScopedValue and other Java 25-specific APIs are not added to this library: its current responsibilities do not require a separate adapter. The minimum remains Java 17.

```sh
./gradlew build -PtestJavaVersion=25
```

## Java 27

Java 27 is tested as an additional non-LTS runtime. The artifact does not add Java 27-specific APIs. GC, TLS and object-header changes belong to the JVM; this library does not present them as its own features or promise unmeasured speedups.

```sh
./gradlew build -PtestJavaVersion=27
```

## Changes between library versions

| Version | Java requirements and capabilities |
| --- | --- |
| 0.1.0 | Java 17 baseline. |
| 0.2.0 | Historical release requiring Java 27; no longer the current recommendation. |
| 0.3.0 | Java 17 restored; JVM 17/21/25/27 tested. Newer APIs do not raise artifact requirements. |

[Compatibility and migration](../JAVA-SUPPORT.md)
