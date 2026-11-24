plugins {
    alias(libs.plugins.android.library)
    `maven-publish`
}

// Published coordinates. FastAir consumes the AAR produced here, so these are
// the compatibility contract between the SDK and its apps.
group = providers.gradleProperty("zevhub.group").get()
version = providers.gradleProperty("zevhub.version").get()

android {
    namespace = "com.zevarc.zevhub"

    compileSdk {
        version = release(37)
    }

    defaultConfig {
        minSdk = 24

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    testOptions {
        unitTests {
            isReturnDefaultValues = true
        }
    }

    publishing {
        // One artifact only: the release variant, plus sources for auditing.
        singleVariant("release") {
            withSourcesJar()
        }
    }
}

// Keep the Kotlin bytecode target in step with compileOptions above. Without
// this the two defaults can drift apart silently.
kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

dependencies {
    api(libs.kotlinx.coroutines.core)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}

// Registered after evaluation: the Android component only exists once the
// library plugin has created the variant, so resolving it during configuration
// would fail with "SoftwareComponent not found".
afterEvaluate {
    publishing {
        publications {
            register<MavenPublication>("release") {
                // AGP publishes the single configured variant as "release".
                from(components.getByName("release"))
                artifactId = "zevhub"

                pom {
                    name.set("ZevHub")
                    description.set(
                        "Link-agnostic Android P2P transport: messages, requests, " +
                            "bulk transfers and ordered streams over a session."
                    )
                    licenses {
                        license {
                            name.set("Apache License 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0")
                        }
                    }
                }
            }
        }
        repositories {
            // Two targets, both opt-in:
            //   ./gradlew :zevhub:publishToMavenLocal   -> ~/.m2 (what apps use)
            //   ./gradlew :zevhub:publishReleasePublicationToStagingRepository
            //                                           -> build/staging-repo
            // The staging directory keeps CI verifiable without a live registry.
            // Override with -Pzevhub.publishUrl=... to target a real repository.
            maven {
                name = "staging"
                url = uri(
                    providers.gradleProperty("zevhub.publishUrl").getOrElse(
                        layout.buildDirectory
                            .dir("staging-repo").get().asFile.toURI().toString()
                    )
                )
            }
        }
    }
}

// Transfers up to 3 MiB are held in memory by the unit test fixtures;
// the default 512m heap is not enough.
tasks.withType<Test>().configureEach {
    maxHeapSize = "2g"
}