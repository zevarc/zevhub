plugins {
    alias(libs.plugins.android.library)
}

android {
    namespace = "com.zevarc.zevbond"

    compileSdk {
        version = release(37)
    }

    defaultConfig {
        minSdk = 24

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    testOptions {
        unitTests {
            isReturnDefaultValues = true
        }
    }
}

dependencies {
    api(libs.kotlinx.coroutines.core)

    testImplementation(libs.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}

// Transfers up to 3 MiB are held in memory by the unit test fixtures;
// the default 512m heap is not enough.
tasks.withType<Test>().configureEach {
    maxHeapSize = "2g"
}
