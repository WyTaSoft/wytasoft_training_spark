# **SETUP — Get the Starter Skeleton Running**

> Branch: `features/UC_Java_Starter_Skeleton`

This guide gets a fresh trainee from a clean machine to a green `mvn test` in under 15 minutes.
Follow it once at the start of the training; you should not need to come back to it.

---

## **1. Prerequisites**

| Tool       | Version          | Check                                                           |
|------------|------------------|-----------------------------------------------------------------|
| **JDK**    | **11** (LTS)     | `java -version` → must print `11.0.x`                           |
| **Maven**  | 3.8 or newer     | `mvn -v` → must show `Apache Maven 3.8+` and the JDK 11 path    |
| **Git**    | any recent       | `git --version`                                                 |
| **IntelliJ IDEA** (or VS Code + Java pack) | Community edition is fine | — |

> **Important:** Spark 3.5.x **does not run on JDK 17/21 without extra effort**. Stick with
> JDK 11 for this training. JDK 8 also works but JDK 11 is what the build is configured for.

### Check your Java version

```bash
java -version
mvn -v
```

If `mvn -v` shows a different JDK than `java -version`, either set `JAVA_HOME` or use IntelliJ's
Maven settings to point at JDK 11.

---

## **2. Clone & checkout**

```bash
git clone https://github.com/WyTaSoft/training_spark.git
cd training_spark
git checkout features/UC_Java_Starter_Skeleton
```

---

## **3. Verify the build before writing code**

The starter skeleton has no Java sources yet, so `mvn compile` succeeds trivially. Use the
build to verify Maven can resolve every dependency:

```bash
mvn clean compile
```

First run downloads ~250 MB of Spark/Hadoop JARs — be patient. Subsequent runs are seconds.

---

## **4. IntelliJ IDEA setup**

1. **Open the project** — `File > Open` and select the cloned folder. IntelliJ detects
   `pom.xml` and offers "Open as Project". Accept.
2. **Set the Project SDK to JDK 11** — `File > Project Structure > Project > SDK` →
   pick (or add) JDK 11. Set **Project language level** to `11 - Local variable syntax for
   lambda parameters`.
3. **Reload Maven** — open the Maven tool window (right edge) and click the **Refresh** icon.
   This re-imports all dependencies.
4. **Mark resources** — right-click `src/main/resources` → `Mark Directory as` →
   **Resources Root** (it should already be marked, but verify). Do the same for
   `src/test/resources` → **Test Resources Root**.
5. **Set the Maven runner JDK** — `Settings > Build, Execution, Deployment > Build Tools >
   Maven > Runner` → **JRE = 11**.
6. **Increase the IDE's import heap** — if IntelliJ struggles during the first import,
   raise `Settings > ... > Maven > Importing > VM options for importer` to `-Xmx2g`.

---

## **5. JDK 11 + Spark — the `--add-opens` flag**

Java 9+ enforces module access. Spark uses reflection on internal JDK packages and will
throw `java.lang.reflect.InaccessibleObjectException` or
`IllegalAccessError: class org.apache.spark.unsafe.Platform ... cannot access ...` unless
you grant access.

This project bundles the required flags in two places:

### 5.1 — Maven (already configured)

`.mvn/jvm.config` (and the Surefire plugin in `pom.xml`) inject the flags for
`mvn compile`, `mvn test`, and `mvn package`. **You don't have to touch this.**

### 5.2 — IntelliJ Run Configurations (you DO have to do this)

When you run `MainDriver` from IntelliJ (green ▶ button), the IDE does **not** read
`.mvn/jvm.config`. Add the flags to the run configuration:

1. `Run > Edit Configurations…`
2. Select your `MainDriver` (or `StreamingDriver`) configuration.
3. Click **Modify options** → **Add VM options**.
4. Paste:

```
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED
--add-opens=java.base/sun.security.action=ALL-UNNAMED
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

5. Save.

Tip: turn it into an **IntelliJ template** — `Edit Configurations… > Edit configuration
templates… > Application` → paste the same VM options. Every new run config inherits them.

---

## **6. (Windows only) `winutils.exe`**

If Spark complains about `HADOOP_HOME` or you see `NullPointerException` on shutdown:

1. Download a `winutils.exe` for Hadoop 3.3 from a trusted mirror.
2. Place it in `C:\hadoop\bin\winutils.exe`.
3. Set the environment variable `HADOOP_HOME=C:\hadoop`.
4. Restart IntelliJ.

For local development against the bundled CSV/log files this is optional — most operations
work without it.

---

## **7. First run**

Once you've completed [STEPS.md](./STEPS.md) up to **Step 4**, run `MainDriver` from IntelliJ.
You should see the `clients` DataFrame printed:

```
+--------+--------+--------+
|clientId|name    |location|
+--------+--------+--------+
|1       |Alice   |Paris   |
|2       |Bob     |Lyon    |
|...     |...     |...     |
+--------+--------+--------+
```

If that works, your toolchain is sound — proceed with the rest of [STEPS.md](./STEPS.md).

---

## **8. Common first-day errors**

| Error                                                                       | Fix                                                                              |
|-----------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `java.lang.UnsupportedClassVersionError: ... has been compiled by ... 17.0` | Project SDK is JDK 17, set it back to JDK 11.                                    |
| `InaccessibleObjectException: Unable to make ... accessible`                | Missing `--add-opens` in IntelliJ run config — see §5.2.                         |
| `ClassNotFoundException: org.apache.spark.sql.SparkSession`                 | Maven hasn't imported. Right-click `pom.xml` → Maven → Reload project.           |
| `Cannot find or load main class com.wts.kayan.app.job.MainDriver`           | The class is empty — finish [STEPS.md](./STEPS.md) Step 4 first.                 |
| `log4j2.xml not found`                                                      | Ensure `src/main/resources` is marked as **Resources Root** (see §4).            |
| Shows `WARN: An illegal reflective access operation` on JDK 11              | Harmless. Spark 3.5 still triggers it; flags in §5 silence the worst noise.      |

---

## **You're ready**

Open [STEPS.md](./STEPS.md) and start with Step 1.
