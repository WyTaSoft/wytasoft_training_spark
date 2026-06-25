package com.wts.kayan.app.job

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.wts.kayan.sessionmanager.SparkSessionManager
import com.wts.kayan.app.common.PrimaryRunner
import com.wts.kayan.app.reader.PrimaryReader
import com.wts.kayan.app.utility.PrimaryConstants
import com.wts.kayan.app.writer.PrimaryWriter
import org.slf4j.LoggerFactory

object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param args jar parameters
    */
  def main(args: Array[String]): Unit = {

    if (args.isEmpty || args(0).trim.isEmpty) {
      logger.error("Missing required argument. Usage: MainDriver <env> [configPath] (e.g. dev localRun/application.conf)")
      throw new IllegalArgumentException(
        "Missing required argument <env>. Usage: MainDriver <env> [configPath] (e.g. dev localRun/application.conf)")
    }

    val env = args(0).trim

    // args(1) (optional): path to an external application.conf (e.g. localRun/application.conf).
    // When provided, it takes precedence over the bundled classpath config; otherwise the
    // classpath application.conf (src/main/resources) is used.
    implicit val config: Config =
      if (args.length > 1 && args(1).trim.nonEmpty) {
        val configFile = new File(args(1).trim)
        if (!configFile.exists()) {
          throw new IllegalArgumentException(
            s"Config file not found: ${configFile.getAbsolutePath} " +
              s"(JVM working directory ${new File(".").getAbsolutePath}). " +
              s"Pass a path relative to the working directory, or an absolute path.")
        }
        logger.info(s"\n**** Loading external configuration from ${configFile.getAbsolutePath} ****\n")
        ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load()).resolve()
      } else {
        logger.info("\n**** Loading bundled configuration from classpath (application.conf) ****\n")
        ConfigFactory.load()
      }

    // Fail fast with an actionable message rather than a cryptic "No setting for key" later on.
    if (!config.hasPath("training_spark")) {
      throw new IllegalStateException(
        "Loaded configuration has no 'training_spark' section. Either pass an external config as the " +
          "second argument (e.g. 'dev localRun/application.conf'), or rebuild so application.conf is on " +
          "the classpath (in IntelliJ: Build > Rebuild Project).")
    }

    val sparkSession = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    logger.info(s"\n\n****  training job has started ... **** \n\n", this.getClass.getName)

    val primaryReader = new PrimaryReader()(sparkSession, env, config)
    val primaryWriter = new PrimaryWriter()(env)
    val primaryRunner = new PrimaryRunner(primaryReader)(sparkSession).runPrimaryRunner()

    primaryWriter.write(primaryRunner, PrimaryConstants.CLIENTS_ORDERS, PrimaryConstants.MODE_OVERWRITE, 2)(env, config)

    // In debug mode, keep the JVM (and Spark UI) alive so it can be browsed before shutdown.
    if (config.getBoolean("training_spark.debug.enabled")) {
      val sleepMillis = config.getLong("training_spark.debug.ui_sleep_millis")
      logger.info(s"\n\n**** debug mode enabled - keeping Spark UI alive for $sleepMillis ms **** \n\n")
      Thread.sleep(sleepMillis)
    }

    sparkSession.stop()
  }
}
