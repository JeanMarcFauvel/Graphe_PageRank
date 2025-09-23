ThisBuild / scalaVersion := "2.13.12"        // ou 2.13.13 si dispo
ThisBuild / organization := "fr.jmf"

lazy val root = (project in file("."))
  .settings(
    name := "pagerank-bench",
    // Même version que  spark-submit via Homebrew)
    // 
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.0.1" % "provided",
      "org.apache.spark" %% "spark-sql"  % "4.0.1" % "provided"
    )
  )

