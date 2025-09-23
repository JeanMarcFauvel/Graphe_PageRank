#!/usr/bin/env bash
set -euo pipefail

MAIN_CLASS="PageRankBench"
SPARK_MEM_DRIVER="16g"
SPARK_MEM_EXEC="16g"
DISABLE_AQE="false"

# parse options (wrapper)
if [[ "${1:-}" == "--main" ]]; then MAIN_CLASS="$2"; shift 2; fi
if [[ "${1:-}" == "--mem"  ]]; then SPARK_MEM_DRIVER="$2"; SPARK_MEM_EXEC="$2"; shift 2; fi
if [[ "${1:-}" == "--no-aqe" ]]; then DISABLE_AQE="true"; shift 1; fi

# tout ce qui suit -- est transmis à l'appli
APP_ARGS=()
if [[ "${1:-}" == "--" ]]; then shift; APP_ARGS=("$@"); fi

# build (sans assembly)
sbt clean package

# jar non-assemblé (Scala 2.13 ici)
JAR="$(ls target/scala-2.13/pagerank-bench_*-*.jar | head -n 1)"

# --- Localiser spark-submit de façon robuste ---
if command -v spark-submit >/dev/null 2>&1; then
  SPARK_BIN="$(command -v spark-submit)"
elif [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_BIN="$SPARK_HOME/bin/spark-submit"
elif command -v brew >/dev/null 2>&1; then
  SPARK_PREFIX="$(brew --prefix apache-spark 2>/dev/null || true)"
  if [[ -n "$SPARK_PREFIX" && -x "$SPARK_PREFIX/libexec/bin/spark-submit" ]]; then
    SPARK_BIN="$SPARK_PREFIX/libexec/bin/spark-submit"
  fi
fi

if [[ -z "${SPARK_BIN:-}" ]]; then
  echo "spark-submit introuvable. Ajoute Spark à ton PATH, ou exporte SPARK_HOME."
  echo "Exemples :"
  echo "  export SPARK_HOME=\"$(brew --prefix apache-spark)/libexec\""
  echo "  export PATH=\"\$SPARK_HOME/bin:\$PATH\""
  exit 1
fi

# Confs pour éviter l'OOM d'explain + logs verbeux
SPARK_CONFS=(
  --conf "spark.driver.memory=${SPARK_MEM_DRIVER}"
  --conf "spark.executor.memory=${SPARK_MEM_EXEC}"
  --conf "spark.sql.maxPlanStringLength=100000"
  --conf "spark.sql.ui.explainMode=simple"
  --conf "spark.eventLog.enabled=false"
  --conf "spark.ui.showConsoleProgress=false"
)

# Optionnel : couper AQE si demandé
if [[ "$DISABLE_AQE" == "true" ]]; then
  SPARK_CONFS+=( --conf "spark.sql.adaptive.enabled=false" )
fi

# Lancement
"$SPARK_BIN" \
  --class "$MAIN_CLASS" \
  "${SPARK_CONFS[@]}" \
  "$JAR" \
  "${APP_ARGS[@]}"



