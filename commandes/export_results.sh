#!/usr/bin/env bash
set -euo pipefail

# -------- Defaults (modifiables) --------
SRC_BENCH_DIR="${SRC_BENCH_DIR:-/tmp/pagerank_bench_results}"
SRC_TOPN_DIR="${SRC_TOPN_DIR:-/tmp/pagerank_exports}"
OUT_BENCH="${OUT_BENCH:-$HOME/Desktop/bench_results.csv}"
OUT_TOPN="${OUT_TOPN:-$HOME/Desktop/pagerank_top_exports.csv}"

show_help() {
  cat <<EOF
Fusionne les résultats Spark en CSV uniques (bench + topN) sur le Bureau.

Options :
  --bench-src <dir>   Dossier des résultats bench (défaut: $SRC_BENCH_DIR)
  --topn-src  <dir>   Dossier des exports TopN (défaut: $SRC_TOPN_DIR)
  --bench-out <file>  CSV fusionné bench (défaut: $OUT_BENCH)
  --topn-out  <file>  CSV fusionné topN (défaut: $OUT_TOPN)
  -h, --help          Affiche cette aide
Exemples :
  ./export_results.sh
  ./export_results.sh --bench-src /tmp/pagerank_bench_results --topn-src /tmp/pagerank_exports
EOF
}

# -------- Parse args --------
while [[ "${1:-}" != "" ]]; do
  case "$1" in
    --bench-src) SRC_BENCH_DIR="$2"; shift 2 ;;
    --topn-src)  SRC_TOPN_DIR="$2";  shift 2 ;;
    --bench-out) OUT_BENCH="$2";     shift 2 ;;
    --topn-out)  OUT_TOPN="$2";      shift 2 ;;
    -h|--help)   show_help; exit 0 ;;
    *) echo "Option inconnue: $1"; show_help; exit 1 ;;
  esac
done

# Pour que les glob part-*.csv vides ne fassent pas échouer la boucle
shopt -s nullglob

# -------- Fonction de fusion générique --------
merge_csv_dir() {
  local src_dir="$1"
  local out_file="$2"
  local header="$3"      # ligne d'en-tête attendue
  local label="$4"       # libellé pour logs (bench/topN)

  if [[ ! -d "$src_dir" ]]; then
    echo "ℹ️  Dossier $label inexistant : $src_dir (skip)"
    return 0
  fi

  local parts=("$src_dir"/part-*.csv)
  if [[ "${#parts[@]}" -eq 0 ]]; then
    echo "ℹ️  Aucun part-*.csv trouvé dans $src_dir pour $label (skip)"
    return 0
  fi

  rm -f "$out_file"
  echo "$header" > "$out_file"

  local added=0
  for f in "${parts[@]}"; do
    # ignorer les fichiers vides
    [[ ! -s "$f" ]] && continue
    local first_line
    first_line="$(head -n 1 "$f" || true)"
    if [[ "$first_line" == "$(echo "$header" | tr -d '\r')" ]]; then
      tail -n +2 "$f" >> "$out_file"
    else
      cat "$f" >> "$out_file"
    fi
    added=$((added+1))
  done

  if [[ $added -eq 0 ]]; then
    echo "ℹ️  Aucun contenu utile fusionné pour $label (tous fichiers vides ?)"
    rm -f "$out_file"
  else
    echo "✅ CSV fusionné ($label) : $out_file"
    ls -lh "$out_file"
  fi
}

# -------- Fusion BENCH --------
merge_csv_dir \
  "$SRC_BENCH_DIR" \
  "$OUT_BENCH" \
  "dataset,approach,nNodes,nEdges,parseMs,computeMs,totalMs,peakUsedMemMB" \
  "bench"

# -------- Fusion TOPN --------
merge_csv_dir \
  "$SRC_TOPN_DIR" \
  "$OUT_TOPN" \
  "dataset,approach,beta,iters,id,rank" \
  "topN"
# -------- Fin --------

