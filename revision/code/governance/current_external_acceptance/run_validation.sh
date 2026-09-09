#!/bin/bash
R=/mnt/data/Stage1C_External_Review_2026-09-08
for k in min public; do
 env -u PYTHONPATH -u PYTHONHOME PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 "$R/venv/bin/python" -B "$R/inputs/$k/src/validate_stage1c.py" --tree "$R/inputs/$k" --kind "$k" --output "$R/validation_$k" > "$R/logs/validation_$k.log" 2>&1
 echo "$k exit=$?" >> "$R/logs/process_exit.txt"
done
