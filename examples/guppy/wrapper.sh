#!/usr/bin/bash
set -e
GUPPY_BASECALL_ID=${GUPPY_BASECALL_ID:-0}
GUPPY_CONFIG=${GUPPY_CONFIG:-dna_r10.4_e8.1_fast.cfg}

guppy_basecaller -i ${1} -s guppy_output -c $GUPPY_CONFIG 


find guppy_output -iname "*.fastq" | xargs cp -t output/
ls -R
for f in output/*.fastq; do
    input_file=input/*.fast5
    mv -- "$f" "${f%.fastq}-tranponster-guppy-$(date +%s).fastq"
done
