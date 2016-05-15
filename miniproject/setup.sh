#!/bin/bash
## Get requirements.
sudo apt-get install -y samtools
curl ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/NA21144/alignment/NA21144.chrom20.ILLUMINA.bwa.GIH.low_coverage.20130415.bam > unfiltered.bam
samtools view unfiltered.bam -f 4 -F 8 > data.bam
rm unfiltered.bam
