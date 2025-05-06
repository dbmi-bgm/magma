#!/bin/bash

# Run alignment metaworkflow runs on the test data
# Test data; https://data.smaht.org/search/?type=FileSet&tags=bioinformatics_test

#smaht:MetaWorkflow-Hi-C_alignment_GRCh38_<version>
create-mwfr-smaht align-hic -e data -f SMAFSOYAP5B1
#smaht:MetaWorkflow-Illumina_alignment_GRCh38_<version>
create-mwfr-smaht align-illumina -e data -l 151 -f SMAFSU6J1YW4
#smaht:MetaWorkflow-ONT_alignment_GRCh38_<version>
create-mwfr-smaht align-ont -e data -f SMAFS2ROFA22
#smaht:MetaWorkflow-PacBio_alignment_GRCh38_<version>
create-mwfr-smaht align-pacbio -e data -f SMAFSBZ1Y9JL
#smaht:MetaWorkflow-RNA-seq_bulk_short_reads_GRCh38_<version>
create-mwfr-smaht align-rnaseq -e data -l 151 -f SMAFS9HC8D37

# Run standalone QC metaworkflows on the test data

#smaht:MetaWorkflow-short_reads_FASTQ_quality_metrics_<version>
create-mwfr-smaht qc-short-read-fastq-illumina -e data -f SMAFSU6J1YW4 -c
#smaht:MetaWorkflow-Illumina_FASTQ_quality_metrics_<version>
create-mwfr-smaht qc-short-read-fastq-illumina -e data -f SMAFSU6J1YW4
#smaht:MetaWorkflow-long_reads_FASTQ_quality_metrics_<version> (ONT, PacBio)
create-mwfr-smaht qc-long-read-ubam -e data -f SMAFSBZ1Y9JL
#smaht:MetaWorkflow-paired-end_short_reads_BAM_quality_metrics_GRCh38_<version>
create-mwfr-smaht qc-short-read-bam -e data -f SMAFI3RBZ67R
#smaht:MetaWorkflow-ultra-long_reads_BAM_quality_metrics_GRCh38_<version> (ONT)
create-mwfr-smaht qc-ultra-long-bam -e data -f SMAFIYQXNODI
#smaht:MetaWorkflow-long_reads_BAM_quality_metrics_GRCh38_<version> (PacBio)
create-mwfr-smaht qc-long-read-bam -e data -f SMAFI5CW6GM7

#smaht:MetaWorkflow-sample_identity_check_<version>
# TODO
