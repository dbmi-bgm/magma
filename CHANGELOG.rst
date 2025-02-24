
==========
Change Log
==========

3.9.0
=====
* Added functionality to run sample identity checks
* Add `dry_run` option to `archive_unaligned_reads` command


3.8.0
=====
* Added possibility to overwrite exsting QCs when running a standalone QC workflow.
* In QC workflows, it is now possible to specify multiple files or filesets at once. MetaworkflowRuns are created for each file or fileset.
* Added a `custom_qc`` function. It has currently no functionality, but it can be used when custom MWFs are created by bioinformatics and need to be run
* Added function to reset a list of MetaWorkflowRuns
* Added function to merge QC items.
* Updated RNA-specific input to Gencode 47
* Added function to archive unaligned reads within a fileset


3.7.0
=====
* Add support for BAM2FASTQ MetaWorkflowRun
* Add RNA-Seq alignment MetaWorkflowRun


3.6.2
=====
* Add short_reads_FASTQ_quality_metrics MetaWorkflowRun
* Rename short read FASTQ MetaWorkflowRun specific for Illumina


3.6.1
=====
* Use only input files with status `uploaded` when creating MetaWorkflowRuns. (Ignore `retracted` files)


3.6.0
=====
* Support for Pyhthon 3.12.


3.5.0
=====
* Add commands to create ultra-long BAM QC (ONT) and long-read BAM QC (Pacbio) MetaWorkflowRuns


3.4.1
=====
* Update dependencies


3.4.0
=====
* Add set_property function to wrangler utils for SMaHT


3.3.0
=====
* Restructure CLI to create MetaWorkflowRuns and execute wrangler functions
* Add more MetaWorkflowRun creation functions for SMaHT (CRAM to FASTQ conversion, long read QC)
* Add wrangler utils for SMaHT


3.2.3
=====
* Update `magma_smaht` to accommodate data model change on Library from `analyte` to `analytes`


3.2.2
=====
* Generalize tibanna-ff dependency


3.2.1
=====
* Use click ^7.00


3.2.0
=====
* Automated MetaWorkflowRun creation scripts


3.1.0
=====
* Add ``mwfr_from_input``


3.0.1
=====
* Change dcicutils from 8.0.0 to ^8.0.0.
* Treat QC rulesets as parameters


3.0.0
=====
* Added magma components for interfacing with SMaHT portal


2.1.1
=====
* Update common fields for more strict validation with jsonschema updates in portal
* Refactor dimensionality handling


2.1.0
=====
* Add ability to specify explicit inputs to gather and sharding on MWFRs
* Cost calculations for MWFRs only performed once after MWFR has finished running to save on tibanna API calls
* Small bug fixes for determination of WorkflowRun status


2.0.0
=====
* Added this CHANGELOG.rst file.
* Upgrade to Python 3.11.
