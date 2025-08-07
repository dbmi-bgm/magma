import click

from magma_smaht.create_metawfr import (
    mwfr_illumina_alignment,
    mwfr_rnaseq_alignment,
    mwfr_kinnex_alignment,
    mwfr_pacbio_alignment,
    mwfr_fastqc,
    mwfr_hic_alignment,
    mwfr_ont_alignment,
    mwfr_cram_to_fastq_paired_end,
    mwfr_bam_to_fastq_paired_end,
    mwfr_bamqc_short_read,
    mwfr_ubam_qc_long_read,
    mwfr_ultra_long_bamqc,
    mwfr_long_read_bamqc,
    mwfr_short_read_fastqc,
    mwfr_custom_qc,
    mwfr_sample_identity_check,
)
from magma_smaht.utils import get_auth_key


@click.group()
@click.help_option("--help", "-h")
def cli():
    # create group for all the commands. -h will show all available commands
    pass


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accessions",
)
@click.option(
    "-l",
    "--length-required",
    required=True,
    type=int,
    help="Required length (can be obtained from FastQC output)",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_illumina(fileset_accessions, length_required, auth_env):
    """Alignment MWFR for Illumina data"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_illumina_alignment(fileset_accession, length_required, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accessions",
)
@click.option(
    "-l",
    "--sequence-length",
    required=True,
    type=int,
    help="Sequence length (can be obtained from FastQC output)",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_rnaseq(fileset_accessions, sequence_length, auth_env):
    """Alignment MWFR for RNA-Seq data"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_rnaseq_alignment(fileset_accession, sequence_length, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accessions",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_kinnex(fileset_accessions, auth_env):
    """Alignment MWFR for Kinnex data"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_kinnex_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accessions",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_pacbio(fileset_accessions, auth_env):
    """Alignment MWFR for PacBio data"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_pacbio_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accessions",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_hic(fileset_accessions, auth_env):
    """Alignment MWFR for HIC data"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_hic_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accessions",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_ont(fileset_accessions, auth_env):
    """Alignment MWFR for ONT data"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_ont_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    type=str,
    multiple=True,
    help="Fileset accessions",
)
@click.option(
    "-c",
    "--check-lanes",
    default=False,
    is_flag=True,
    show_default=True,
    help="Whether to check lanes or not (different MWFs)",
)
@click.option(
    "-r",
    "--replace-qc",
    is_flag=True,
    show_default=True,
    default=False,
    help="Replace existing QC items.",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_short_read_fastq_illumina(fileset_accessions, check_lanes, replace_qc, auth_env):
    """QC MWFR for paired short-read Illumina FASTQs"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_fastqc(fileset_accession, check_lanes, replace_qc, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option("-f", "--file-accession", required=True, type=str, help="File accession")
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_short_read_fastq(file_accession, auth_env):
    """QC MWFR for short-read FASTQs"""
    smaht_key = get_auth_key(auth_env)
    mwfr_short_read_fastqc(file_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    type=str,
    multiple=True,
    help="Fileset accessions",
)
@click.option(
    "-r",
    "--replace-qc",
    is_flag=True,
    show_default=True,
    default=False,
    help="Replace existing QC items.",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_long_read_ubam(fileset_accessions, replace_qc, auth_env):
    """QC MWFR for unaligned long-read BAMs"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on Fileset {fileset_accession}")
        mwfr_ubam_qc_long_read(fileset_accession, replace_qc, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option("-f", "--file-accession", required=True, type=str, help="File accession")
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_short_read_bam(file_accession, auth_env):
    """QC MWFR for aligned short-read BAMs"""
    smaht_key = get_auth_key(auth_env)
    mwfr_bamqc_short_read(file_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option("-f", "--file-accession", required=True, type=str, help="File accession")
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def custom_qc(file_accession, auth_env):
    """Custom QC MWFR"""
    smaht_key = get_auth_key(auth_env)
    mwfr_custom_qc(file_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--file-accessions",
    required=True,
    type=str,
    multiple=True,
    help="File accession(s)",
)
@click.option(
    "-r",
    "--replace-qc",
    is_flag=True,
    show_default=True,
    default=False,
    help="Replace existing QC items.",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_ultra_long_bam(file_accessions, replace_qc, auth_env):
    """QC MWFR for aligned, ultra-long BAMs (ONT)"""
    smaht_key = get_auth_key(auth_env)
    for file_accession in file_accessions:
        print(f"Working on File {file_accession}")
        mwfr_ultra_long_bamqc(file_accession, replace_qc, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--file-accessions",
    required=True,
    type=str,
    multiple=True,
    help="File accession(s)",
)
@click.option(
    "-r",
    "--replace-qc",
    is_flag=True,
    show_default=True,
    default=False,
    help="Replace existing QC items.",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_long_read_bam(file_accessions, replace_qc, auth_env):
    """QC MWFR for aligned, long-read BAMs (PacBio)"""
    smaht_key = get_auth_key(auth_env)
    for file_accession in file_accessions:
        print(f"Working on File {file_accession}")
        mwfr_long_read_bamqc(file_accession, replace_qc, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accession(s)",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def conversion_cram_to_fastq(fileset_accessions, auth_env):
    """Conversion MWFR for CRAM to FASTQ (paired-end)"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on FileSet {fileset_accession}")
        mwfr_cram_to_fastq_paired_end(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f",
    "--fileset-accessions",
    required=True,
    multiple=True,
    type=str,
    help="Fileset accession(s)",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def conversion_bam_to_fastq(fileset_accessions, auth_env):
    """Conversion MWFR for BAM to FASTQ (paired-end)"""
    smaht_key = get_auth_key(auth_env)
    for fileset_accession in fileset_accessions:
        print(f"Working on FileSet {fileset_accession}")
        mwfr_bam_to_fastq_paired_end(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--files", required=True, type=str, multiple=True, help="BAM file accessions"
)
@click.option(
    "-d", "--donor", required=True, type=str, help="Accession of the associated donor"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def sample_identity_check(files, donor, auth_env):
    """Sample Identity Check accross BAMs. Run `sample_identity_check_status` before this."""
    smaht_key = get_auth_key(auth_env)
    mwfr_sample_identity_check(files, donor, smaht_key)


if __name__ == "__main__":
    cli()
