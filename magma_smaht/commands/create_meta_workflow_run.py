import click

from magma_smaht.create_metawfr import (
    mwfr_illumina_alignment,
    mwfr_pacbio_alignment,
    mwfr_fastqc,
    mwfr_hic_alignment,
    mwfr_ont_alignment,
    mwfr_cram_to_fastq_paired_end,
    mwfr_bamqc_short_read,
    mwfr_ubam_qc_long_read,
    mwfr_ultra_long_bamqc,
    mwfr_long_read_bamqc
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
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
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
def align_illumina(fileset_accession, length_required, auth_env):
    """Alignment MWFR for Illumina data"""
    smaht_key = get_auth_key(auth_env)
    mwfr_illumina_alignment(fileset_accession, length_required, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_pacbio(fileset_accession, auth_env):
    """Alignment MWFR for PacBio data"""
    smaht_key = get_auth_key(auth_env)
    mwfr_pacbio_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_hic(fileset_accession, auth_env):
    """Alignment MWFR for HIC data"""
    smaht_key = get_auth_key(auth_env)
    mwfr_hic_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def align_ont(fileset_accession, auth_env):
    """Alignment MWFR for ONT data"""
    smaht_key = get_auth_key(auth_env)
    mwfr_ont_alignment(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_short_read_fastq(fileset_accession, auth_env):
    """QC MWFR for short-read FASTQs"""
    smaht_key = get_auth_key(auth_env)
    mwfr_fastqc(fileset_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def qc_long_read_ubam(fileset_accession, auth_env):
    """QC MWFR for unaligned long-read BAMs"""
    smaht_key = get_auth_key(auth_env)
    mwfr_ubam_qc_long_read(fileset_accession, smaht_key)


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
def qc_ultra_long_bam(file_accession, auth_env):
    """QC MWFR for aligned, ultra-long BAMs (ONT)"""
    smaht_key = get_auth_key(auth_env)
    mwfr_ultra_long_bamqc(file_accession, smaht_key)


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
def qc_long_read_bam(file_accession, auth_env):
    """QC MWFR for aligned, long-read BAMs (PacBio)"""
    smaht_key = get_auth_key(auth_env)
    mwfr_long_read_bamqc(file_accession, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-f", "--fileset-accession", required=True, type=str, help="Fileset accession"
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def conversion_cram_to_fastq(fileset_accession, auth_env):
    """Conversion MWFR for CRAM to FASTQ (paired-end)"""
    smaht_key = get_auth_key(auth_env)
    mwfr_cram_to_fastq_paired_end(fileset_accession, smaht_key)


if __name__ == "__main__":
    cli()
