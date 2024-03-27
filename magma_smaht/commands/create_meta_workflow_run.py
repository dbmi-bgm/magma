import click

from magma_smaht.create_metawfr import (
    mwfr_illumina_alignment,
    mwfr_pacbio_alignment,
    mwfr_fastqc,
    mwfr_hic_alignment,
    mwfr_ont_alignment,
)
from magma_smaht.utils import get_auth_key


@click.command()
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
def mwfr_illumina_alignment_cmd(fileset_accession, length_required, auth_env):
    """Creates a MetaWorflowRun item in the portal for Illumina alignment of submitted files within a fileset"""
    smaht_key = get_auth_key(auth_env)
    mwfr_illumina_alignment(fileset_accession, length_required, smaht_key)


@click.command()
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
def mwfr_pacbio_alignment_cmd(fileset_accession, auth_env):
    """Creates a MetaWorflowRun item in the portal for PacBio alignment of submitted files within a fileset"""
    smaht_key = get_auth_key(auth_env)
    mwfr_pacbio_alignment(fileset_accession, smaht_key)


@click.command()
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
def mwfr_hic_alignment_cmd(fileset_accession, auth_env):
    """Creates a MetaWorflowRun item in the portal for HIC alignment of submitted files within a fileset"""
    smaht_key = get_auth_key(auth_env)
    mwfr_hic_alignment(fileset_accession, smaht_key)


@click.command()
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
def mwfr_ont_alignment_cmd(fileset_accession, auth_env):
    """Creates a MetaWorflowRun item in the portal for ONT alignment of submitted files within a fileset"""
    smaht_key = get_auth_key(auth_env)
    mwfr_ont_alignment(fileset_accession, smaht_key)



@click.command()
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
def mwfr_fastqc_cmd(fileset_accession, auth_env):
    smaht_key = get_auth_key(auth_env)
    mwfr_fastqc(fileset_accession, smaht_key)
