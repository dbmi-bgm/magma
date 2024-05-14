import click

from magma_smaht.wrangler_utils import (
    associate_conversion_output_with_fileset,
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
    "-m",
    "--mwfr-identifier",
    required=True,
    type=str,
    help="Conversion MetaWorkflowRun identifier",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def cram2fastq_out_to_fileset(mwfr_identifier, auth_env):
    """Associate CRAM2FASTQ output with fileset"""
    smaht_key = get_auth_key(auth_env)
    associate_conversion_output_with_fileset(mwfr_identifier, smaht_key)


if __name__ == "__main__":
    cli()
