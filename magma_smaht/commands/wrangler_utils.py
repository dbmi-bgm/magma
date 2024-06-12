import click
from magma_smaht.utils import get_auth_key
import magma_smaht.wrangler_utils as wrangler_utils


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
    wrangler_utils.associate_conversion_output_with_fileset(mwfr_identifier, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-m",
    "--mwfr-uuids",
    required=True,
    type=str,
    multiple=True,
    help="List of MWFRs to reset",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def reset_failed_mwfrs(mwfr_uuids, auth_env):
    """Reset a list of failed MetaWorkflowRuns"""
    smaht_key = get_auth_key(auth_env)
    wrangler_utils.reset_failed_mwfrs(mwfr_uuids, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def reset_all_failed_mwfrs(auth_env):
    """Reset all failed MetaWorkflowRuns on the portal"""
    smaht_key = get_auth_key(auth_env)
    wrangler_utils.reset_all_failed_mwfrs(smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-i",
    "--identifier",
    required=True,
    type=str,
    help="Item UUID",
)
@click.option(
    "-p",
    "--property-key",
    required=True,
    type=str,
    help="Item property",
)
@click.option(
    "-v",
    "--property-value",
    required=True,
    type=str,
    help="Item property value",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def set_property(identifier,property_key,property_value,auth_env):
    """Set item property to value by uuid. """
    smaht_key = get_auth_key(auth_env)
    wrangler_utils.set_property(identifier,property_key,property_value,smaht_key)


if __name__ == "__main__":
    cli()
