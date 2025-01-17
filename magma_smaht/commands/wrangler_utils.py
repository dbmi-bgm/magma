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
def associate_conversion_output_with_fileset(mwfr_identifier, auth_env):
    """Associate CRAM2FASTQ or BAM2FASTQ output with fileset"""
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
def reset_mwfrs(mwfr_uuids, auth_env):
    """Reset a list of MetaWorkflowRuns"""
    smaht_key = get_auth_key(auth_env)
    wrangler_utils.reset_mwfrs(mwfr_uuids, smaht_key)


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
    "-f",
    "--file-accessions",
    required=True,
    type=str,
    multiple=True,
    help="File accessions",
)
@click.option(
    "-m",
    "--mode",
    required=True,
    type=click.Choice(['keep_oldest', 'keep_newest']),
    help="Merge mode",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def merge_qc_items(file_accessions, mode, auth_env):
    """
    Merged QC items of a file.
    Mode "keep_oldest" will merge the qc values and patch them to the oldest qc_item. The other qc_items will be removed from the file
    Mode "keep_newest" will merge the qc values and patch them to the newest qc_item. The other qc_items will be removed from the file
    In general, QC values of newer QC items will overwrite existing QC values of older items
    """
    smaht_key = get_auth_key(auth_env)
    for f in file_accessions:
        print(f"Working on {f}")
        wrangler_utils.merge_qc_items(f, mode, smaht_key)


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
