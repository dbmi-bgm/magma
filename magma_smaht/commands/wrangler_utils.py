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
    "-r",
    "--mwfr-uuid",
    required=True,
    type=str,
    help="MetaWorkflowRun UUID or accession used as the basis for the new MWFR",
)
@click.option(
    "-m",
    "--mwf-uuid",
    required=True,
    type=str,
    help="MetaWorkflow UUID or accession used as the basis for the new MWFR",
)
@click.option(
    "-i",
    "--input-arg",
    required=True,
    type=str,
    help="argument_name of the input argument to use to calculate input structure, e.g. `input_files_bam`",
)
@click.option(
    "-s",
    "--steps-to-import",
    required=True,
    type=str,
    help="Comma-separated list of workflow run names to import from the old MWFR",
)
@click.option(
    "--remove-file-qc",
    required=False,
    type=str,
    help="Removes the QC item of the specified file",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def rerun_mwfr(mwfr_uuid: str, mwf_uuid : str, input_arg: str, steps_to_import: str, remove_file_qc: str, auth_env: str):
    """Creates a new MetaWorkflowRun based on the MWF and MWFR specified. All workflow runs 
    specified in steps_to_import will be imported from the given MWFR to the new MWFR. All
    input variables and other properties will be copied over. The old MWFR will deleted.
    """
    smaht_key = get_auth_key(auth_env)
    steps_to_import_list = steps_to_import.split(",")
    wrangler_utils.rerun_mwfr(mwfr_uuid, mwf_uuid, input_arg, steps_to_import_list, remove_file_qc, smaht_key)


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
    Merge QC items of a file.
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
    "-f",
    "--file-accession",
    required=True,
    type=str,
    help="File accession",
)
@click.option(
    "-k",
    "--keep-index",
    required=True,
    type=int,
    help="Index of the QC item to keep",
)
@click.option(
    "-r",
    "--release",
    default=False,
    is_flag=True,
    show_default=True,
    help="Release the remaining QC item",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def replace_qc_item(file_accession, keep_index, release, auth_env):
    """
    Replace the QC item of a file with the one at the given index.
    If a file has multiple QC items, this command will remove all but the one with given index.
    Can be useful if QC has been rerun and the old QC item is no longer needed. This function also
    releases the remaining QC item if the release flag is set.
    """
    smaht_key = get_auth_key(auth_env)
    wrangler_utils.replace_qc_item(file_accession, keep_index, release, smaht_key)


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
    "-d",
    "--dry-run",
    default=False,
    is_flag=True,
    show_default=True,
    help="Dry run",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def archive_unaligned_reads(fileset_accessions, dry_run, auth_env):
    """
    Archive (submitted) unaligned reads of a fileset. 
    Every submitted unaligned read in the fileset will receive the s3_lifecycle_categor=short_term_archive.
    """
    smaht_key = get_auth_key(auth_env)
    for f in fileset_accessions:
        print(f"Working on Fileset {f}")
        wrangler_utils.archive_unaligned_reads(f, dry_run, smaht_key)


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "-n",
    "--num-files",
    required=True,
    type=int,
    help="Number of files to check",
)
@click.option(
    "-e",
    "--auth-env",
    required=True,
    type=str,
    help="Name of environment in smaht-keys file",
)
def sample_identity_check_status(num_files, auth_env):
    """
    Check which files need to be checked for sample identity.
    """
    smaht_key = get_auth_key(auth_env)
    wrangler_utils.sample_identity_check_status(num_files, smaht_key)
        


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
