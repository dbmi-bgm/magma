import logging
from typing import Any, Dict, List

from dcicutils import ff_utils

from magma_smaht.create_metawfr import mwfr_from_input
from magma_smaht.utils import get_auth_key


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# def test_create_bamqc_mwfrs() -> None:
#     key = get_auth_key("staging")
#     formatted_inputs = get_bamqc_inputs(key)
#     import pdb; pdb.set_trace()
#     for formatted_input in formatted_inputs:
#         mwf_identifier = "SMAMWZ2LM2RP"
#         input_arg = "input_files_bam"
#         mwfr = mwfr_from_input(mwf_identifier, formatted_input, input_arg, key)
#         post_response = post_meta_workflow_run(mwfr, key)
#         assert post_response["status"] == "success"
#         logger.info("Posted MetaWorkflowRun %s", post_response["@graph"][0]["uuid"])


def get_bamqc_inputs(key: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
    result = []
    total_files = 0
    file_uuid = "03c3c52c-b540-4b7e-b447-1634f9e2aba8"
    files_to_run = [
        ff_utils.get_metadata(file_uuid, key=key)
    ]
    total_files += 1
    logger.info(f"Found {total_files} BAMs for BAMQC")
    input_ = [
        {
              "argument_name": "input_files_bam",
              "argument_type": "file",
              "files": [
                  {"file": file["uuid"], "dimension": str(idx)}
                  for idx, file in enumerate(files_to_run)
              ],
        }
    ]
    result.append(input_)

#    file_sets = ff_utils.search_metadata("search/?type=FileSet", key=key)
#    for file_set in file_sets:
#        files_to_run = []
#        files = ff_utils.search_metadata(
#            (
#                f"search/?type=File&file_format.display_title=bam"
#                f"&description=analysis-ready+BAM"
#                f"&file_sets.uuid={file_set['uuid']}"
#                f"&quality_metrics=No+value"
#            ),
#            key=key,
#        )
#        if not files:
#            logger.info("file set %s with all files run" % file_set["uuid"])
#            continue
#        else:
#            logger.info(
#                "%s files found for file set %s"
#                % (len(files), file_set["uuid"])
#            )
#
#
#        files_to_run += files
#        total_files += len(files)
#        input_ = [
#            {
#                  "argument_name": "input_files_bam",
#                  "argument_type": "file",
#                  "files": [
#                      {"file": file["uuid"], "dimension": str(idx)}
#                      for idx, file in enumerate(files_to_run)
#                  ],
#            }
#        ]
#        result.append(input_)
    logger.info("%s files to run for BAMQC" % total_files)
    return result




# def test_create_fiberseq_alignment_mwfrs() -> None:
#     key = get_auth_key("staging")
#     formatted_inputs = get_fiberseq_alignment_inputs(key)
#     import pdb; pdb.set_trace()
#     for formatted_input in formatted_inputs:
#         mwf_identifier = "SMAMWV7W5T2D"
#         input_arg = "input_files_bam"
#         mwfr = mwfr_from_input(mwf_identifier, formatted_input, input_arg, key)
#         post_response = post_meta_workflow_run(mwfr, key)
#         assert post_response["status"] == "success"
#         logger.info("Posted MetaWorkflowRun %s", post_response["@graph"][0]["uuid"])


def get_fiberseq_alignment_inputs(key: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
    result = []
    total_files = 0
    file_set_accessions = [
        "SMAFSCDPM1SA",
        "SMAFSSJ98QHL",
        "SMAFSLTWXNP3",
        "SMAFSZNMU83N",
    ]
    file_sets = [
        ff_utils.get_metadata(accession, key=key)
        for accession in file_set_accessions
    ]
    for file_set in file_sets:
        files_to_run = []
        files = ff_utils.search_metadata(
            (
                f"search/?type=SubmittedFile&file_format.display_title=bam"
                f"&file_sets.uuid={file_set['uuid']}"
            ),
            key=key,
        )
        if not files:
            logger.info("file set %s with all files run" % file_set["uuid"])
            continue
        else:
            logger.info(
                "%s files found for file set %s"
                % (len(files), file_set["uuid"])
            )
        for file in files:
            mwfr_created = ff_utils.search_metadata(
                f"search/?type=MetaWorkflowRun&input.files.file.uuid={file['uuid']}",
                key=key,
            )
            if mwfr_created:
                break
        if mwfr_created:
            logger.info(
                "file set %s with a file already aligned. Not running now."
                % file_set["uuid"]
            )
            continue
        files_to_run += files
        total_files += len(files)
        library_uuids = file_set.get("libraries", [])
        assert len(library_uuids) == 1
        library_uuid = library_uuids[0]["uuid"]
        library = ff_utils.get_metadata(library_uuid, key=key, add_on="frame=raw")
        analyte_uuid = library.get("analyte", [])
        assert analyte_uuid
        analyte = ff_utils.get_metadata(analyte_uuid, key=key, add_on="frame=raw")
        sample_uuids = analyte.get("samples", [])
        assert len(sample_uuids) == 1
        sample_uuid = sample_uuids[0]
        sample = ff_utils.get_metadata(sample_uuid, key=key)
        input_ = [
            {
                  "argument_name": "input_files_bam",
                  "argument_type": "file",
                  "files": [
                      {"file": file["uuid"], "dimension": str(idx)}
                      for idx, file in enumerate(files_to_run)
                  ],
            },
            {
                'argument_name': 'sample_name',
                'argument_type': 'parameter',
                'value': sample["accession"],
            },
            {
                'argument_name': 'library_id',
                'argument_type': 'parameter',
                'value': library["accession"],
            },
        ]
        result.append(input_)
    logger.info("%s file sets to run for fiberseq alignment" % len(input_))
    return result




# def test_create_fastqc_mwfrs() -> None:
#     key = get_auth_key("staging")
#     formatted_inputs = get_fastqc_input(key)
#     import pdb; pdb.set_trace()
#     for formatted_input in formatted_inputs:
#         mwf_identifier = "SMAMW2QDZ2SQ"
#         input_arg = "input_files_fastq_gz"
#         mwfr = mwfr_from_input(mwf_identifier, formatted_input, input_arg, key)
#         post_response = post_meta_workflow_run(mwfr, key)
#         assert post_response["status"] == "success"
#         logger.info("Posted MetaWorkflowRun %s", post_response["@graph"][0]["uuid"])


def get_fastqc_input(key):
    result = []
    total_files = 0
    file_sets = ff_utils.search_metadata("search/?type=FileSet", key=key)
    for file_set in file_sets:
        files_to_run = []
        files = ff_utils.search_metadata(
            (
                f"search/?type=SubmittedFile&file_format.display_title=fastq_gz"
                f"&file_sets.uuid={file_set['uuid']}"
                f"&quality_metrics=No+value"
            ),
            key=key,
        )
        if not files:
            logger.info("file set %s with all files run" % file_set["uuid"])
            continue
        else:
            logger.info(
                "%s files found for file set %s"
                % (len(files), file_set["uuid"])
            )


        files_to_run += files
        if total_files >= 500:
            break
        total_files += len(files)
        input_ = [
            {
                  "argument_name": "input_files_fastq_gz",
                  "argument_type": "file",
                  "files": [
                      {"file": file["uuid"], "dimension": str(idx)}
                      for idx, file in enumerate(files_to_run)
                  ],
            }
        ]
        result.append(input_)
    logger.info("%s files to run for FASTQC" % total_files)
    return result


def post_meta_workflow_run(post_body, key):
    return ff_utils.post_metadata(post_body, "MetaWorkflowRun", key)


# def test_create_fastq_alignment_mwfr():
#     key = get_auth_key("staging")
#     input_ = get_input(key)
#     mwf_uuid = "df0466e1-356d-45bc-bd55-950704596a1b"
#     input_arg = "input_files_r1_fastq_gz"
#     mwfr = mwfr_from_input(mwf_uuid, input_, input_arg, key)
#     post_response = ff_utils.post_metadata(mwfr, "MetaWorkflowRun", key)
#     assert post_response["status"] == "success"
#     logger.info("Posted MetaWorkflowRun %s", post_response["@graph"][0]["uuid"])


def get_fastq_input(key: Dict[str, Any]) -> Dict[str, Any]:
    file_sets = ff_utils.search_metadata("search/?type=FileSet", key=key)
    file_sets_to_run = []
    for file_set in file_sets:
        files = ff_utils.search_metadata(
            f"search/?type=SubmittedFile&file_sets.uuid={file_set['uuid']}",
            key=key,
        )
        assert files
        file = files[0]
        mwfr_created = ff_utils.search_metadata(
            f"search/?type=MetaWorkflowRun&input.files.file.uuid={file['uuid']}",
            key=key,
        )
        if mwfr_created:
            logger.info("File set %s already aligned", file_set["uuid"])
        else:
            file_sets_to_run.append(file_set)
    logger.info("%s file sets remain to run", len(file_sets_to_run))
    file_set = file_sets_to_run[0]
    file_set_uuid = file_set["uuid"]
    library_uuids = file_set.get("libraries", [])
    assert len(library_uuids) == 1
    library_uuid = library_uuids[0]["uuid"]
    library = ff_utils.get_metadata(library_uuid, key=key, add_on="frame=raw")
    analyte_uuid = library.get("analyte", [])
    assert analyte_uuid
    analyte = ff_utils.get_metadata(analyte_uuid, key=key, add_on="frame=raw")
    sample_uuids = analyte.get("samples", [])
    assert len(sample_uuids) == 1
    sample_uuid = sample_uuids[0]
    sample = ff_utils.get_metadata(sample_uuid, key=key)
    file_set_files = ff_utils.search_metadata(
        f"search/?type=SubmittedFile&file_sets.uuid={file_set_uuid}",
        key=key,
    )
    logger.info("%s files for file set to run", len(file_set_files))
    r2_fastqs = [file for file in file_set_files if file["read_pair_number"] == "R2"]
    logger.info("%s FASTQ pairs for file set", len(r2_fastqs))
    r2_fastq_uuids = [file["uuid"] for file in r2_fastqs]
    r1_fastq_uuids = [file["paired_with"]["uuid"] for file in r2_fastqs]
    assert len(r1_fastq_uuids) == len(r2_fastq_uuids)
    input_ = [
        {
            'argument_name': 'input_files_r1_fastq_gz',
            'argument_type': 'file',
            'files': [
                {"file": file_uuid, "dimension": str(idx)}
                for idx, file_uuid in enumerate(r1_fastq_uuids)
            ]
        },
        {
            'argument_name': 'input_files_r2_fastq_gz',
            'argument_type': 'file',
            'files': [
                {"file": file_uuid, "dimension": str(idx)}
                for idx, file_uuid in enumerate(r2_fastq_uuids)
            ]
        },
        {
            'argument_name': 'sample_name',
            'argument_type': 'parameter',
            'value': sample["accession"],
        },
        {
            'argument_name': 'library_id',
            'argument_type': 'parameter',
            'value': library["accession"],
        }
    ]
    return input_
