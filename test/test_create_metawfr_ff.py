import datetime
import json
from contextlib import contextmanager
from copy import deepcopy
from typing import Iterator, List

import mock
import pytest

import magma_ff.create_metawfr as create_mwfr_module
from magma_ff.create_metawfr import (
    InputPropertiesFromSampleProcessing,
    InputPropertiesFromSample,
    MetaWorkflowRunCreationError,
    MetaWorkflowRunFromSampleProcessing,
    MetaWorkflowRunFromSample,
    MetaWorkflowRunFromItem,
    MetaWorkflowRunInput,
    create_meta_workflow_run,
    get_files_for_file_formats,
    _get_item_types,
    _is_item_of_type,
)
from magma_ff.utils import JsonObject
from test.utils import patch_context

BAM_UUID_1 = "bam_sample_1"
BAM_UUID_2 = "bam_sample_2"
BAM_UUID_3 = "bam_sample_3"
BAM_UUID_4 = "bam_sample_4"
BAM_UUIDS = [[BAM_UUID_3], [BAM_UUID_4], [BAM_UUID_2], [BAM_UUID_1]]
GVCF_UUID_1 = "gvcf_sample_1"
GVCF_UUID_1_2 = "gvcf_sample_1_2"
GVCF_UUID_2 = "gvcf_sample_2"
GVCF_UUID_3 = "gvcf_sample_3"
GVCF_UUID_4 = "gvcf_sample_4"
GVCF_UUIDS = [[GVCF_UUID_3], [GVCF_UUID_4], [GVCF_UUID_2], [GVCF_UUID_1, GVCF_UUID_1_2]]
CRAM_UUID_1 = "cram_sample_1"
CRAM_UUID_2 = "cram_sample_2"
CRAM_UUID_3 = "cram_sample_3"
CRAM_UUID_4 = "cram_sample_4"
INPUT_CRAMS = [[CRAM_UUID_3], [CRAM_UUID_4], [CRAM_UUID_2], [CRAM_UUID_1]]
FASTQ_R1_UUID_1 = "fastq_r1_sample_1"
FASTQ_R1_UUID_1_2 = "fastq_r1_sample_1_2"
FASTQ_R1_UUID_2 = "fastq_r1_sample_2"
FASTQ_R1_UUID_3 = "fastq_r1_sample_3"
FASTQ_R1_UUID_4 = "fastq_r1_sample_4"
FASTQ_R1_UUIDS = [
    [FASTQ_R1_UUID_3],
    [FASTQ_R1_UUID_4],
    [FASTQ_R1_UUID_2],
    [FASTQ_R1_UUID_1, FASTQ_R1_UUID_1_2],
]
FASTQ_R2_UUID_1 = "fastq_r2_sample_1"
FASTQ_R2_UUID_1_2 = "fastq_r2_sample_1_2"
FASTQ_R2_UUID_2 = "fastq_r2_sample_2"
FASTQ_R2_UUID_3 = "fastq_r2_sample_3"
FASTQ_R2_UUID_4 = "fastq_r2_sample_4"
FASTQ_R2_UUIDS = [
    [FASTQ_R2_UUID_3],
    [FASTQ_R2_UUID_4],
    [FASTQ_R2_UUID_2],
    [FASTQ_R2_UUID_1, FASTQ_R2_UUID_1_2],
]
PROJECT = "/projects/cgap-core/"
INSTITUTION = "/institutions/hms-dbmi/"
SAMPLE_NAME_1 = "SAMPLE1-DNA-WGS"
SAMPLE_NAME_2 = "SAMPLE2-DNA-WGS"
SAMPLE_NAME_3 = "SAMPLE3-DNA-WGS"
SAMPLE_NAME_4 = "SAMPLE4-DNA-WGS"
SORTED_SAMPLE_NAMES = [SAMPLE_NAME_3, SAMPLE_NAME_4, SAMPLE_NAME_2, SAMPLE_NAME_1]
SAMPLE_UUID_1 = "uuid_1"
SAMPLE_UUID_2 = "uuid_2"
SAMPLE_UUID_3 = "uuid_3"
SAMPLE_UUID_4 = "uuid_4"
SORTED_SAMPLE_UUIDS = [SAMPLE_UUID_3, SAMPLE_UUID_4, SAMPLE_UUID_2, SAMPLE_UUID_1]
RCKTAR_FILE_NAMES = [f"{sample_name}.rck.gz" for sample_name in SORTED_SAMPLE_NAMES]
SAMPLE_1 = {
    "uuid": SAMPLE_UUID_1,
    "bam_sample_id": SAMPLE_NAME_1,
    "files": [{"uuid": CRAM_UUID_1, "file_format": {"file_format": "cram"}}],
    "processed_files": [
        {"uuid": BAM_UUID_2, "file_format": {"file_format": "bam"}},
        {"uuid": BAM_UUID_1, "file_format": {"file_format": "bam"}},
        {"uuid": GVCF_UUID_1, "file_format": {"file_format": "gvcf_gz"}},
        {"uuid": GVCF_UUID_1_2, "file_format": {"file_format": "gvcf_gz"}},
        {
            "uuid": FASTQ_R1_UUID_1,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
        },
        {
            "uuid": FASTQ_R1_UUID_1_2,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
        },
        {
            "uuid": FASTQ_R2_UUID_1,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
        },
        {
            "uuid": FASTQ_R2_UUID_1_2,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
        },
    ],
    "@type": ["Sample", "Item"],
}
SAMPLE_2 = {
    "uuid": SAMPLE_UUID_2,
    "bam_sample_id": SAMPLE_NAME_2,
    "files": [
        {
            "uuid": FASTQ_R1_UUID_2,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
            "related_files": [
                {
                    "file": {
                        "uuid": FASTQ_R2_UUID_2,
                        "file_format": {"file_format": "fastq"},
                        "paired_end": "2",
                    },
                    "relationship_type": "paired with",
                }
            ],
        },
        {
            "uuid": FASTQ_R2_UUID_2,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
            "related_files": [
                {
                    "file": {
                        "uuid": FASTQ_R1_UUID_2,
                        "file_format": {"file_format": "fastq"},
                        "paired_end": "1",
                    },
                    "relationship_type": "paired with",
                }
            ],
        },
        {"uuid": CRAM_UUID_2, "file_format": {"file_format": "cram"}},
    ],
    "processed_files": [
        {"uuid": BAM_UUID_2, "file_format": {"file_format": "bam"}},
        {"uuid": GVCF_UUID_2, "file_format": {"file_format": "gvcf_gz"}},
    ],
}
SAMPLE_3 = {
    "uuid": SAMPLE_UUID_3,
    "project": PROJECT,
    "institution": INSTITUTION,
    "bam_sample_id": SAMPLE_NAME_3,
    "files": [
        {
            "uuid": FASTQ_R1_UUID_3,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
            "related_files": [
                {
                    "file": {
                        "uuid": FASTQ_R2_UUID_3,
                        "file_format": {"file_format": "fastq"},
                        "paired_end": "2",
                    },
                    "relationship_type": "paired with",
                }
            ],
        },
        {
            "uuid": FASTQ_R2_UUID_3,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
            "related_files": [
                {
                    "file": {
                        "uuid": FASTQ_R1_UUID_3,
                        "file_format": {"file_format": "fastq"},
                        "paired_end": "1",
                    },
                    "relationship_type": "paired with",
                }
            ],
        },
        {"uuid": CRAM_UUID_3, "file_format": {"file_format": "cram"}},
    ],
    "processed_files": [
        {"uuid": BAM_UUID_3, "file_format": {"file_format": "bam"}},
        {"uuid": GVCF_UUID_3, "file_format": {"file_format": "gvcf_gz"}},
    ],
}
SAMPLE_4 = {
    "uuid": SAMPLE_UUID_4,
    "bam_sample_id": SAMPLE_NAME_4,
    "files": [
        {
            "uuid": FASTQ_R1_UUID_4,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
            "related_files": [
                {
                    "file": {
                        "uuid": FASTQ_R2_UUID_4,
                        "file_format": {"file_format": "fastq"},
                        "paired_end": "2",
                    },
                    "relationship_type": "paired with",
                }
            ],
        },
        {
            "uuid": FASTQ_R2_UUID_4,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
            "related_files": [
                {
                    "file": {
                        "uuid": FASTQ_R1_UUID_4,
                        "file_format": {"file_format": "fastq"},
                        "paired_end": "1",
                    },
                    "relationship_type": "paired with",
                }
            ],
        },
        {"uuid": CRAM_UUID_4, "file_format": {"file_format": "cram"}},
    ],
    "processed_files": [
        {"uuid": BAM_UUID_4, "file_format": {"file_format": "bam"}},
        {"uuid": GVCF_UUID_4, "file_format": {"file_format": "gvcf_gz"}},
    ],
}
SAMPLES = [SAMPLE_1, SAMPLE_2, SAMPLE_3, SAMPLE_4]
SAMPLE_3_CRAMS = [[CRAM_UUID_3]]
SAMPLE_3_GVCFS = [[GVCF_UUID_3]]
SAMPLE_3_FASTQS_R1 = [[FASTQ_R1_UUID_3]]
SAMPLE_3_FASTQS_R2 = [[FASTQ_R2_UUID_3]]
SAMPLE_3_FASTQS = [[FASTQ_R1_UUID_3, FASTQ_R2_UUID_3]]
SAMPLE_3_BAMS = [[BAM_UUID_3]]
SAMPLE_3_RCKTARS = [SAMPLE_NAME_3 + ".rck.gz"]
SAMPLE_3_NAMES = [SAMPLE_NAME_3]
SAMPLE_3_UUIDS = [SAMPLE_UUID_3]
SAMPLE_PROCESSING_FAMILY_SIZE = 4
PEDIGREE_1 = {
    "individual": "GAPID000001",
    "sample_accession": "GAPSA000001",
    "sample_name": "SAMPLE1-DNA-WGS",
    "parents": ["GAPID000003"],
    "relationship": "daughter",
    "sex": "F",
    "bam_location": "uuid1/GAPFI00001.bam",
}
PEDIGREE_2 = {
    "individual": "GAPID000002",
    "sample_accession": "GAPSA000002",
    "sample_name": "SAMPLE2-DNA-WGS",
    "parents": [],
    "relationship": "father",
    "sex": "M",
    "bam_location": "uuid2/GAPFI00002.bam",
}
PEDIGREE_3 = {
    "individual": "GAPID000003",
    "sample_accession": "GAPSA000003",
    "sample_name": "SAMPLE3-DNA-WGS",
    "parents": ["GAPID000002", "GAPID000004"],
    "relationship": "proband",
    "sex": "F",
    "bam_location": "uuid3/GAPFI00003.bam",
}
PEDIGREE_4 = {
    "individual": "GAPID000004",
    "sample_accession": "GAPSA000004",
    "sample_name": "SAMPLE4-DNA-WGS",
    "parents": ["GAPID000005"],
    "relationship": "mother",
    "sex": "F",
    "bam_location": "uuid4/GAPFI00004.bam",
}
CLEANED_PEDIGREE_4 = {
    "individual": "GAPID000004",
    "sample_accession": "GAPSA000004",
    "sample_name": "SAMPLE4-DNA-WGS",
    "parents": [],
    "relationship": "mother",
    "sex": "F",
    "bam_location": "uuid4/GAPFI00004.bam",
}
SAMPLES_PEDIGREE = [PEDIGREE_1, PEDIGREE_2, PEDIGREE_3, PEDIGREE_4]
PEDIGREE = [
    {
        "individual": "GAPID000003",
        "sample_name": "SAMPLE3-DNA-WGS",
        "parents": ["GAPID000002", "GAPID000004"],
        "gender": "F",
    },
    {
        "individual": "GAPID000004",
        "sample_name": "SAMPLE4-DNA-WGS",
        "parents": [],
        "gender": "F",
    },
    {
        "individual": "GAPID000002",
        "sample_name": "SAMPLE2-DNA-WGS",
        "parents": [],
        "gender": "M",
    },
    {
        "individual": "GAPID000001",
        "sample_name": "SAMPLE1-DNA-WGS",
        "parents": ["GAPID000003"],
        "gender": "F",
    },
]
SAMPLE_NAME_PROBAND = ["SAMPLE3-DNA-WGS"]
BAMSNAP_TITLES = [
    "SAMPLE3-DNA-WGS (proband)",
    "SAMPLE4-DNA-WGS (mother)",
    "SAMPLE2-DNA-WGS (father)",
    "SAMPLE1-DNA-WGS (daughter)",
]
SORTED_INDICES = [2, 3, 1, 0]
SORTED_SAMPLES_PEDIGREE = [PEDIGREE_3, CLEANED_PEDIGREE_4, PEDIGREE_2, PEDIGREE_1]
SORTED_SAMPLES = [SAMPLE_3, SAMPLE_4, SAMPLE_2, SAMPLE_1]
INPUT_SAMPLES = [sample["uuid"] for sample in SORTED_SAMPLES]
SNV_VCF_UUID = "some_vcf"
SNV_VCF_FILE_METADATA = {
    "uuid": SNV_VCF_UUID,
    "file_format": {"file_format": "vcf_gz"},
    "foo": "bar",
    "variant_type": "SNV",
}
SV_VCF_UUID = "some_other_vcf"
SV_VCF_FILE_METADATA = {
    "uuid": SV_VCF_UUID,
    "file_format": {"file_format": "vcf"},
    "variant_type": "SV",
}
VCF_FILES = [SNV_VCF_FILE_METADATA, SV_VCF_FILE_METADATA]
VCF_UUIDS = [SNV_VCF_UUID, SV_VCF_UUID]
SOME_FILE_UUID = "some_file_uuid"
SOME_FILE_METADATA = {"uuid": SOME_FILE_UUID, "file_format": {"file_format": "bar"}}
SAMPLE_PROCESSING_SUBMITTED_FILES = VCF_FILES + [SOME_FILE_METADATA]
SAMPLE_PROCESSING_UUID = "some_uuid"
SAMPLE_PROCESSING_META_WORKFLOW_RUNS = [{"uuid": "run_1"}, {"uuid": "run_2"}]
SAMPLE_PROCESSING = {
    "uuid": SAMPLE_PROCESSING_UUID,
    "project": PROJECT,
    "institution": INSTITUTION,
    "samples_pedigree": SAMPLES_PEDIGREE,
    "samples": SAMPLES,
    "meta_workflow_runs": SAMPLE_PROCESSING_META_WORKFLOW_RUNS,
    "files": SAMPLE_PROCESSING_SUBMITTED_FILES,
    "@type": ["SampleProcessing", "Item"],
}
ITEM_UUID = "item_uuid"
ARBITRARY_ITEM = {
    "uuid": ITEM_UUID,
    "project": PROJECT,
    "institution": INSTITUTION,
}
INPUT_BAMS = "input_bams"
INPUT_FASTQS_R1 = "fastqs_r1"
FAMILY_SIZE = "family_size"
PROBAND_ONLY_FALSE = False
META_WORKFLOW_UUID = "1234"
META_WORKFLOW = {
    "uuid": META_WORKFLOW_UUID,
    "title": "WGS Family v0.0.0",
    "input": [
        {"argument_name": INPUT_BAMS, "argument_type": "file", "dimensionality": 2},
        {
            "argument_name": FAMILY_SIZE,
            "argument_type": "parameter",
            "value_type": "integer",
        },
    ],
    "workflows": [
        {
            "name": "workflow_do-something",
            "workflow": "some_uuid",
            "config": {
                "run_name": "A fine workflow",
            },
            "input": [
                {
                    "scatter": 1,
                    "argument_name": "input_bam",
                    "argument_type": "file",
                    "source_argument_name": INPUT_BAMS,
                },
            ],
        },
    ],
    "proband_only": PROBAND_ONLY_FALSE,
}
META_WORKFLOW_FOR_SAMPLE = {
    "uuid": META_WORKFLOW_UUID,
    "title": "FASTQ QC v0.0.0",
    "input": [
        {
            "argument_name": INPUT_FASTQS_R1,
            "argument_type": "file",
            "dimensionality": 2,
        },
    ],
    "workflows": [
        {
            "name": "workflow_do-something",
            "workflow": "some_uuid",
            "config": {
                "run_name": "A fine workflow",
            },
            "input": [
                {
                    "scatter": 1,
                    "argument_name": "input_fastq_r1",
                    "argument_type": "file",
                    "source_argument_name": INPUT_FASTQS_R1,
                },
            ],
        },
    ],
    "proband_only": PROBAND_ONLY_FALSE,
}
INPUT_PROPERTIES_INPUT_BAMS = [[BAM_UUID_1], [BAM_UUID_2]]
INPUT_PROPERTIES_FAMILY_SIZE = 2
AUTH_KEY = {"key": "foo"}
TODAY = datetime.date.today().isoformat()
META_WORKFLOW_RUN_UUID = "another_uuid"
COMMON_FIELDS = {
    "project": PROJECT,
    "institution": INSTITUTION,
    "associated_meta_workflow_runs": [META_WORKFLOW_RUN_UUID],
}
META_WORKFLOW_RUN = {
    "meta_workflow": META_WORKFLOW_UUID,
    "input": [
        {
            "argument_name": "family_size",
            "argument_type": "parameter",
            "value_type": "integer",
            "value": "4",
        },
        {
            "argument_name": "input_bams",
            "argument_type": "file",
            "files": [
                {"file": "bam_sample_3", "dimension": "0,0"},
                {"file": "bam_sample_4", "dimension": "1,0"},
                {"file": "bam_sample_2", "dimension": "2,0"},
                {"file": "bam_sample_1", "dimension": "3,0"},
            ],
        },
    ],
    "title": "MetaWorkflowRun WGS Family v0.0.0 from %s" % TODAY,
    "project": PROJECT,
    "institution": INSTITUTION,
    "common_fields": COMMON_FIELDS,
    "input_samples": INPUT_SAMPLES,
    "associated_sample_processing": SAMPLE_PROCESSING_UUID,
    "final_status": "pending",
    "workflow_runs": [
        {"name": "workflow_do-something", "status": "pending", "shard": "0"},
        {"name": "workflow_do-something", "status": "pending", "shard": "1"},
        {"name": "workflow_do-something", "status": "pending", "shard": "2"},
        {"name": "workflow_do-something", "status": "pending", "shard": "3"},
    ],
    "uuid": META_WORKFLOW_RUN_UUID,
}
META_WORKFLOW_RUN_NO_WORKFLOW_RUNS = {
    "meta_workflow": META_WORKFLOW_UUID,
    "input": [
        {
            "argument_name": "family_size",
            "argument_type": "parameter",
            "value_type": "integer",
            "value": "4",
        },
        {
            "argument_name": "input_bams",
            "argument_type": "file",
            "files": [
                {"file": "bam_sample_3", "dimension": "0,0"},
                {"file": "bam_sample_4", "dimension": "1,0"},
                {"file": "bam_sample_2", "dimension": "2,0"},
                {"file": "bam_sample_1", "dimension": "3,0"},
            ],
        },
    ],
    "title": "MetaWorkflowRun WGS Family v0.0.0 from %s" % TODAY,
    "project": PROJECT,
    "institution": INSTITUTION,
    "common_fields": COMMON_FIELDS,
    "input_samples": INPUT_SAMPLES,
    "associated_sample_processing": SAMPLE_PROCESSING_UUID,
    "final_status": "pending",
    "workflow_runs": [],
    "uuid": META_WORKFLOW_RUN_UUID,
}
META_WORKFLOW_RUN_NO_FILES_INPUT = {
    "meta_workflow": META_WORKFLOW_UUID,
    "input": [
        {
            "argument_name": "family_size",
            "argument_type": "parameter",
            "value_type": "integer",
            "value": "4",
        },
    ],
    "title": "MetaWorkflowRun WGS Family v0.0.0 from %s" % TODAY,
    "project": PROJECT,
    "institution": INSTITUTION,
    "common_fields": COMMON_FIELDS,
    "input_samples": INPUT_SAMPLES,
    "associated_sample_processing": SAMPLE_PROCESSING_UUID,
    "final_status": "pending",
    "workflow_runs": [],
    "uuid": META_WORKFLOW_RUN_UUID,
}
META_WORKFLOW_RUN_FOR_SAMPLE_3 = {
    "meta_workflow": META_WORKFLOW_UUID,
    "input": [
        {
            "argument_name": INPUT_FASTQS_R1,
            "argument_type": "file",
            "files": [
                {"file": FASTQ_R1_UUID_3, "dimension": "0,0"},
            ],
        },
    ],
    "title": "MetaWorkflowRun FASTQ QC v0.0.0 from %s" % TODAY,
    "project": PROJECT,
    "institution": INSTITUTION,
    "common_fields": COMMON_FIELDS,
    "input_samples": [SAMPLE_UUID_3],
    "final_status": "pending",
    "workflow_runs": [
        {"name": "workflow_do-something", "status": "pending", "shard": "0"},
    ],
    "uuid": META_WORKFLOW_RUN_UUID,
}


@pytest.fixture
def meta_workflow_run_input():
    """MetaWorkflowRunInput class for tests."""
    return MetaWorkflowRunInput(META_WORKFLOW, InputPropertiesForTest())


@pytest.fixture
def inputs_from_sample_processing():
    """Class for tests."""
    return InputPropertiesFromSampleProcessing(deepcopy(SAMPLE_PROCESSING))


@pytest.fixture
def inputs_from_sample():
    """Class for tests."""
    return InputPropertiesFromSample(deepcopy(SAMPLE_3))


@pytest.fixture
def meta_workflow_run_from_item():
    """Class for testing with portal requests mocked."""
    with mock.patch(
        "magma_ff.create_metawfr.make_embed_request",
        return_value=ARBITRARY_ITEM,
    ):
        with mock.patch(
            ("magma_ff.create_metawfr.MetaWorkflowRunFromItem" ".get_item_properties"),
            return_value=META_WORKFLOW,
        ):
            with mock.patch(
                "magma_ff.create_metawfr.uuid.uuid4",
                return_value=META_WORKFLOW_RUN_UUID,
            ):
                return MetaWorkflowRunFromItem(None, None, AUTH_KEY)


@pytest.fixture
def meta_workflow_run_from_sample_processing():
    """Class for testing with portal requests mocked."""
    with mock.patch(
        "magma_ff.create_metawfr.make_embed_request",
        return_value=SAMPLE_PROCESSING,
    ):
        with mock.patch(
            (
                "magma_ff.create_metawfr.MetaWorkflowRunFromSampleProcessing"
                ".get_item_properties"
            ),
            return_value=META_WORKFLOW,
        ):
            with mock.patch(
                "magma_ff.create_metawfr.uuid.uuid4",
                return_value=META_WORKFLOW_RUN_UUID,
            ):
                return MetaWorkflowRunFromSampleProcessing(None, None, AUTH_KEY)


@pytest.fixture
def meta_workflow_run_from_sample():
    """Class for testing with portal requests mocked."""
    with mock.patch(
        "magma_ff.create_metawfr.make_embed_request",
        return_value=SAMPLE_3,
    ):
        with mock.patch(
            (
                "magma_ff.create_metawfr.MetaWorkflowRunFromSample"
                ".get_item_properties"
            ),
            return_value=META_WORKFLOW_FOR_SAMPLE,
        ):
            with mock.patch(
                "magma_ff.create_metawfr.uuid.uuid4",
                return_value=META_WORKFLOW_RUN_UUID,
            ):
                return MetaWorkflowRunFromSample(None, None, AUTH_KEY)


@contextmanager
def patch_get_metadata(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_mwfr_module.ff_utils, "get_metadata", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_create_from_sample(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_mwfr_module, "create_meta_workflow_run_from_sample", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_create_from_sample_processing(**kwargs) -> Iterator[mock.MagicMock]:
    with patch_context(
        create_mwfr_module, "create_meta_workflow_run_from_sample_processing", **kwargs
    ) as mock_item:
        yield mock_item


@pytest.mark.parametrize(
    "item,exception_expected,from_sample_expected,from_sample_processing_expected",
    [
        ({}, True, False, False),
        (META_WORKFLOW, True, False, False),
        (SAMPLE_1, False, True, False),
        (SAMPLE_PROCESSING, False, False, True),
    ],
)
def test_create_meta_workflow_run(
    item: JsonObject,
    exception_expected: bool,
    from_sample_expected: bool,
    from_sample_processing_expected: bool,
) -> None:
    item_identifier = "foo"
    meta_workflow_identifier = "bar"
    auth_key = "fu"
    post = True
    patch = False
    with patch_get_metadata(return_value=item) as mock_get_metadata:
        with patch_create_from_sample() as mock_create_from_sample:
            with patch_create_from_sample_processing() as mock_create_from_sample_processing:
                if exception_expected:
                    with pytest.raises(MetaWorkflowRunCreationError):
                        create_meta_workflow_run(
                            item_identifier,
                            meta_workflow_identifier,
                            auth_key,
                            post=post,
                            patch=patch,
                        )
                else:
                    create_meta_workflow_run(
                        item_identifier,
                        meta_workflow_identifier,
                        auth_key,
                        post=post,
                        patch=patch,
                    )
                mock_get_metadata.assert_called_once_with(
                    item_identifier, key=auth_key, add_on="frame=object"
                )
                if from_sample_expected:
                    mock_create_from_sample.assert_called_once_with(
                        item_identifier,
                        meta_workflow_identifier,
                        auth_key,
                        post=post,
                        patch=patch,
                    )
                else:
                    mock_create_from_sample.assert_not_called()
                if from_sample_processing_expected:
                    mock_create_from_sample_processing.assert_called_once_with(
                        item_identifier,
                        meta_workflow_identifier,
                        auth_key,
                        post=post,
                        patch=patch,
                    )
                else:
                    mock_create_from_sample_processing.assert_not_called()


@pytest.mark.parametrize(
    "item_type,item,expected",
    [
        ("foo", {}, False),
        ("foo", {"@type": ["foo", "bar"]}, True),
        ("fu", {"@type": ["foo", "bar"]}, False),
    ],
)
def test__is_item_of_type(item_type: str, item: JsonObject, expected: bool) -> None:
    result = _is_item_of_type(item_type, item)
    assert result == expected


@pytest.mark.parametrize(
    "item,expected",
    [
        ({}, []),
        ({"@type": ["foo", "bar"]}, ["foo", "bar"]),
    ],
)
def test__get_item_types(item: JsonObject, expected: List[str]) -> None:
    result = _get_item_types(item)
    assert result == expected


class InputPropertiesForTest:
    """A simple input properties class for tests."""

    def __init__(self):
        attributes_to_set = [
            (INPUT_BAMS, INPUT_PROPERTIES_INPUT_BAMS),
            (FAMILY_SIZE, INPUT_PROPERTIES_FAMILY_SIZE),
        ]
        for attribute_name, attribute_value in attributes_to_set:
            setattr(self, attribute_name, attribute_value)


class TestMetaWorkflowRunInput:
    @pytest.mark.parametrize(
        "parameter_value,expected",
        [
            (1, "1"),
            (1.001, "1.001"),
            ("string", "string"),
            (True, "True"),
            ([1, "string"], json.dumps([1, "string"])),
            ({1: "string"}, json.dumps({1: "string"})),
        ],
    )
    def test_cast_parameter_value(
        self, parameter_value, expected, meta_workflow_run_input
    ):
        """Test formatting of value parameter."""
        result = meta_workflow_run_input.cast_parameter_value(parameter_value)
        assert result == expected

    @pytest.mark.parametrize(
        "parameters_to_fetch,error,expected",
        [
            ([("foo", "bar")], True, None),
            (
                [("family_size", "integer")],
                False,
                [
                    {
                        "argument_name": "family_size",
                        "argument_type": "parameter",
                        "value": "2",
                        "value_type": "integer",
                    },
                ],
            ),
        ],
    )
    def test_fetch_parameters(
        self, parameters_to_fetch, error, expected, meta_workflow_run_input
    ):
        """Test retrieval and formatting of value parameters."""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_input.fetch_parameters(parameters_to_fetch)
        else:
            result = meta_workflow_run_input.fetch_parameters(parameters_to_fetch)
            assert result == expected

    @pytest.mark.parametrize(
        "file_parameter,file_value,input_dimensions,error,expected",
        [
            ("foo", [], 1, False, []),
            (
                "input_files",
                [["file_1", "file_2"]],
                1,
                False,
                [
                    {"file": "file_1", "dimension": "0"},
                    {"file": "file_2", "dimension": "1"},
                ],
            ),
            (
                "input_files",
                [["file_1"], ["file_2"]],
                1,
                True,
                [],
            ),
            (
                "input_files",
                [["file_1"], ["file_2"]],
                2,
                False,
                [
                    {"file": "file_1", "dimension": "0,0"},
                    {"file": "file_2", "dimension": "1,0"},
                ],
            ),
            (
                "input_files",
                [["file_1"], ["file_2", "file_3"]],
                2,
                False,
                [
                    {"file": "file_1", "dimension": "0,0"},
                    {"file": "file_2", "dimension": "1,0"},
                    {"file": "file_3", "dimension": "1,1"},
                ],
            ),
            ("input_files", [["file_1"], ["file_2", "file_3"]], 1, True, None),
        ],
    )
    def test_format_file_input_value(
        self,
        file_parameter,
        file_value,
        input_dimensions,
        error,
        expected,
        meta_workflow_run_input,
    ):
        """Test formatting of file parameter values."""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_input.format_file_input_value(
                    file_parameter, file_value, input_dimensions
                )
        else:
            result = meta_workflow_run_input.format_file_input_value(
                file_parameter, file_value, input_dimensions
            )
            assert result == expected

    @pytest.mark.parametrize(
        "files_to_fetch,error,expected",
        [
            ([("input_foo_files", 1)], True, None),
            (
                [("input_bams", 2)],
                False,
                [
                    {
                        "argument_name": "input_bams",
                        "argument_type": "file",
                        "files": [
                            {"file": "bam_sample_1", "dimension": "0,0"},
                            {"file": "bam_sample_2", "dimension": "1,0"},
                        ],
                    },
                ],
            ),
        ],
    )
    def test_fetch_files(
        self, files_to_fetch, error, expected, meta_workflow_run_input
    ):
        """Test retrieval and formatting of file parameter values."""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_input.fetch_files(files_to_fetch)
        else:
            result = meta_workflow_run_input.fetch_files(files_to_fetch)
            assert result == expected

    @pytest.mark.parametrize(
        "meta_workflow,error,expected",
        [
            ({}, False, []),
            (
                {
                    "input": [
                        {
                            "argument_name": "some_default_input",
                            "argument_type": "file",
                            "files": ["default_file"],
                        },
                    ]
                },
                False,
                [],
            ),
            (
                {
                    "input": [
                        {
                            "argument_name": "some_default_parameter",
                            "argument_type": "parameter",
                            "value": "5",
                            "value_type": "integer",
                        },
                    ]
                },
                False,
                [],
            ),
            (
                {
                    "input": [
                        {
                            "argument_name": "nonexistent_case_input",
                            "argument_type": "parameter",
                            "value_type": "string",
                        },
                    ]
                },
                True,
                None,
            ),
            (
                {
                    "input": [
                        {
                            "argument_name": "input_bams",
                            "argument_type": "file",
                            "dimensionality": 2,
                        },
                    ]
                },
                False,
                [
                    {
                        "argument_name": "input_bams",
                        "argument_type": "file",
                        "files": [
                            {"file": "bam_sample_1", "dimension": "0,0"},
                            {"file": "bam_sample_2", "dimension": "1,0"},
                        ],
                    },
                ],
            ),
            (
                {
                    "input": [
                        {
                            "argument_name": "family_size",
                            "argument_type": "parameter",
                            "value_type": "integer",
                        },
                    ]
                },
                False,
                [
                    {
                        "argument_name": "family_size",
                        "argument_type": "parameter",
                        "value_type": "integer",
                        "value": "2",
                    },
                ],
            ),
        ],
    )
    def test_create_input(
        self, meta_workflow, error, expected, meta_workflow_run_input
    ):
        """Test creation of MetaWorkflowRun input parameters based on
        given MetaWorkflow.
        """
        meta_workflow_run_input.meta_workflow = meta_workflow
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_input.create_input()
        else:
            result = meta_workflow_run_input.create_input()
            assert result == expected


@pytest.mark.parametrize(
    "file_items,file_formats,requirements,expected",
    [
        ([], [], None, []),
        (SAMPLE_PROCESSING_SUBMITTED_FILES, [], None, []),
        (SAMPLE_PROCESSING_SUBMITTED_FILES, ["vcf_gz"], None, [SNV_VCF_UUID]),
        (
            SAMPLE_PROCESSING_SUBMITTED_FILES,
            ["bar", "vcf"],
            None,
            [SV_VCF_UUID, SOME_FILE_UUID],
        ),
        (SAMPLE_PROCESSING_SUBMITTED_FILES, ["vcf_gz"], {"foo": ["value"]}, []),
        (
            SAMPLE_PROCESSING_SUBMITTED_FILES,
            ["vcf_gz"],
            {"foo": ["bar"]},
            [SNV_VCF_UUID],
        ),
    ],
)
def test_get_files_for_file_formats(file_items, file_formats, requirements, expected):
    """Test gather of file UUIDs based on file formats and property
    requirements.
    """
    result = get_files_for_file_formats(
        file_items, file_formats, requirements=requirements
    )
    assert result == expected


class TestInputPropertiesFromSampleProcessing:
    @pytest.mark.parametrize(
        "items_to_sort,sample_name_key,proband,mother,father,expected_order",
        [
            ([], "foo", "bar", "fu", "bur", []),
            ([{"foo": "bar"}], "foo", "bar", "fu", "bur", [0]),
            ([{"fool": "bar"}], "foo", "bar", "fu", "bur", [0]),
            ([{"foo": "bar"}, {"foo": "par"}], "foo", "bar", "fu", "bur", [0, 1]),
            ([{"foo": "par"}, {"foo": "bar"}], "foo", "bar", "fu", "bur", [1, 0]),
            (
                [{"foo": "par"}, {"foo": "fu"}, {"foo": "bar"}],
                "foo",
                "bar",
                "fu",
                "bur",
                [2, 1, 0],
            ),
            (
                [{"foo": "par"}, {"foo": "fu"}, {"foo": "bar"}, {"foo": "bur"}],
                "foo",
                "bar",
                "fu",
                "bur",
                [2, 1, 3, 0],
            ),
            (
                deepcopy(SAMPLES),
                "bam_sample_id",
                "SAMPLE3-DNA-WGS",
                "SAMPLE4-DNA-WGS",
                "SAMPLE2-DNA-WGS",
                [2, 3, 1, 0],
            ),
            (
                deepcopy(SAMPLES_PEDIGREE),
                "sample_name",
                "SAMPLE3-DNA-WGS",
                "SAMPLE4-DNA-WGS",
                "SAMPLE2-DNA-WGS",
                [2, 3, 1, 0],
            ),
        ],
    )
    def test_sort_by_sample_names(
        self,
        items_to_sort,
        sample_name_key,
        proband,
        mother,
        father,
        expected_order,
        inputs_from_sample_processing,
    ):
        """Test sorting of items by sample names to give expected order
        of proband, mother, father, and then other family members.
        """
        expected = []
        for idx in expected_order:
            expected.append(items_to_sort[idx])
        inputs_from_sample_processing.sort_by_sample_name(
            items_to_sort, sample_name_key, proband, mother=mother, father=father
        )
        assert items_to_sort == expected

    @pytest.mark.parametrize(
        (
            "expect_family_structure,proband_only,samples,samples_pedigree,error"
            ",expected_samples,expected_samples_pedigree"
        ),
        [
            (True, False, [], [], True, [], []),
            (True, True, [], [], True, [], []),
            (True, False, SAMPLES, [], True, SORTED_SAMPLES, []),
            (True, False, [], SAMPLES_PEDIGREE, True, [], SORTED_SAMPLES_PEDIGREE),
            (
                True,
                False,
                SAMPLES,
                SAMPLES_PEDIGREE,
                False,
                SORTED_SAMPLES,
                SORTED_SAMPLES_PEDIGREE,
            ),
            (
                True,
                True,
                SAMPLES,
                SAMPLES_PEDIGREE,
                False,
                [SORTED_SAMPLES[0]],
                [SORTED_SAMPLES_PEDIGREE[0]],
            ),
            (False, False, [], [], True, [], []),
            (False, False, SAMPLES, [], False, SAMPLES, []),
            (False, False, [], SAMPLES_PEDIGREE, True, [], SAMPLES_PEDIGREE),
            (False, False, SAMPLES, SAMPLES_PEDIGREE, False, SAMPLES, SAMPLES_PEDIGREE),
        ],
    )
    def test_clean_and_sort_samples_and_pedigree(
        self,
        expect_family_structure,
        proband_only,
        samples,
        samples_pedigree,
        error,
        expected_samples,
        expected_samples_pedigree,
    ):
        """Test sorting of samples and pedigree or lack thereof if
        samples are not connected as family (i.e. a cohort).
        """
        test_samples = deepcopy(samples)
        test_pedigree = deepcopy(samples_pedigree)
        sample_processing = {"samples": test_samples, "samples_pedigree": test_pedigree}
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing = InputPropertiesFromSampleProcessing(
                    sample_processing,
                    expect_family_structure=expect_family_structure,
                    proband_only=proband_only,
                )
        else:
            inputs_from_sample_processing = InputPropertiesFromSampleProcessing(
                sample_processing,
                expect_family_structure=expect_family_structure,
                proband_only=proband_only,
            )
            sorted_samples = inputs_from_sample_processing.samples
            sorted_pedigree = inputs_from_sample_processing.samples_pedigree
            assert sorted_samples == expected_samples
            assert sorted_pedigree == expected_samples_pedigree

    @pytest.mark.parametrize("proband_only", [True, False])
    def test_remove_non_proband_samples(
        self, proband_only, inputs_from_sample_processing
    ):
        """Test removal of non-proband samples from input samples and
        samples pedigree.

        Result should be identical regardless of proband-only property
        on MWF.
        """
        samples = inputs_from_sample_processing.samples
        samples_pedigree = inputs_from_sample_processing.samples_pedigree
        inputs_from_sample_processing.proband_only = proband_only
        inputs_from_sample_processing.remove_non_proband_samples()
        assert samples == SORTED_SAMPLES[:1]
        assert samples_pedigree == SORTED_SAMPLES_PEDIGREE[:1]

    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("family_size", SAMPLE_PROCESSING_FAMILY_SIZE),
            ("sample_name_proband", SAMPLE_NAME_PROBAND),
            ("bamsnap_titles", BAMSNAP_TITLES),
            ("pedigree", PEDIGREE),
            ("probands", [SAMPLE_NAME_3]),
            ("sample_names", SORTED_SAMPLE_NAMES),
            ("input_sample_uuids", SORTED_SAMPLE_UUIDS),
            ("input_crams", INPUT_CRAMS),
            ("fastqs_r1", FASTQ_R1_UUIDS),
            ("fastqs_r2", FASTQ_R2_UUIDS),
            ("input_bams", BAM_UUIDS),
            ("input_gvcfs", GVCF_UUIDS),
            ("input_vcfs", [VCF_UUIDS]),
            ("input_snv_vcfs", [[SNV_VCF_UUID]]),
            ("input_sv_vcfs", [[SV_VCF_UUID]]),
            ("rcktar_file_names", RCKTAR_FILE_NAMES),
        ],
    )
    def test_attributes(self, attribute, expected, inputs_from_sample_processing):
        """Test property values set correctly on class."""
        result = getattr(inputs_from_sample_processing, attribute)
        assert result == expected

    @pytest.mark.parametrize(
        "requirements,return_value",
        [
            (None, []),
            ({"foo": ["bur"]}, []),
            ({"foo": ["bur"]}, VCF_UUIDS),
        ],
    )
    def test_get_submitted_vcf_files(
        self, requirements, return_value, inputs_from_sample_processing
    ):
        """Test collection of VCFs submitted to SampleProcessing."""
        with mock.patch.object(
            create_mwfr_module,
            "get_files_for_file_formats",
            return_value=return_value,
        ) as mocked_get_files_for_file_formats:
            if not return_value:
                with pytest.raises(MetaWorkflowRunCreationError):
                    inputs_from_sample_processing.get_submitted_vcf_files(requirements)
            else:
                result = inputs_from_sample_processing.get_submitted_vcf_files(
                    requirements
                )
                assert result == [return_value]
            mocked_get_files_for_file_formats.assert_called_once_with(
                SAMPLE_PROCESSING_SUBMITTED_FILES,
                InputPropertiesFromSampleProcessing.VCF_FORMATS,
                requirements=requirements,
            )


class TestInputPropertiesFromSample:
    @pytest.mark.parametrize(
        "file_format,requirements,get_files_result,error",
        [
            ("foo", None, [], True),
            ("foo", {"fu": ["bur"]}, [], True),
            ("foo", None, ["some_uuid"], False),
        ],
    )
    def test_get_processed_files_for_file_format(
        self,
        file_format,
        requirements,
        get_files_result,
        error,
    ):
        """Test retrieval of files of given file format meeting
        requirements from given Sample.processed_files.
        """
        input_properties = InputPropertiesFromSample(SAMPLE_1)
        processed_files = input_properties.sample.get("processed_files")
        with mock.patch.object(
            create_mwfr_module,
            "get_files_for_file_formats",
            return_value=get_files_result,
        ) as mock_get_files_for_file_formats:
            if error:
                with pytest.raises(MetaWorkflowRunCreationError):
                    input_properties.get_processed_files_for_file_format(
                        file_format, requirements
                    )
            else:
                result = input_properties.get_processed_files_for_file_format(
                    file_format, requirements
                )
                assert result == get_files_result
            mock_get_files_for_file_formats.assert_called_once_with(
                processed_files, [file_format], requirements=requirements
            )

    @pytest.mark.parametrize(
        "file_format,requirements,get_files_result,error",
        [
            ("foo", None, [], True),
            ("foo", {"fu": ["bur"]}, [], True),
            ("foo", None, ["some_uuid"], False),
        ],
    )
    def test_get_submitted_files_for_file_format(
        self,
        file_format,
        requirements,
        get_files_result,
        error,
    ):
        """Test retrieval of files of given file format meeting
        requirements from given Sample.files.
        """
        input_properties = InputPropertiesFromSample(SAMPLE_1)
        submitted_files = input_properties.sample.get("files")
        with mock.patch.object(
            create_mwfr_module,
            "get_files_for_file_formats",
            return_value=get_files_result,
        ) as mock_get_files_for_file_formats:
            if error:
                with pytest.raises(MetaWorkflowRunCreationError):
                    input_properties.get_submitted_files_for_file_format(
                        file_format, requirements
                    )
            else:
                result = input_properties.get_submitted_files_for_file_format(
                    file_format, requirements
                )
                assert result == get_files_result
            mock_get_files_for_file_formats.assert_called_once_with(
                submitted_files, [file_format], requirements=requirements
            )

    @pytest.mark.parametrize(
        "sample,paired_end,error,expected",
        [
            (SAMPLE_1, "0", True, None),
            (SAMPLE_1, "1", False, [FASTQ_R1_UUID_1, FASTQ_R1_UUID_1_2]),
            (SAMPLE_1, "2", False, [FASTQ_R2_UUID_1, FASTQ_R2_UUID_1_2]),
            (SAMPLE_2, "1", False, [FASTQ_R1_UUID_2]),
            (SAMPLE_2, "2", False, [FASTQ_R2_UUID_2]),
        ],
    )
    def test_get_fastqs_for_paired_end(self, sample, paired_end, error, expected):
        """Test retrieval of FASTQ files of given paired end from
        Samples on SampleProcessing.
        """
        input_properties = InputPropertiesFromSample(sample)
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                input_properties.get_fastqs_for_paired_end(paired_end)
        else:
            result = input_properties.get_fastqs_for_paired_end(paired_end)
            assert result == expected

    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("input_crams", SAMPLE_3_CRAMS),
            ("input_gvcfs", SAMPLE_3_GVCFS),
            ("fastqs_r1", SAMPLE_3_FASTQS_R1),
            ("fastqs_r2", SAMPLE_3_FASTQS_R2),
            ("fastqs", SAMPLE_3_FASTQS),
            ("input_bams", SAMPLE_3_BAMS),
            ("rcktar_file_names", SAMPLE_3_RCKTARS),
            ("sample_names", SAMPLE_3_NAMES),
            ("input_sample_uuids", SAMPLE_3_UUIDS),
        ],
    )
    def test_attributes(self, attribute, expected, inputs_from_sample):
        """Test properties on class."""
        result = getattr(inputs_from_sample, attribute)
        assert result == expected


class TestMetaWorkflowRunFromItem:
    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("project", PROJECT),
            ("institution", INSTITUTION),
            ("existing_meta_workflow_runs", []),
            ("input_item", ARBITRARY_ITEM),
            ("input_item_uuid", ITEM_UUID),
            ("auth_key", AUTH_KEY),
            ("meta_workflow", META_WORKFLOW),
            ("meta_workflow_run_uuid", META_WORKFLOW_RUN_UUID),
            ("proband_only", PROBAND_ONLY_FALSE),
        ],
    )
    def test_attributes(self, attribute, expected, meta_workflow_run_from_item):
        """Test attributes set correctly."""
        result = getattr(meta_workflow_run_from_item, attribute)
        assert result == expected

    @pytest.mark.parametrize(
        "meta_workflow_run,error,expected",
        [
            (META_WORKFLOW_RUN_NO_FILES_INPUT, True, None),
            (META_WORKFLOW_RUN_NO_WORKFLOW_RUNS, False, META_WORKFLOW_RUN),
        ],
    )
    def test_create_workflow_runs(
        self,
        meta_workflow_run,
        error,
        expected,
        meta_workflow_run_from_item,
    ):
        """Test creation of workflow runs from given MetaWorkflowRun
        properties.
        """
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_from_item.create_workflow_runs(meta_workflow_run)
        else:
            meta_workflow_run_from_item.create_workflow_runs(meta_workflow_run)
            assert meta_workflow_run == expected

    @pytest.mark.parametrize(
        "return_value,exception,expected",
        [
            ({"foo": "bar"}, True, None),
            ({"foo": "bar"}, False, {"foo": "bar"}),
        ],
    )
    def test_get_item_properties(
        self, meta_workflow_run_from_item, return_value, exception, expected
    ):
        """Test item GET from portal."""
        side_effect = None
        if exception:
            side_effect = Exception
        with mock.patch(
            "magma_ff.create_metawfr.ff_utils.get_metadata",
            return_value=return_value,
            side_effect=side_effect,
        ) as mock_get_metadata:
            result = meta_workflow_run_from_item.get_item_properties("foo")
            assert result == expected
            mock_get_metadata.assert_called_once_with(
                "foo", key=AUTH_KEY, add_on="frame=raw"
            )

    @pytest.mark.parametrize("exception", [True, False])
    def test_post_meta_workflow_item(self, meta_workflow_run_from_item, exception):
        """Test MWFR POST to portal."""
        side_effect = None
        if exception:
            side_effect = Exception
        with mock.patch(
            "magma_ff.create_metawfr.ff_utils.post_metadata",
            side_effect=side_effect,
        ) as mock_post_metadata:
            if exception:
                with pytest.raises(MetaWorkflowRunCreationError):
                    meta_workflow_run_from_item.post_meta_workflow_run()
            else:
                meta_workflow_run_from_item.post_meta_workflow_run()
            mock_post_metadata.assert_called_once_with(
                {}, MetaWorkflowRunFromItem.META_WORKFLOW_RUN_ENDPOINT, key=AUTH_KEY
            )

    @pytest.mark.parametrize(
        "existing_meta_workflow_runs,exception,expected_meta_workflow_runs",
        [
            ([], True, [META_WORKFLOW_RUN_UUID]),
            (["foo", "bar"], False, ["foo", "bar", META_WORKFLOW_RUN_UUID]),
        ],
    )
    def test_patch_input_item(
        self,
        meta_workflow_run_from_item,
        existing_meta_workflow_runs,
        exception,
        expected_meta_workflow_runs,
    ):
        """Test PATCH input item on portal."""
        setattr(
            meta_workflow_run_from_item,
            "existing_meta_workflow_runs",
            existing_meta_workflow_runs,
        )
        side_effect = None
        if exception:
            side_effect = Exception
        with mock.patch(
            "magma_ff.create_metawfr.ff_utils.patch_metadata",
            side_effect=side_effect,
        ) as mock_patch_metadata:
            if exception:
                with pytest.raises(MetaWorkflowRunCreationError):
                    meta_workflow_run_from_item.patch_input_item()
            else:
                meta_workflow_run_from_item.patch_input_item()
            mock_patch_metadata.assert_called_once_with(
                {"meta_workflow_runs": expected_meta_workflow_runs},
                obj_id=ITEM_UUID,
                key=AUTH_KEY,
            )


class TestMetaWorkflowRunFromSampleProcessing:
    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("project", PROJECT),
            ("institution", INSTITUTION),
            ("input_item", SAMPLE_PROCESSING),
            ("input_item_uuid", SAMPLE_PROCESSING_UUID),
            ("auth_key", AUTH_KEY),
            ("meta_workflow", META_WORKFLOW),
            ("existing_meta_workflow_runs", SAMPLE_PROCESSING_META_WORKFLOW_RUNS),
            ("meta_workflow_run", META_WORKFLOW_RUN),
        ],
    )
    def test_attributes(
        self, attribute, expected, meta_workflow_run_from_sample_processing
    ):
        """Test attributes set correctly."""
        result = getattr(meta_workflow_run_from_sample_processing, attribute)
        assert result == expected

    def test_create_meta_workflow_run(self, meta_workflow_run_from_sample_processing):
        """Test creation of MetaWorkflowRun properties."""
        result = meta_workflow_run_from_sample_processing.create_meta_workflow_run()
        assert result == META_WORKFLOW_RUN


class TestMetaWorkflowRunFromSample:
    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("project", PROJECT),
            ("institution", INSTITUTION),
            ("input_item", SAMPLE_3),
            ("input_item_uuid", SAMPLE_UUID_3),
            ("auth_key", AUTH_KEY),
            ("meta_workflow", META_WORKFLOW_FOR_SAMPLE),
            ("existing_meta_workflow_runs", []),
            ("meta_workflow_run", META_WORKFLOW_RUN_FOR_SAMPLE_3),
        ],
    )
    def test_attributes(self, attribute, expected, meta_workflow_run_from_sample):
        """Test attributes set correctly."""
        result = getattr(meta_workflow_run_from_sample, attribute)
        assert result == expected

    def test_create_meta_workflow_run(self, meta_workflow_run_from_sample):
        """Test creation of MetaWorkflowRun properties."""
        result = meta_workflow_run_from_sample.create_meta_workflow_run()
        assert result == META_WORKFLOW_RUN_FOR_SAMPLE_3
