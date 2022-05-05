import datetime
import json
from copy import deepcopy

import mock
import pytest

from magma_ff.create_metawfr import (
    InputPropertiesFromSampleProcessing,
    MetaWorkflowRunCreationError,
    MetaWorkflowRunFromSampleProcessing,
    MetaWorkflowRunInput,
)

BAM_UUID_1 = "bam_sample_1"
BAM_UUID_2 = "bam_sample_2"
BAM_UUID_3 = "bam_sample_3"
BAM_UUID_4 = "bam_sample_4"
BAM_UUIDS = [BAM_UUID_1, BAM_UUID_2, BAM_UUID_3, BAM_UUID_4]
GVCF_UUID_1 = "gvcf_sample_1"
GVCF_UUID_1_2 = "gvcf_sample_1_2"
GVCF_UUID_2 = "gvcf_sample_2"
GVCF_UUID_3 = "gvcf_sample_3"
GVCF_UUID_4 = "gvcf_sample_4"
GVCF_UUIDS = [[GVCF_UUID_1, GVCF_UUID_1_2], GVCF_UUID_2, GVCF_UUID_3, GVCF_UUID_4]
CRAM_UUID_1 = "cram_sample_1"
CRAM_UUID_2 = "cram_sample_2"
CRAM_UUID_3 = "cram_sample_3"
CRAM_UUID_4 = "cram_sample_4"
INPUT_CRAMS = {2: [CRAM_UUID_2], 3: [CRAM_UUID_1], 1: [CRAM_UUID_4], 0: [CRAM_UUID_3]}
FASTQ_R1_UUID_1 = "fastq_r1_sample_1"
FASTQ_R1_UUID_1_2 = "fastq_r1_sample_1_2"
FASTQ_R1_UUID_2 = "fastq_r1_sample_2"
FASTQ_R1_UUID_3 = "fastq_r1_sample_3"
FASTQ_R1_UUID_4 = "fastq_r1_sample_4"
FASTQ_R1_UUIDS = [
    [FASTQ_R1_UUID_1, FASTQ_R1_UUID_1_2],
    FASTQ_R1_UUID_2,
    FASTQ_R1_UUID_3,
    FASTQ_R1_UUID_4,
]
FASTQ_R2_UUID_1 = "fastq_r2_sample_1"
FASTQ_R2_UUID_2 = "fastq_r2_sample_2"
FASTQ_R2_UUID_3 = "fastq_r2_sample_3"
FASTQ_R2_UUID_4 = "fastq_r2_sample_4"
FASTQ_R2_UUIDS = [FASTQ_R2_UUID_1, FASTQ_R2_UUID_2, FASTQ_R2_UUID_3, FASTQ_R2_UUID_4]
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
SAMPLE_1 = {
    "uuid": SAMPLE_UUID_1,
    "bam_sample_id": SAMPLE_NAME_1,
    "files": [],
    "cram_files": [{"uuid": CRAM_UUID_1}],
    "processed_files": [
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
    ],
}
SAMPLE_2 = {
    "uuid": SAMPLE_UUID_2,
    "bam_sample_id": SAMPLE_NAME_2,
    "files": [
        {
            "uuid": FASTQ_R1_UUID_2,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
            "related_files": [{"file": FASTQ_R2_UUID_2}],
        },
        {
            "uuid": FASTQ_R2_UUID_2,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
            "related_files": [{"file": FASTQ_R1_UUID_2}],
        },
    ],
    "cram_files": [{"uuid": CRAM_UUID_2}],
    "processed_files": [
        {"uuid": BAM_UUID_2, "file_format": {"file_format": "bam"}},
        {"uuid": GVCF_UUID_2, "file_format": {"file_format": "gvcf_gz"}},
    ],
}
SAMPLE_3 = {
    "uuid": SAMPLE_UUID_3,
    "bam_sample_id": SAMPLE_NAME_3,
    "files": [
        {
            "uuid": FASTQ_R1_UUID_3,
            "paired_end": "1",
            "file_format": {"file_format": "fastq"},
            "related_files": [{"file": FASTQ_R2_UUID_3}],
        },
        {
            "uuid": FASTQ_R2_UUID_3,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
            "related_files": [{"file": FASTQ_R1_UUID_3}],
        },
    ],
    "cram_files": [{"uuid": CRAM_UUID_3}],
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
            "related_files": [{"file": FASTQ_R2_UUID_4}],
        },
        {
            "uuid": FASTQ_R2_UUID_4,
            "paired_end": "2",
            "file_format": {"file_format": "fastq"},
            "related_files": [{"file": FASTQ_R1_UUID_4}],
        },
    ],
    "cram_files": [{"uuid": CRAM_UUID_4}],
    "processed_files": [
        {"uuid": BAM_UUID_4, "file_format": {"file_format": "bam"}},
        {"uuid": GVCF_UUID_4, "file_format": {"file_format": "gvcf_gz"}},
    ],
}
SAMPLES = [SAMPLE_1, SAMPLE_2, SAMPLE_3, SAMPLE_4]
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
SAMPLE_PROCESSING_UUID = "some_uuid"
SAMPLE_PROCESSING_META_WORKFLOW_RUNS = [{"uuid": "run_1"}, {"uuid": "run_2"}]
SAMPLE_PROCESSING_PROJECT = "/projects/cgap-core/"
SAMPLE_PROCESSING_INSTITUTION = "/institutions/hms-dbmi/"
SAMPLE_PROCESSING = {
    "uuid": SAMPLE_PROCESSING_UUID,
    "project": SAMPLE_PROCESSING_PROJECT,
    "institution": SAMPLE_PROCESSING_INSTITUTION,
    "samples_pedigree": SAMPLES_PEDIGREE,
    "samples": SAMPLES,
    "meta_workflow_runs": SAMPLE_PROCESSING_META_WORKFLOW_RUNS,
}
INPUT_BAMS = "input_bams"
FAMILY_SIZE = "family_size"
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
}
INPUT_PROPERTIES_INPUT_BAMS = {1: [BAM_UUID_2], 0: [BAM_UUID_1]}
INPUT_PROPERTIES_FAMILY_SIZE = 2
TODAY = datetime.date.today().isoformat()
META_WORKFLOW_RUN_UUID = "another_uuid"
COMMON_FIELDS = {
    "project": SAMPLE_PROCESSING_PROJECT,
    "institution": SAMPLE_PROCESSING_INSTITUTION,
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
    "project": SAMPLE_PROCESSING_PROJECT,
    "institution": SAMPLE_PROCESSING_INSTITUTION,
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
    "project": SAMPLE_PROCESSING_PROJECT,
    "institution": SAMPLE_PROCESSING_INSTITUTION,
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
    "project": SAMPLE_PROCESSING_PROJECT,
    "institution": SAMPLE_PROCESSING_INSTITUTION,
    "common_fields": COMMON_FIELDS,
    "input_samples": INPUT_SAMPLES,
    "associated_sample_processing": SAMPLE_PROCESSING_UUID,
    "final_status": "pending",
    "workflow_runs": [],
    "uuid": META_WORKFLOW_RUN_UUID,
}


class InputPropertiesForTest:
    """A simple input properties class for tests."""

    def __init__(self):
        attributes_to_set = [
            (INPUT_BAMS, INPUT_PROPERTIES_INPUT_BAMS),
            (FAMILY_SIZE, INPUT_PROPERTIES_FAMILY_SIZE),
        ]
        for attribute_name, attribute_value in attributes_to_set:
            setattr(self, attribute_name, attribute_value)


@pytest.fixture
def meta_workflow_run_input():
    """MetaWorkflowRunInput class for tests."""
    return MetaWorkflowRunInput(META_WORKFLOW, InputPropertiesForTest())


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
            ("foo", {}, 1, False, []),
            (
                "input_files",
                {1: ["file_2"], 0: ["file_1"]},
                1,
                False,
                [
                    {"file": "file_1", "dimension": "0"},
                    {"file": "file_2", "dimension": "1"},
                ],
            ),
            (
                "input_files",
                {1: ["file_2"], 0: ["file_1"]},
                2,
                False,
                [
                    {"file": "file_1", "dimension": "0,0"},
                    {"file": "file_2", "dimension": "1,0"},
                ],
            ),
            (
                "input_files",
                {1: ["file_2", "file_3"], 0: ["file_1"]},
                2,
                False,
                [
                    {"file": "file_1", "dimension": "0,0"},
                    {"file": "file_2", "dimension": "1,0"},
                    {"file": "file_3", "dimension": "1,1"},
                ],
            ),
            ("input_files", {1: ["file_2", "file_3"], 0: ["file_1"]}, 1, True, None),
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


@pytest.fixture
def inputs_from_sample_processing():
    """Class for tests."""
    return InputPropertiesFromSampleProcessing(SAMPLE_PROCESSING)


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

    def sort_and_format_uuids(self, uuids):
        """Reformat given uuids to match SAMPLES order."""
        result = {}
        for idx, sorted_idx in enumerate(SORTED_INDICES):
            file_uuids = uuids[sorted_idx]
            if isinstance(file_uuids, str):
                file_uuids = [file_uuids]
            result[idx] = file_uuids
        return result

    @pytest.mark.parametrize(
        "file_format,error,expected_uuids",
        [
            ("foo", True, None),
            ("fastq", True, None),
            ("cram", True, None),
            ("bam", False, BAM_UUIDS),
            ("gvcf_gz", False, GVCF_UUIDS),
        ],
    )
    def test_get_samples_processed_file_for_format(
        self, file_format, error, expected_uuids, inputs_from_sample_processing
    ):
        """Test retrieval of files of given file format from
        Samples.processed_files on SampleProcessing.
        """
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing.get_samples_processed_file_for_format(
                    file_format
                )
        else:
            expected = self.sort_and_format_uuids(expected_uuids)
            result = (
                inputs_from_sample_processing.get_samples_processed_file_for_format(
                    file_format
                )
            )
            assert result == expected

    @pytest.mark.parametrize(
        "sample,file_format,requirements,error,expected",
        [
            ({}, "", None, True, None),
            (SAMPLE_1, "foo", None, True, None),
            (SAMPLE_1, "bam", None, False, [BAM_UUID_1]),
            (SAMPLE_1, "bam", {"foo": ["bar"]}, True, None),
            (
                SAMPLE_1,
                "fastq",
                None,
                False,
                [FASTQ_R1_UUID_1, FASTQ_R1_UUID_1_2, FASTQ_R2_UUID_1],
            ),
            (
                SAMPLE_1,
                "fastq",
                {"paired_end": ["1"]},
                False,
                [
                    FASTQ_R1_UUID_1,
                    FASTQ_R1_UUID_1_2,
                ],
            ),
            (SAMPLE_1, "fastq", {"paired_end": ["2"]}, False, [FASTQ_R2_UUID_1]),
        ],
    )
    def test_get_processed_file_for_format(
        self,
        sample,
        file_format,
        requirements,
        error,
        expected,
        inputs_from_sample_processing,
    ):
        """Test retrieval of files of given file format meeting
        requirements from given Sample.processed_files.
        """
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing.get_processed_file_for_format(
                    sample, file_format, requirements
                )
        else:
            result = inputs_from_sample_processing.get_processed_file_for_format(
                sample, file_format, requirements
            )
            assert result == expected

    @pytest.mark.parametrize(
        "paired_end,error,expected_uuids",
        [
            ("0", True, None),
            ("1", False, FASTQ_R1_UUIDS),
            ("2", False, FASTQ_R2_UUIDS),
        ],
    )
    def test_get_fastqs_for_paired_end(
        self, paired_end, error, expected_uuids, inputs_from_sample_processing
    ):
        """Test retrieval of FASTQ files of given paired end from
        Samples on SampleProcessing.
        """
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing.get_fastqs_for_paired_end(paired_end)
        else:
            expected = self.sort_and_format_uuids(expected_uuids)
            result = inputs_from_sample_processing.get_fastqs_for_paired_end(paired_end)
            assert result == expected

    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("family_size", SAMPLE_PROCESSING_FAMILY_SIZE),
            ("sample_name_proband", SAMPLE_NAME_PROBAND),
            ("bamsnap_titles", BAMSNAP_TITLES),
            ("pedigree", PEDIGREE),
            ("sample_names", SORTED_SAMPLE_NAMES),
            ("input_sample_uuids", SORTED_SAMPLE_UUIDS),
            ("input_crams", INPUT_CRAMS),
        ],
    )
    def test_attributes(self, attribute, expected, inputs_from_sample_processing):
        """Test property values set correctly on class."""
        result = getattr(inputs_from_sample_processing, attribute)
        assert result == expected


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
                return MetaWorkflowRunFromSampleProcessing(None, None, "auth_key")


class TestMetaWorkflowRunFromSampleProcessing:
    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("project", SAMPLE_PROCESSING_PROJECT),
            ("institution", SAMPLE_PROCESSING_INSTITUTION),
            ("existing_meta_workflow_runs", SAMPLE_PROCESSING_META_WORKFLOW_RUNS),
            ("sample_processing_uuid", SAMPLE_PROCESSING_UUID),
            ("auth_key", "auth_key"),
            ("meta_workflow", META_WORKFLOW),
            ("existing_meta_workflow_runs", SAMPLE_PROCESSING_META_WORKFLOW_RUNS),
        ],
    )
    def test_attributes(
        self, attribute, expected, meta_workflow_run_from_sample_processing
    ):
        """Test attributes set correctly."""
        result = getattr(meta_workflow_run_from_sample_processing, attribute)
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
        meta_workflow_run_from_sample_processing,
    ):
        """Test creation of workflow runs from given MetaWorkflowRun
        properties.
        """
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_from_sample_processing.create_workflow_runs(
                    meta_workflow_run
                )
        else:
            meta_workflow_run_from_sample_processing.create_workflow_runs(
                meta_workflow_run
            )
            assert meta_workflow_run == expected

    def test_create_meta_workflow_run(self, meta_workflow_run_from_sample_processing):
        """Test creation of MetaWorkflowRun properties."""
        result = meta_workflow_run_from_sample_processing.create_meta_workflow_run()
        assert result == META_WORKFLOW_RUN
