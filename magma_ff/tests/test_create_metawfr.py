import json
import mock
import pytest

from ..create_metawfr import (
    MetaWorkflowRunCreationError,
    MetaWorkflowRunFromItem,
    MetaWorkflowRunFromCase,
    InputPropertiesFromSampleProcessing,
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
CRAM_UUIDS = [CRAM_UUID_1, CRAM_UUID_2, CRAM_UUID_3, CRAM_UUID_4]
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
SAMPLE_NAMES = [SAMPLE_NAME_1, SAMPLE_NAME_2, SAMPLE_NAME_3, SAMPLE_NAME_4]
SORTED_SAMPLE_NAMES = [SAMPLE_NAME_3, SAMPLE_NAME_4, SAMPLE_NAME_2, SAMPLE_NAME_1]
SAMPLE_1 = {
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
SAMPLE_NAME_PROBAND = "SAMPLE3-DNA-WGS"
BAMSNAP_TITLES = [
    "SAMPLE3-DNA-WGS (proband)",
    "SAMPLE4-DNA-WGS (mother)",
    "SAMPLE2-DNA-WGS (father)",
    "SAMPLE1-DNA-WGS (daughter)",
]
SORTED_INDICES = [2, 3, 1, 0]
SORTED_SAMPLES_PEDIGREE = [PEDIGREE_3, CLEANED_PEDIGREE_4, PEDIGREE_2, PEDIGREE_1]
SORTED_SAMPLES = [SAMPLE_3, SAMPLE_4, SAMPLE_2, SAMPLE_1]
SAMPLE_PROCESSING_UUID = "some_uuid"
SAMPLE_PROCESSING_META_WORKFLOW_RUNS = [{"uuid": "run_1"}, {"uuid": "run_2"}]
SAMPLE_PROCESSING = {
    "uuid": SAMPLE_PROCESSING_UUID,
    "samples_pedigree": SAMPLES_PEDIGREE,
    "samples": SAMPLES,
    "meta_workflow_runs": SAMPLE_PROCESSING_META_WORKFLOW_RUNS,
}
CASE_ACCESSION = "ABCDEF123"
CASE_PROJECT = "/projects/cgap-core/"
CASE_INSTITUTION = "/institutions/hms-dbmi/"
CASE_COMMON_FIELDS = {
    "project": CASE_PROJECT,
    "institution": CASE_INSTITUTION,
    "case_accession": CASE_ACCESSION,
}
CASE = {
    "project": CASE_PROJECT,
    "institution": CASE_INSTITUTION,
    "accession": CASE_ACCESSION,
    "sample_processing": SAMPLE_PROCESSING,
    "nonembedded_field": "foo",
}
INPUT_BAMS = "input_bams"
FAMILY_SIZE = "family_size"
META_WORKFLOW = {
    "uuid": "1234",
    "title": "WGS Family v0.0.0",
    "input": [
        {"argument_name": INPUT_BAMS, "argument_type": "file"},
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
ITEM_TYPE = "AbstractItem"
ITEM_ACCESSION = "GAPITEM"
ITEM_PROJECT = "project_1"
ITEM_INSTITUTION = "institution_1"
ITEM_COMMON_FIELDS = {
    "project": ITEM_PROJECT,
    "institution": ITEM_INSTITUTION,
    "some_field": "some_value",
}
INPUT_PROPERTIES_INPUT_BAMS = {1: [BAM_UUID_2], 0: [BAM_UUID_1]}
INPUT_PROPERTIES_FAMILY_SIZE = 2
META_WORKFLOW_RUN_FOR_ITEM = {
    "meta_workflow": META_WORKFLOW,
    "input": [
        {
            "argument_name": "family_size",
            "argument_type": "parameter",
            "value_type": "integer",
            "value": "2",
        },
        {
            "argument_name": "input_bams",
            "argument_type": "file",
            "files": [
                {"file": "bam_sample_1", "dimension": "0,0"},
                {"file": "bam_sample_2", "dimension": "1,0"},
            ],
        },
    ],
    "title": "MetaWorkflowRun WGS Family v0.0.0 on %s %s" % (ITEM_TYPE, ITEM_ACCESSION),
    "project": ITEM_PROJECT,
    "institution": ITEM_INSTITUTION,
    "common_fields": ITEM_COMMON_FIELDS,
    "final_status": "pending",
    "workflow_runs": [
        {"name": "workflow_do-something", "status": "pending", "shard": "0"},
        {"name": "workflow_do-something", "status": "pending", "shard": "1"},
    ],
    "uuid": "some_uuid",
}
META_WORKFLOW_RUN_FOR_ITEM_NO_WORKFLOW_RUNS = {
    "meta_workflow": META_WORKFLOW,
    "input": [
        {
            "argument_name": "family_size",
            "argument_type": "parameter",
            "value_type": "integer",
            "value": "2",
        },
        {
            "argument_name": "input_bams",
            "argument_type": "file",
            "files": [
                {"file": "bam_sample_1", "dimension": "0,0"},
                {"file": "bam_sample_2", "dimension": "1,0"},
            ],
        },
    ],
    "title": "MetaWorkflowRun WGS Family v0.0.0 on %s %s" % (ITEM_TYPE, ITEM_ACCESSION),
    "project": ITEM_PROJECT,
    "institution": ITEM_INSTITUTION,
    "common_fields": ITEM_COMMON_FIELDS,
    "final_status": "pending",
    "workflow_runs": [],
    "uuid": "some_uuid",
}
META_WORKFLOW_RUN_FOR_ITEM_NO_FILES_INPUT = {
    "meta_workflow": META_WORKFLOW,
    "input": [
        {
            "argument_name": "family_size",
            "argument_type": "parameter",
            "value_type": "integer",
            "value": "2",
        },
    ],
    "title": "MetaWorkflowRun WGS Family v0.0.0 on %s %s" % (ITEM_TYPE, ITEM_ACCESSION),
    "project": ITEM_PROJECT,
    "institution": ITEM_INSTITUTION,
    "common_fields": ITEM_COMMON_FIELDS,
    "final_status": "pending",
    "workflow_runs": [],
    "uuid": "some_uuid",
}


class InputPropertiesForTest:

    def __init__(self, proband_only=False):
        self.proband_only = proband_only
        attributes_to_set = [
            (INPUT_BAMS, INPUT_PROPERTIES_INPUT_BAMS),
            (FAMILY_SIZE, INPUT_PROPERTIES_FAMILY_SIZE),
        ]
        for attribute_name, attribute_value in attributes_to_set:
            setattr(self, attribute_name, attribute_value)

    def is_proband_only(self):
        """"""
        return self.proband_only


class MetaWorkflowRunFromTestItem(MetaWorkflowRunFromItem):

    def __init__(self, proband_only=False):
        self.project = ITEM_PROJECT
        self.institution = ITEM_INSTITUTION
        self.accession = ITEM_ACCESSION
        self.item_type = ITEM_TYPE
        self.common_fields = ITEM_COMMON_FIELDS
        self.auth_key = ""
        self.input_properties = InputPropertiesForTest(proband_only)
        super().__init__("")


@pytest.fixture
def meta_workflow_run_from_item():
    """"""
    with mock.patch(
        "magma_ff.create_metawfr.MetaWorkflowRunFromItem.get_item_properties",
        return_value=META_WORKFLOW,
    ):
        return MetaWorkflowRunFromTestItem()


class TestMetaWorkflowRunFromItem:
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
        self, parameter_value, expected, meta_workflow_run_from_item
    ):
        """"""
        result = meta_workflow_run_from_item.cast_parameter_value(parameter_value)
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
        self, parameters_to_fetch, error, expected, meta_workflow_run_from_item
    ):
        """"""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_from_item.fetch_parameters(parameters_to_fetch)
        else:
            result = meta_workflow_run_from_item.fetch_parameters(parameters_to_fetch)
            assert result == expected

    @pytest.mark.parametrize(
        "is_proband_only,file_value,expected",
        [
            (False, {}, []),
            (True, {}, []),
            (False, {0: ["file_1"]}, [{"file": "file_1", "dimension": "0,0"}]),
            (True, {0: ["file_1"]}, [{"file": "file_1", "dimension": "0"}]),
            (
                False,
                {0: ["file_1"], 1: ["file_2", "file_3"]},
                [
                    {"file": "file_1", "dimension": "0,0"},
                    {"file": "file_2", "dimension": "1,0"},
                    {"file": "file_3", "dimension": "1,1"},
                ],
            ),
            (
                True,
                {0: ["file_1", "file_2"]},
                [
                    {"file": "file_1", "dimension": "0"},
                    {"file": "file_2", "dimension": "1"},
                ],
            ),
        ],
    )
    def test_format_file_input_value(self, is_proband_only, file_value, expected):
        """"""
        with mock.patch(
            "magma_ff.create_metawfr.MetaWorkflowRunFromItem.get_item_properties",
            return_value=META_WORKFLOW,
        ):
            meta_workflow_run_from_item = MetaWorkflowRunFromTestItem(is_proband_only)
            result = meta_workflow_run_from_item.format_file_input_value(file_value)
            assert result == expected

    @pytest.mark.parametrize(
        "files_to_fetch,error,expected",
        [
            (["input_foo_files"], True, None),
            (
                ["input_bams"],
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
        self, files_to_fetch, error, expected, meta_workflow_run_from_item
    ):
        """"""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_from_item.fetch_files(files_to_fetch)
        else:
            result = meta_workflow_run_from_item.fetch_files(files_to_fetch)
            assert result == expected

    @pytest.mark.parametrize(
        "meta_workflow_properties,error,expected",
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
    def test_create_meta_workflow_run_input(
        self, meta_workflow_properties, error, expected, meta_workflow_run_from_item
    ):
        """"""
        meta_workflow_run_from_item.meta_workflow_properties = meta_workflow_properties
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_from_item.create_meta_workflow_run_input()
        else:
            result = meta_workflow_run_from_item.create_meta_workflow_run_input()
            assert result == expected

    def test_create_meta_workflow_run(self, meta_workflow_run_from_item):
        """"""
        result = meta_workflow_run_from_item.create_meta_workflow_run()
        assert len(result) == len(META_WORKFLOW_RUN_FOR_ITEM)
        for key, value in result.items():
            if key == "uuid":
                continue
            assert value == META_WORKFLOW_RUN_FOR_ITEM[key]

    @pytest.mark.parametrize(
        "meta_workflow_run,error,expected",
        [
            (
                META_WORKFLOW_RUN_FOR_ITEM_NO_WORKFLOW_RUNS,
                False,
                META_WORKFLOW_RUN_FOR_ITEM,
            ),
            (META_WORKFLOW_RUN_FOR_ITEM_NO_FILES_INPUT, True, None),
        ],
    )
    def test_create_workflow_runs(
        self, meta_workflow_run, error, expected, meta_workflow_run_from_item
    ):
        """"""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                meta_workflow_run_from_item.create_workflow_runs(meta_workflow_run)
        else:
            meta_workflow_run_from_item.create_workflow_runs(meta_workflow_run)
            assert meta_workflow_run == expected


@pytest.fixture
def inputs_from_sample_processing():
    """"""
    return InputPropertiesFromSampleProcessing(SAMPLE_PROCESSING)


class TestInputPropertiesFromSampleProcessing:
    """"""
    @pytest.mark.parametrize(
        "sample_names,expected",
        [
            ([], False),
            (["FooBar"], True),
            (["Foo", "Bar"], False),
        ],
    )
    def test_is_proband_only(self, sample_names, expected, inputs_from_sample_processing):
        """"""
        with mock.patch(
            "magma_ff.create_metawfr.InputPropertiesFromSampleProcessing.sample_names",
            new_callable=mock.PropertyMock(return_value=sample_names),
        ):
            result = inputs_from_sample_processing.is_proband_only()
            assert result == expected

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
                SAMPLES,
                "bam_sample_id",
                "SAMPLE3-DNA-WGS",
                "SAMPLE4-DNA-WGS",
                "SAMPLE2-DNA-WGS",
                [2, 3, 1, 0],
            ),
            (
                SAMPLES_PEDIGREE,
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
        """"""
        expected = []
        for idx in expected_order:
            expected.append(items_to_sort[idx])
        result = inputs_from_sample_processing.sort_by_sample_name(
            items_to_sort, sample_name_key, proband, mother=mother, father=father
        )
        assert result == expected

    @pytest.mark.parametrize(
        "samples,samples_pedigree,error,expected_samples,expected_samples_pedigree",
        [
            ([], [], True, [], []),
            (SAMPLES, [], True, SORTED_SAMPLES, []),
            ([], SAMPLES_PEDIGREE, True, [], SORTED_SAMPLES_PEDIGREE),
            (SAMPLES, SAMPLES_PEDIGREE, False, SORTED_SAMPLES, SORTED_SAMPLES_PEDIGREE),
        ],
    )
    def test_clean_and_sort_samples_and_pedigree(
        self,
        samples,
        samples_pedigree,
        error,
        expected_samples,
        expected_samples_pedigree,
    ):
        """"""
        sample_processing = {"samples": samples, "samples_pedigree": samples_pedigree}
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing = InputPropertiesFromSampleProcessing(
                    sample_processing
                )
        else:
            inputs_from_sample_processing = InputPropertiesFromSampleProcessing(
                sample_processing
            )
            sorted_samples = inputs_from_sample_processing.sorted_samples
            sorted_pedigree = inputs_from_sample_processing.sorted_samples_pedigree
            assert sorted_samples == expected_samples
            assert sorted_pedigree == expected_samples_pedigree

    def sort_and_format_uuids(self, uuids):
        """"""
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
        """"""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing.get_samples_processed_file_for_format(
                    file_format
                )
        else:
            expected = self.sort_and_format_uuids(expected_uuids)
            result = inputs_from_sample_processing.get_samples_processed_file_for_format(
                file_format
            )
            assert result == expected

    @pytest.mark.parametrize(
        "sample,file_format,key,value,error,expected",
        [
            ({}, "", None, None, True, None),
            (SAMPLE_1, "foo", None, None, True, None),
            (SAMPLE_1, "bam", None, None, False, [BAM_UUID_1]),
            (SAMPLE_1, "bam", "foo", "bar", True, None),
            (
                SAMPLE_1,
                "fastq",
                None,
                None,
                False,
                [FASTQ_R1_UUID_1, FASTQ_R1_UUID_1_2, FASTQ_R2_UUID_1],
            ),
            (
                SAMPLE_1,
                "fastq",
                "paired_end",
                "1",
                False,
                [
                    FASTQ_R1_UUID_1,
                    FASTQ_R1_UUID_1_2,
                ],
            ),
            (SAMPLE_1, "fastq", "paired_end", "2", False, [FASTQ_R2_UUID_1]),
        ],
    )
    def test_get_processed_file_for_format(
        self, sample, file_format, key, value, error, expected, inputs_from_sample_processing
    ):
        """"""
        if error:
            with pytest.raises(MetaWorkflowRunCreationError):
                inputs_from_sample_processing.get_processed_file_for_format(
                    sample, file_format, key, value
                )
        else:
            result = inputs_from_sample_processing.get_processed_file_for_format(
                sample, file_format, key, value
            )
            assert result == expected

    @pytest.mark.parametrize(
        "paired_end,expected_uuids",
        [
            ("1", FASTQ_R1_UUIDS),
            ("2", FASTQ_R2_UUIDS),
        ],
    )
    def test_get_fastqs_for_paired_end(
        self, paired_end, expected_uuids, inputs_from_sample_processing
    ):
        """"""
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
        ],
    )
    def test_properties(self, attribute, expected, inputs_from_sample_processing):
        """"""
        result = getattr(inputs_from_sample_processing, attribute)
        assert result == expected


@pytest.fixture
def meta_workflow_run_from_case():
    """"""
    with mock.patch(
        "magma_ff.create_metawfr.make_embed_request",
        return_value=CASE,
    ):
        with mock.patch(
            "magma_ff.create_metawfr.MetaWorkflowRunFromItem.get_item_properties",
            return_value=META_WORKFLOW,
        ):

            return MetaWorkflowRunFromCase(None, None, None)


class TestMetaWorkflowRunFromCase:

    @pytest.mark.parametrize(
        "attribute,expected",
        [
            ("project", CASE_PROJECT),
            ("institution", CASE_INSTITUTION),
            ("accession", CASE_ACCESSION),
            ("common_fields", CASE_COMMON_FIELDS),
            ("existing_meta_workflow_runs", SAMPLE_PROCESSING_META_WORKFLOW_RUNS),
            ("sample_processing_uuid", SAMPLE_PROCESSING_UUID),
        ]
    )
    def test_properties(self, attribute, expected, meta_workflow_run_from_case):
        """"""
        result = getattr(meta_workflow_run_from_case, attribute)
        assert result == expected
