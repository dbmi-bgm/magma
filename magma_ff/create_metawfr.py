#!/usr/bin/env python3

################################################
#
#   create_metawfr
#
################################################
from __future__ import annotations

import datetime
import json
import uuid
from functools import lru_cache
from typing import Any, List

from dcicutils import ff_utils

from magma_ff.metawfl import MetaWorkflow
from magma_ff.metawflrun import MetaWorkflowRun
from magma_ff.utils import JsonObject, keep_last_item, make_embed_request


COHORT_ANALYSIS_TYPE = "CohortAnalysis"
FILE_FORMAT = "file_format"
SAMPLE_PROCESSING_TYPE = "SampleProcessing"
SAMPLE_TYPE = "Sample"
SOMATIC_ANALYSIS_TYPE = "SomaticAnalysis"
TYPE = "@type"
UUID = "uuid"


def create_meta_workflow_run(
    item_identifier: str,
    meta_workflow_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
    patch: bool = True,
) -> JsonObject:
    """Create a MetaWorkflowRun for the given item and MetaWorkflow.

    POST MetaWorkflowRun and PATCH associated item as instructed.

    :param item_identfier: Identifier (e.g. UUID, @id) for item from
        which to create the MetaWorkflowRun
    :param meta_workflow_identifier: Identifier for the MetaWorkflow
        from which to create the MetaWorkflowRun
    :param auth_key: Authorization keys for C4 account
    :param post: Whether to POST the MetaWorkflowRun created
    :param patch: Whether to PATCH the item given by the
        item_identifier with the created MetaWorkflowRun
    :returns: MetaWorkflowRun created
    """
    item = ff_utils.get_metadata(item_identifier, key=auth_key, add_on="frame=object")
    if _is_item_of_type(SAMPLE_TYPE, item):
        return create_meta_workflow_run_from_sample(
            item_identifier, meta_workflow_identifier, auth_key, post=post, patch=patch
        )
    if _is_item_of_type(SAMPLE_PROCESSING_TYPE, item):
        return create_meta_workflow_run_from_sample_processing(
            item_identifier, meta_workflow_identifier, auth_key, post=post, patch=patch
        )
    if _is_item_of_type(COHORT_ANALYSIS_TYPE, item):
        return create_meta_workflow_run_from_cohort_analysis(
            item_identifier, meta_workflow_identifier, auth_key, post=post, patch=patch
        )
    if _is_item_of_type(SOMATIC_ANALYSIS_TYPE, item):
        return create_meta_workflow_run_from_somatic_analysis(
            item_identifier, meta_workflow_identifier, auth_key, post=post, patch=patch
        )
    raise MetaWorkflowRunCreationError(
        f"No methods available to create MetaWorkflowRun for item of type(s):"
        f" {_get_item_types(item)}"
    )


def _is_item_of_type(item_type: str, item: JsonObject) -> bool:
    item_types = _get_item_types(item)
    if item_type in item_types:
        return True
    return False


def _get_item_types(item: JsonObject) -> List[str]:
    return item.get(TYPE, [])


def create_meta_workflow_run_from_sample(
    sample_identifier: str,
    meta_workflow_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
    patch: bool = True,
) -> JsonObject:
    return _create_meta_workflow_run_for_item_type(
        MetaWorkflowRunFromSample,
        sample_identifier,
        meta_workflow_identifier,
        auth_key,
        post=post,
        patch=patch,
    )


def create_meta_workflow_run_from_sample_processing(
    sample_processing_identifier: str,
    meta_workflow_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
    patch: bool = True,
) -> JsonObject:
    return _create_meta_workflow_run_for_item_type(
        MetaWorkflowRunFromSampleProcessing,
        sample_processing_identifier,
        meta_workflow_identifier,
        auth_key,
        post=post,
        patch=patch,
    )


def create_meta_workflow_run_from_cohort_analysis(
    cohort_analysis_identifier: str,
    meta_workflow_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
    patch: bool = True,
) -> JsonObject:
    return _create_meta_workflow_run_for_item_type(
        MetaWorkflowRunFromCohortAnalysis,
        cohort_analysis_identifier,
        meta_workflow_identifier,
        auth_key,
        post=post,
        patch=patch,
    )


def create_meta_workflow_run_from_somatic_analysis(
    somatic_analysis_identifier: str,
    meta_workflow_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
    patch: bool = True,
) -> JsonObject:
    return _create_meta_workflow_run_for_item_type(
        MetaWorkflowRunFromSomaticAnalysis,
        somatic_analysis_identifier,
        meta_workflow_identifier,
        auth_key,
        post=post,
        patch=patch,
    )


def _create_meta_workflow_run_for_item_type(
    meta_workflow_run_creator_class: MetaWorkflowRunFromItem,
    item_identifier: str,
    meta_workflow_identifier: str,
    auth_key: JsonObject,
    post: bool = True,
    patch: bool = True,
) -> JsonObject:
    meta_workflow_run_creator = meta_workflow_run_creator_class(
        item_identifier, meta_workflow_identifier, auth_key
    )
    if post:
        if patch:
            meta_workflow_run_creator.post_and_patch()
        else:
            meta_workflow_run_creator.post_meta_workflow_run()
    return meta_workflow_run_creator.get_meta_workflow_run()


class MetaWorkflowRunCreationError(Exception):
    """Custom exception for error tracking."""


class MetaWorkflowRunFromItem:
    """Base class to hold common methods required to create/POST a
    MetaWorkflowRun and PATCH the Item used to create it.
    """

    # Embedding API Fields
    FIELDS_TO_GET = ["*", "project.uuid", "institution.uuid"]

    # Schema constants
    META_WORKFLOW_RUNS = "meta_workflow_runs"
    ASSOCIATED_META_WORKFLOW_RUN = "associated_meta_workflow_runs"
    PROJECT = "project"
    INSTITUTION = "institution"
    UUID = "uuid"
    META_WORKFLOW = "meta_workflow"
    FINAL_STATUS = "final_status"
    PENDING = "pending"
    WORKFLOW_RUNS = "workflow_runs"
    TITLE = "title"
    INPUT = "input"
    COMMON_FIELDS = "common_fields"
    FILES = "files"
    PROBAND_ONLY = "proband_only"
    INPUT_SAMPLES = "input_samples"
    ASSOCIATED_SAMPLE_PROCESSING = "associated_sample_processing"

    # Class constants
    META_WORKFLOW_RUN_ENDPOINT = "meta-workflow-runs"

    def __init__(self, input_item_identifier, meta_workflow_identifier, auth_key):
        """Initialize the object and set all attributes.

        :param meta_workflow_identifier: MetaWorkflow[portal] UUID,
            @id, or accession
        :type meta_workflow_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        :raises MetaWorkflowRunCreationError: If required item cannot
            be found on environment of authorization key
        """
        self.auth_key = auth_key
        self.input_item = make_embed_request(
            input_item_identifier,
            self.FIELDS_TO_GET,
            self.auth_key,
            single_item=True,
        )
        if not self.input_item:
            raise MetaWorkflowRunCreationError(
                "No Item found for given identifier: %s" % input_item_identifier
            )
        self.meta_workflow = self.get_item_properties(meta_workflow_identifier)
        if not self.meta_workflow:
            raise MetaWorkflowRunCreationError(
                "No MetaWorkflow found for given identifier: %s"
                % meta_workflow_identifier
            )
        self.proband_only = self.meta_workflow.get(self.PROBAND_ONLY, False)
        self.project = self.input_item.get(self.PROJECT)
        self.institution = self.input_item.get(self.INSTITUTION)
        self.input_item_uuid = self.input_item.get(self.UUID)
        self.existing_meta_workflow_runs = self.input_item.get(
            self.META_WORKFLOW_RUNS, []
        )
        self.meta_workflow_run_uuid = str(uuid.uuid4())
        self.meta_workflow_run = {}  # Overwrite in child classes

    def get_meta_workflow_run(self) -> JsonObject:
        return self.meta_workflow_run

    def create_workflow_runs(self, meta_workflow_run):
        """Create shards and update MetaWorkflowRun[json].

        :param meta_workflow_run: MetaWorkflowRun[json]
        :type meta_workflow_run: dict
        :raises MetaWorkflowRunCreationError: If input files to
            MetaWorkflowRun cannot be identified
        """
        reformatted_file_input = None
        reformatted_meta_workflow_run = MetaWorkflowRun(meta_workflow_run).to_json()
        reformatted_input = reformatted_meta_workflow_run[self.INPUT]
        for input_item in reformatted_input:
            input_files = input_item.get(self.FILES)
            if input_files is None:
                continue
            reformatted_file_input = input_files
            break
        if reformatted_file_input is None:
            raise MetaWorkflowRunCreationError(
                "No input files were provided for the MetaWorkflowRun: %s"
                % meta_workflow_run
            )
        run_with_workflows = MetaWorkflow(self.meta_workflow).write_run(
            reformatted_file_input
        )
        meta_workflow_run[self.WORKFLOW_RUNS] = run_with_workflows[self.WORKFLOW_RUNS]

    def get_item_properties(self, item_identifier):
        """Retrieve item from given environment without raising
        exception if not found.
        :param item_identifier: Item identifier on the portal
        :type item_identifier: str
        :return: Raw view of item if found
        :rtype: dict or None
        """
        try:
            result = ff_utils.get_metadata(
                item_identifier, key=self.auth_key, add_on="frame=raw"
            )
        except Exception:
            result = None
        return result

    def post_and_patch(self):
        """POST MetaWorkflowRun[json] and PATCH SampleProcessing[portal]
        to update list of its linked MetaWorkflowRun[portal].
        """
        self.post_meta_workflow_run()
        self.patch_input_item()

    def post_meta_workflow_run(self):
        """POST MetaWorkflowRun[json] to portal."""
        try:
            ff_utils.post_metadata(
                self.meta_workflow_run,
                self.META_WORKFLOW_RUN_ENDPOINT,
                key=self.auth_key,
            )
        except Exception as error_msg:
            raise MetaWorkflowRunCreationError(
                "MetaWorkflowRun not POSTed: \n%s" % str(error_msg)
            )

    def patch_input_item(self):
        """PATCH input item with link to the new MetaWorkflowRun[portal].

        We assume property to patch is self.META_WORKFLOW_RUNS, which
        is true for Sample and SampleProcessing.

        Method will fail unless MetaWorkflowRun[json] previously
        POSTed.
        """
        meta_workflow_runs = [
            identifier for identifier in self.existing_meta_workflow_runs
        ]
        meta_workflow_runs.append(self.meta_workflow_run_uuid)
        patch_body = {self.META_WORKFLOW_RUNS: meta_workflow_runs}
        try:
            ff_utils.patch_metadata(
                patch_body, obj_id=self.input_item_uuid, key=self.auth_key
            )
        except Exception as error_msg:
            raise MetaWorkflowRunCreationError(
                "Item could not be PATCHed: \n%s" % str(error_msg)
            )

    def _get_meta_workflow_run_title(self) -> str:
        meta_workflow_title = self.meta_workflow.get(self.TITLE)
        today = datetime.date.today().isoformat()
        return f"MetaWorkflowRun {meta_workflow_title} from {today}"

    def _get_meta_workflow_run_common_fields(self) -> JsonObject:
        return {
            self.PROJECT: self._get_project_uuid(),
            self.INSTITUTION: self._get_institution_uuid(),
            self.ASSOCIATED_META_WORKFLOW_RUN: [self.meta_workflow_run_uuid],
        }

    def _get_project_uuid(self) -> str:
        return self._get_uuid_or_raise_exception(self.project, self.PROJECT)

    def _get_institution_uuid(self) -> str:
        return self._get_uuid_or_raise_exception(self.institution, self.INSTITUTION)

    def _get_uuid_or_raise_exception(self, item: Any, item_type: str) -> str:
        if not isinstance(item, dict):
            raise TypeError(f"{item_type.capitalize()} was not embedded as an object")
        uuid = item.get(self.UUID)
        if uuid:
            return uuid
        raise ValueError(f"UUID was not found on the {item_type}")


################################################
#   MetaWorkflowRunFromSampleProcessing
################################################
class MetaWorkflowRunFromSampleProcessing(MetaWorkflowRunFromItem):
    """Class to create and POST|PATCH to portal a MetaWorkflowRun[json]
    from a SampleProcessing[portal] and a MetaWorkflow[portal].
    """

    # Embedding API fields
    FIELDS_TO_GET = [
        "samples.bam_sample_id",
        "samples.uuid",
        "samples.files.uuid",
        "samples.files.paired_end",
        "samples.files.file_format.file_format",
        "samples.files.related_files.relationship_type",
        "samples.files.related_files.file.uuid",
        "samples.files.related_files.file.file_format.file_format",
        "samples.files.related_files.file.paired_end",
        "samples.processed_files.uuid",
        "samples.processed_files.paired_end",
        "samples.processed_files.file_format.file_format",
        "files.uuid",
        "files.file_format.file_format",
        "files.variant_type",
    ] + MetaWorkflowRunFromItem.FIELDS_TO_GET

    def __init__(
        self,
        sample_processing_identifier,
        meta_workflow_identifier,
        auth_key,
        expect_family_structure=True,
    ):
        """Initialize the object and set all attributes.

        :param sample_processing_identifier: SampleProcessing[portal] UUID
            or @id
        :type sample_processing_identifier: str
        :param meta_workflow_identifier: MetaWorkflow[portal] UUID,
            @id, or accession
        :type meta_workflow_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        :param expect_family_structure: Whether a family structure is
            expected on the SampleProcessing[portal]
        :type expect_family_structure: bool
        :raises MetaWorkflowRunCreationError: If required item cannot
            be found on environment of authorization key
        """
        super().__init__(
            sample_processing_identifier, meta_workflow_identifier, auth_key
        )
        self.input_properties = InputPropertiesFromSampleProcessing(
            self.input_item,
            expect_family_structure=expect_family_structure,
            proband_only=self.proband_only,
        )
        self.meta_workflow_run_input = MetaWorkflowRunInput(
            self.meta_workflow, self.input_properties
        ).create_input()
        self.meta_workflow_run = self.create_meta_workflow_run()

    def create_meta_workflow_run(self):
        """Create MetaWorkflowRun[json] to later POST to portal.

        :return: MetaWorkflowRun[json]
        :rtype: dict
        """
        meta_workflow_run = {
            self.META_WORKFLOW: self.meta_workflow.get(self.UUID),
            self.INPUT: self.meta_workflow_run_input,
            self.TITLE: self._get_meta_workflow_run_title(),
            self.PROJECT: self._get_project_uuid(),
            self.INSTITUTION: self._get_institution_uuid(),
            self.INPUT_SAMPLES: self.input_properties.input_sample_uuids,
            self.ASSOCIATED_SAMPLE_PROCESSING: self.input_item_uuid,
            self.COMMON_FIELDS: self._get_meta_workflow_run_common_fields(),
            self.FINAL_STATUS: self.PENDING,
            self.WORKFLOW_RUNS: [],
            self.UUID: self.meta_workflow_run_uuid,
        }
        self.create_workflow_runs(meta_workflow_run)
        return meta_workflow_run


class MetaWorkflowRunFromSample(MetaWorkflowRunFromItem):
    """Class to create and POST/PATCH to portal a MetaWorkflowRun from
    a Sample and MetaWorkflow.
    """

    # Embedding API fields
    FIELDS_TO_GET = [
        "files.uuid",
        "files.paired_end",
        "files.file_format.file_format",
        "files.related_files.relationship_type",
        "files.related_files.file.uuid",
        "files.related_files.file.file_format.file_format",
        "files.related_files.file.paired_end",
        "processed_files.uuid",
        "processed_files.paired_end",
        "processed_files.file_format.file_format",
    ] + MetaWorkflowRunFromItem.FIELDS_TO_GET

    def __init__(self, sample_identifier, meta_workflow_identifier, auth_key):
        """Initialize the object and set all attributes.
        :param sample_identifier: Sample UUID or @id
        :type sample_identifier: str
        :param meta_workflow_identifier: MetaWorkflow[portal] UUID,
            @id, or accession
        :type meta_workflow_identifier: str
        :param auth_key: Portal authorization key
        :type auth_key: dict
        :raises MetaWorkflowRunCreationError: If required item cannot
            be found on environment of authorization key
        """
        super().__init__(sample_identifier, meta_workflow_identifier, auth_key)
        self.input_properties = InputPropertiesFromSample(self.input_item)
        self.meta_workflow_run_input = MetaWorkflowRunInput(
            self.meta_workflow, self.input_properties
        ).create_input()
        self.meta_workflow_run = self.create_meta_workflow_run()

    def create_meta_workflow_run(self):
        """Create MetaWorkflowRun[json] to later POST to portal.

        :return: MetaWorkflowRun[json]
        :rtype: dict
        """
        meta_workflow_run = {
            self.META_WORKFLOW: self.meta_workflow.get(self.UUID),
            self.INPUT: self.meta_workflow_run_input,
            self.TITLE: self._get_meta_workflow_run_title(),
            self.PROJECT: self._get_project_uuid(),
            self.INSTITUTION: self._get_institution_uuid(),
            self.INPUT_SAMPLES: self.input_properties.input_sample_uuids,
            self.COMMON_FIELDS: self._get_meta_workflow_run_common_fields(),
            self.FINAL_STATUS: self.PENDING,
            self.WORKFLOW_RUNS: [],
            self.UUID: self.meta_workflow_run_uuid,
        }
        self.create_workflow_runs(meta_workflow_run)
        return meta_workflow_run


def _get_sample_fields_to_embed(sample_prefix: str) -> List[str]:
    sample_fields_to_get = [
        "bam_sample_id",
        "uuid",
        "files.uuid",
        "files.file_format.file_format",
        "processed_files.uuid",
        "processed_files.file_format.file_format",
        "tissue_type",
    ]
    return [f"{sample_prefix}.{field}" for field in sample_fields_to_get]


class MetaWorkflowRunFromCohortAnalysis(MetaWorkflowRunFromItem):
    FIELDS_TO_GET = (
        _get_sample_fields_to_embed("case_samples")
        + _get_sample_fields_to_embed("control_samples")
        + MetaWorkflowRunFromItem.FIELDS_TO_GET
    )

    def __init__(
        self,
        cohort_analysis_identifier: str,
        meta_workflow_identifier: str,
        auth_key: JsonObject,
    ):
        """Initialize the object and set all attributes.
        :param cohort_analysis_identifier: cohort_analysis UUID or @id
        :param meta_workflow_identifier: MetaWorkflow[portal] UUID,
            @id, or accession
        :param auth_key: Portal authorization key
        :raises MetaWorkflowRunCreationError: If required item cannot
            be found on environment of authorization key
        """
        super().__init__(cohort_analysis_identifier, meta_workflow_identifier, auth_key)
        self.input_properties = InputPropertiesFromCohortAnalysis(self.input_item)
        self.meta_workflow_run_input = MetaWorkflowRunInput(
            self.meta_workflow, self.input_properties
        ).create_input()
        self.meta_workflow_run = self.create_meta_workflow_run()

    def create_meta_workflow_run(self) -> JsonObject:
        """Create MetaWorkflowRun[json] to later POST to portal.

        :return: MetaWorkflowRun[json]
        """
        meta_workflow_run = {
            self.META_WORKFLOW: self.meta_workflow.get(self.UUID),
            self.INPUT: self.meta_workflow_run_input,
            self.TITLE: self._get_meta_workflow_run_title(),
            self.PROJECT: self._get_project_uuid(),
            self.INSTITUTION: self._get_institution_uuid(),
            self.COMMON_FIELDS: self._get_meta_workflow_run_common_fields(),
            self.FINAL_STATUS: self.PENDING,
            self.WORKFLOW_RUNS: [],
            self.UUID: self.meta_workflow_run_uuid,
        }
        self.create_workflow_runs(meta_workflow_run)
        return meta_workflow_run


class MetaWorkflowRunFromSomaticAnalysis(MetaWorkflowRunFromItem):
    FIELDS_TO_GET = (
        [
            "processed_files.*",
            "processed_files.file_format.*",
            "individual.*",
        ]
        + _get_sample_fields_to_embed("samples")
        + MetaWorkflowRunFromItem.FIELDS_TO_GET
    )

    def __init__(
        self,
        somatic_analysis_identifier: str,
        meta_workflow_identifier: str,
        auth_key: JsonObject,
    ):
        """Initialize the object and set all attributes.
        :param somatic_analysis_identifier: somatic_analysis UUID or @id
        :param meta_workflow_identifier: MetaWorkflow[portal] UUID,
            @id, or accession
        :param auth_key: Portal authorization key
        :raises MetaWorkflowRunCreationError: If required item cannot
            be found on environment of authorization key
        """
        super().__init__(
            somatic_analysis_identifier, meta_workflow_identifier, auth_key
        )
        self.input_properties = InputPropertiesFromSomaticAnalysis(self.input_item)
        self.meta_workflow_run_input = MetaWorkflowRunInput(
            self.meta_workflow, self.input_properties
        ).create_input()
        self.meta_workflow_run = self.create_meta_workflow_run()

    def create_meta_workflow_run(self) -> JsonObject:
        """Create MetaWorkflowRun[json] to later POST to portal.

        :return: MetaWorkflowRun[json]
        """
        meta_workflow_run = {
            self.META_WORKFLOW: self.meta_workflow.get(self.UUID),
            self.INPUT: self.meta_workflow_run_input,
            self.TITLE: self._get_meta_workflow_run_title(),
            self.PROJECT: self._get_project_uuid(),
            self.INSTITUTION: self._get_institution_uuid(),
            self.COMMON_FIELDS: self._get_meta_workflow_run_common_fields(),
            self.FINAL_STATUS: self.PENDING,
            self.WORKFLOW_RUNS: [],
            self.UUID: self.meta_workflow_run_uuid,
        }
        self.create_workflow_runs(meta_workflow_run)
        return meta_workflow_run


################################################
#   MetaWorkflowRunInput
################################################
class MetaWorkflowRunInput:
    """Generate MetaWorkflowRun[json] input given MetaWorkflow[json] and
    input properties object with required input fields.
    """

    # Schema constants
    ARGUMENT_NAME = "argument_name"
    ARGUMENT_TYPE = "argument_type"
    VALUE = "value"
    VALUE_TYPE = "value_type"
    FILE = "file"
    FILES = "files"
    PARAMETER = "parameter"
    DIMENSION = "dimension"
    DIMENSIONALITY = "dimensionality"
    INPUT = "input"
    UUID = "uuid"

    def __init__(self, meta_workflow, input_properties):
        """Initialize the object and set all attributes.

        :param meta_workflow: MetaWorkflow[json]
        :type meta_workflow: dict
        :param input_properties: Object containing expected input
            parameters for MetaWorkflow[json]
        :type input_properties: object
        """
        self.meta_workflow = meta_workflow
        self.input_properties = input_properties

    def create_input(self):
        """Create MetaWorkflowRun[json] input based on arguments specified in
        MetaWorkflow[json].

        :return: MetaWorkflowRun[json] input
        :rtype: dict
        :raises MetaWorkflowRunCreationError: If input argument provided
            from MetaWorkflow could not be handled
        """
        result = []
        input_files_to_fetch = []
        input_parameters_to_fetch = []
        input_files = []
        input_parameters = []
        meta_workflow_input = self.meta_workflow.get(self.INPUT, [])
        for input_arg in meta_workflow_input:
            if self.FILES in input_arg or self.VALUE in input_arg:
                continue
            input_arg_name = input_arg.get(self.ARGUMENT_NAME)
            input_arg_type = input_arg.get(self.ARGUMENT_TYPE)
            if input_arg_type == self.FILE:
                input_arg_dimensions = input_arg.get(self.DIMENSIONALITY)
                input_files_to_fetch.append((input_arg_name, input_arg_dimensions))
            elif input_arg_type == self.PARAMETER:
                parameter_value_type = input_arg.get(self.VALUE_TYPE)
                input_parameters_to_fetch.append((input_arg_name, parameter_value_type))
            else:
                raise MetaWorkflowRunCreationError(
                    "Found an unexpected MetaWorkflow input argument type (%s) for"
                    " MetaWorkflow with uuid: %s"
                    % (input_arg_type, self.meta_workflow.get(self.UUID))
                )
        if input_parameters_to_fetch:
            input_parameters = self.fetch_parameters(input_parameters_to_fetch)
            result += input_parameters
        if input_files_to_fetch:
            input_files = self.fetch_files(input_files_to_fetch)
            result += input_files
        return result

    def fetch_files(self, files_to_fetch):
        """Create file inputs for MetaWorkflowRun[json].

        :param files_to_fetch: File input arguments from MetaWorkflow[json]
        :type files_to_fetch: list((str, int))
        :return: Structured file input for MetaWorkflowRun[json]
        :rtype: list(dict)
        :raises MetaWorkflowRunCreationError: If file input argument name
            from MetaWorkflow could not be found on the input properties
            class
        """
        result = []
        for file_parameter, input_dimensions in files_to_fetch:
            try:
                file_parameter_value = getattr(
                    self.input_properties, file_parameter.lower()
                )
            except AttributeError:
                raise MetaWorkflowRunCreationError(
                    "Could not find input parameter: %s" % file_parameter
                )
            formatted_file_value = self.format_file_input_value(
                file_parameter, file_parameter_value, input_dimensions
            )
            file_parameter_result = {
                self.ARGUMENT_NAME: file_parameter,
                self.ARGUMENT_TYPE: self.FILE,
                self.FILES: formatted_file_value,
            }
            result.append(file_parameter_result)
        return result

    def format_file_input_value(
        self,
        file_parameter: str,
        file_input_value: List[List[str]],
        input_dimensions: int,
    ) -> List[JsonObject]:
        """Create one structured file input for MetaWorkflowRun[json].

        File inputs arrive as lists of lists of UUIDs for now. For
        dimension 1, each sub-list should contain <= 1 UUID. For
        dimension 2, any number of UUIDs can be in each sub-list.

        To add more dimensions, will need to refactor inputs and this
        method.

        :param file_parameter: Name of file input argument
        :param file_input_value: File input values
        :param input_dimensions: The number of dimensions to use for
            the given file parameter
        :return: Structured file argument input
        :raises MetaWorkflowRunCreationError: If expected dimensions
            could not be handled or an input of dimension 1 has more
            than 1 entry per sample (i.e. is 2 dimensional)
        """
        result = []
        for input_idx, file_input in enumerate(file_input_value):
            if not file_input:
                raise MetaWorkflowRunCreationError(
                    f"Found an empty list of input files for parameter {file_parameter}"
                )
            if input_dimensions == 1:
                if len(file_input_value) > 1:
                    if len(file_input) > 1:
                        raise MetaWorkflowRunCreationError(
                            f"Found multiple input files when only 1 was expected for"
                            f" parameter {file_parameter}: {file_input}"
                        )
                    for file_idx, file_uuid in enumerate(file_input):
                        if not isinstance(file_uuid, str):
                            raise MetaWorkflowRunCreationError(
                                f"File input for parameter {file_parameter} was"
                                f" unexpected. Exected input file dimension"
                                f" {input_dimensions} but received the following:"
                                f" {file_uuid}."
                            )
                        dimension = str(input_idx)
                        formatted_file_result = {
                            self.FILE: file_uuid,
                            self.DIMENSION: dimension,
                        }
                        result.append(formatted_file_result)
                else:
                    for file_idx, file_uuid in enumerate(file_input):
                        if not isinstance(file_uuid, str):
                            raise MetaWorkflowRunCreationError(
                                f"File input for parameter {file_parameter} was"
                                f" unexpected. Exected input file dimension"
                                f" {input_dimensions} but received the following:"
                                f" {file_uuid}."
                            )
                        dimension = str(file_idx)
                        formatted_file_result = {
                            self.FILE: file_uuid,
                            self.DIMENSION: dimension,
                        }
                        result.append(formatted_file_result)
            elif input_dimensions == 2:
                for file_uuid_idx, file_uuid in enumerate(file_input):
                    if not isinstance(file_uuid, str):
                        raise MetaWorkflowRunCreationError(
                            f"File input for parameter {file_parameter} was unexpected."
                            f" Exected input file dimension {input_dimensions} but"
                            f" received the following: {file_input_value}."
                        )
                    dimension = f"{input_idx},{file_uuid_idx}"
                    formatted_file_result = {
                        self.FILE: file_uuid,
                        self.DIMENSION: dimension,
                    }
                    result.append(formatted_file_result)
            else:
                raise MetaWorkflowRunCreationError(
                    f"Received an unexpected dimension number for parameter"
                    f" {file_parameter}: {input_dimensions}"
                )
        return result

    def fetch_parameters(self, parameters_to_fetch):
        """Create non-file parameters for MetaWorkflowRun[json].

        :param parameters_to_fetch: Non-file input parameters from
            MetaWorkflow[json]
        :type parameters_to_fetch: list((str, str))
        :return: Structured non-file input
        :rtype: list(dict)
        :raises MetaWorkflowRunCreationError: If given parameter could
            not be found on the input properties class
        """
        result = []
        for parameter, value_type in parameters_to_fetch:
            try:
                parameter_value = getattr(self.input_properties, parameter.lower())
            except AttributeError:
                raise MetaWorkflowRunCreationError(
                    "Could not find input parameter: %s" % parameter
                )
            parameter_value = self.cast_parameter_value(parameter_value)
            parameter_result = {
                self.ARGUMENT_NAME: parameter,
                self.ARGUMENT_TYPE: self.PARAMETER,
                self.VALUE: parameter_value,
                self.VALUE_TYPE: value_type,
            }
            result.append(parameter_result)
        return result

    def cast_parameter_value(self, parameter_value):
        """Cast parameter value in expected format based on value
        type.

        :param parameter_value: Value for a given input parameter
        :type parameter_value: object
        :return: Possibly JSON-formatted string representation of the
            value
        :rtype: str
        """
        if isinstance(parameter_value, list) or isinstance(parameter_value, dict):
            result = json.dumps(parameter_value)
        else:
            result = str(parameter_value)
        return result


def get_files_for_file_formats(file_items, file_formats, requirements=None):
    """Gather file UUIDs for File items matching the file format
    and meeting the requirements.

    :param file_items: The files to sort through
    :type file_items: list(dict)
    :param file_formats: Accepted formats of files to get
    :type file_formats: list or set
    :param requirements: Requirements a file must meet in order to
        be acceptable, as key, value pairs of property names, lists
        of acceptable property values
    :type requirements: dict
    :returns: File UUIDs of files meeting file format and
        requirements
    :rtype: list(str)
    """
    result = []
    for file_item in file_items:
        file_item_format = file_item.get(FILE_FORMAT, {}).get(FILE_FORMAT)
        if file_item_format in file_formats:
            if requirements:
                if not all(
                    file_item.get(key) in accepted_values
                    for key, accepted_values in requirements.items()
                ):
                    continue
            file_uuid = file_item.get(UUID)
            result.append(file_uuid)
    return result


################################################
#   InputPropertiesFromSampleProcessing
################################################
class InputPropertiesFromSampleProcessing:
    """Class for accessing MetaWorkflowRun[json] input arguments from a
    SampleProcessing[json].
    """

    # Schema constants
    UUID = "uuid"
    BAM_SAMPLE_ID = "bam_sample_id"
    SAMPLES_PEDIGREE = "samples_pedigree"
    SAMPLES = "samples"
    RELATIONSHIP = "relationship"
    PROBAND = "proband"
    MOTHER = "mother"
    FATHER = "father"
    INDIVIDUAL = "individual"
    PARENTS = "parents"
    SAMPLE_NAME = "sample_name"
    SEX = "sex"
    FILES = "files"
    FILE_FORMAT = "file_format"
    VARIANT_TYPE = "variant_type"
    SNV_VARIANT_TYPE = "SNV"
    SV_VARIANT_TYPE = "SV"

    # Class constants
    GENDER = "gender"
    VCF_GZ_FORMAT = "vcf_gz"
    VCF_FORMAT = "vcf"
    VCF_FORMATS = set([VCF_GZ_FORMAT, VCF_FORMAT])

    def __init__(
        self,
        sample_processing,
        expect_family_structure=True,
        proband_only=False,
    ):
        """Initialize the object and set attributes.

        :param sample_processing: SampleProcessing[json]
        :type sample_processing: dict
        :param expect_family_structure: Whether a family structure is
            expected on the SampleProcessing, which influences sample
            sorting and pedigree expectations
        :type expect_family_structure: bool
        :param proband_only: Whether inputs should only use proband
            samples. Only applicable within family structure.
        :type proband_only: bool
        """
        self.expect_family_structure = expect_family_structure
        self.sample_processing = sample_processing
        self.samples = sample_processing.get(self.SAMPLES)
        if not self.samples:
            raise MetaWorkflowRunCreationError(
                "No Samples found on SampleProcessing: %s" % self.sample_processing
            )
        self.samples_pedigree = sample_processing.get(self.SAMPLES_PEDIGREE, [])
        if expect_family_structure:
            self.clean_and_sort_samples_and_pedigree()
        if proband_only:
            self.remove_non_proband_samples()
        self.sample_inputs = [
            InputPropertiesFromSample(sample) for sample in self.samples
        ]

    def clean_and_sort_samples_and_pedigree(self):
        """Sort Samples and pedigree and remove parents from pedigree
        if not included in Samples.

        Sorting order will be proband, then mother, then father, as
        applicable.

        :raises MetaWorkflowRunCreationError: If pedigree not found,
            samples and pegigree of different lengths, or required
            properties not found
        """
        proband_name = None
        mother_name = None
        father_name = None
        if not self.samples_pedigree:
            raise MetaWorkflowRunCreationError(
                "No samples_pedigree found on SampleProcessing: %s"
                % self.sample_processing
            )
        if len(self.samples) != len(self.samples_pedigree):
            raise MetaWorkflowRunCreationError(
                "Number of Samples did not match number of entries in samples_pedigree"
                " on SampleProcessing: %s" % self.sample_processing
            )
        all_individuals = [
            sample.get(self.INDIVIDUAL)
            for sample in self.samples_pedigree
            if sample.get(self.INDIVIDUAL)
        ]
        bam_sample_ids = [
            sample.get(self.BAM_SAMPLE_ID)
            for sample in self.samples
            if sample.get(self.BAM_SAMPLE_ID)
        ]
        for pedigree_sample in self.samples_pedigree:
            parents = pedigree_sample.get(self.PARENTS, [])
            if parents:  # Remove parents that aren't in samples_pedigree
                missing_parents = [
                    parent for parent in parents if parent not in all_individuals
                ]
                for missing_parent in missing_parents:
                    parents.remove(missing_parent)
            sample_name = pedigree_sample.get(self.SAMPLE_NAME)
            if sample_name is None:
                raise MetaWorkflowRunCreationError(
                    "No sample name given for sample in pedigree: %s" % pedigree_sample
                )
            elif sample_name not in bam_sample_ids:
                raise MetaWorkflowRunCreationError(
                    "Sample in pedigree not found on SampleProcessing: %s" % sample_name
                )
            sex = pedigree_sample.get(self.SEX)
            if sex is None:
                raise MetaWorkflowRunCreationError(
                    "No sex given for sample in pedigree: %s" % pedigree_sample
                )
            relationship = pedigree_sample.get(self.RELATIONSHIP)
            if relationship == self.PROBAND:
                proband_name = sample_name
            elif relationship == self.MOTHER:
                mother_name = sample_name
            elif relationship == self.FATHER:
                father_name = sample_name
        if proband_name is None:
            raise MetaWorkflowRunCreationError(
                "No proband found within the pedigree: %s" % self.samples_pedigree
            )
        self.sort_by_sample_name(
            self.samples_pedigree,
            self.SAMPLE_NAME,
            proband_name,
            mother=mother_name,
            father=father_name,
        )
        self.sort_by_sample_name(
            self.samples,
            self.BAM_SAMPLE_ID,
            proband_name,
            mother=mother_name,
            father=father_name,
        )

    def remove_non_proband_samples(self):
        """Remove all non-proband members from samples and pedigree."""
        if self.expect_family_structure:  # Already sorted to proband-first
            del self.samples_pedigree[1:]
            del self.samples[1:]
        else:
            proband_only_samples = []
            proband_only_samples_pedigree = []
            proband_sample_names = self.probands
            for sample in self.samples:
                sample_name = sample.get(self.BAM_SAMPLE_ID)
                if sample_name in proband_sample_names:
                    proband_only_samples.append(sample)
            for pedigree_sample in self.samples_pedigree:
                sample_name = pedigree_sample.get(self.SAMPLE_NAME)
                if sample_name in proband_sample_names:
                    proband_only_samples_pedigree.append(pedigree_sample)
            self.samples = proband_only_samples
            self.samples_pedigree = proband_only_samples_pedigree

    def sort_by_sample_name(
        self, items_to_sort, sample_name_key, proband, mother=None, father=None
    ):
        """Sort items to be proband, mother, father, then other family
        members, as applicable.

        :param items_to_sort: Items to sort
        :type items_to_sort: list(dict)
        :param sample_name_key: Key of the item dict corresponding to
            sample name of the item
        :type sample_name_key: str
        :param proband: Proband sample name
        :type proband: str
        :param mother: Mother sample name
        :type mother: str or None
        :param father: Father sample name
        :type father: str or None
        """
        sorted_items = []
        other_idx = []
        proband_idx = None
        mother_idx = None
        father_idx = None
        for idx, item in enumerate(items_to_sort):
            sample_name = item.get(sample_name_key)
            if sample_name == proband:
                proband_idx = idx
            elif mother and sample_name == mother:
                mother_idx = idx
            elif father and sample_name == father:
                father_idx = idx
            else:
                other_idx.append(idx)
        if proband_idx is not None:
            sorted_items.append(items_to_sort[proband_idx])
        if mother_idx is not None:
            sorted_items.append(items_to_sort[mother_idx])
        if father_idx is not None:
            sorted_items.append(items_to_sort[father_idx])
        for idx in other_idx:
            sorted_items.append(items_to_sort[idx])
        items_to_sort.clear()
        items_to_sort.extend(sorted_items)

    def get_property_from_samples(self, sample_property):
        """Retrieve input property from samples on this
        SampleProcessing.

        Input property names must align here and on corresponding
        class for Samples.

        :param sample_property: The MWF input argument to get
        :type sample_property: str
        :return: Property retrieved from all Samples on item
        :rtype: list
        """
        result = []
        for sample_input in self.sample_inputs:
            result += getattr(sample_input, sample_property)
        return result

    def get_submitted_vcf_files(self, requirements=None):
        """Collect submitted VCFs meeting given requirements.

        :param requirements: Property requirements for the VCF files to
            meet
        :type requirements: dict
        :returns: UUIDs of found VCF files
        :rtype: list
        :raises MetaWorkflowRunCreationError: If no VCF files meeting
            requirements found
        """
        submitted_files = self.sample_processing.get(self.FILES, [])
        result = get_files_for_file_formats(
            submitted_files, self.VCF_FORMATS, requirements=requirements
        )
        if not result:
            raise MetaWorkflowRunCreationError(
                f"No file with acceptable VCF file format meeting requirements found on"
                f" SampleProcessing: {self.sample_processing}"
            )
        return [result]

    # SampleProcessing-specific properties
    @property
    def pedigree(self):
        """Sorted pedigree input."""
        result = []
        for pedigree_sample in self.samples_pedigree:
            result.append(
                {
                    self.PARENTS: pedigree_sample.get(self.PARENTS, []),
                    self.INDIVIDUAL: pedigree_sample.get(self.INDIVIDUAL, ""),
                    self.SAMPLE_NAME: pedigree_sample.get(self.SAMPLE_NAME),
                    # May want to switch gender key to sex below
                    self.GENDER: pedigree_sample.get(self.SEX),
                }
            )
        return result

    @property
    def sample_name_proband(self):
        """Proband Sample name input."""
        return [self.sample_names[0]]  # Already sorted to proband-first

    @property
    def bamsnap_titles(self):
        """Sorted BAMSnap name input."""
        result = []
        for sample_pedigree in self.samples_pedigree:
            sample_name = sample_pedigree.get(self.SAMPLE_NAME)
            sample_relationship = sample_pedigree.get(self.RELATIONSHIP, "")
            result.append("%s (%s)" % (sample_name, sample_relationship))
        return result

    @property
    def family_size(self):
        """Family size input."""
        return len(self.sample_names)

    @property
    def probands(self):
        """Proband list for jointly calling multiple g.vcf files"""
        probands = []
        if not self.samples_pedigree:
            raise MetaWorkflowRunCreationError(
                "Cannot create probands list because there is no samples pedigree."
            )
        for pedigree_sample in self.samples_pedigree:
            relationship = pedigree_sample.get(self.RELATIONSHIP)
            sample_name = pedigree_sample.get(self.SAMPLE_NAME)
            if (relationship is None) or (sample_name is None):
                raise MetaWorkflowRunCreationError(
                    "Pedigree information incomplete. Relationship and sample name"
                    " are required."
                )
            if relationship == self.PROBAND:
                probands.append(sample_name)
        return probands

    @property
    def input_vcfs(self):
        """VCFs submitted to the SampleProcessing."""
        return self.get_submitted_vcf_files()

    @property
    def input_snv_vcfs(self):
        """SNV VCFs submitted to the SampleProcessing."""
        requirements = {self.VARIANT_TYPE: [self.SNV_VARIANT_TYPE]}
        return self.get_submitted_vcf_files(requirements=requirements)

    @property
    def input_sv_vcfs(self):
        """SV VCFs submitted to the SampleProcessing."""
        requirements = {self.VARIANT_TYPE: [self.SV_VARIANT_TYPE]}
        return self.get_submitted_vcf_files(requirements=requirements)

    # Properties from Samples
    @property
    def sample_names(self):
        """Sorted Sample name input."""
        return self.get_property_from_samples("sample_names")

    @property
    def input_sample_uuids(self):
        """Sorted Sample UUID input."""
        return self.get_property_from_samples("input_sample_uuids")

    @property
    def input_crams(self):
        """CRAM file input."""
        return self.get_property_from_samples("input_crams")

    @property
    def input_gvcfs(self):
        """gVCF file input."""
        return self.get_property_from_samples("input_gvcfs")

    @property
    def fastqs_r1(self):
        """FASTQ paired-end 1 file input."""
        return self.get_property_from_samples("fastqs_r1")

    @property
    def fastqs_r2(self):
        """FASTQ paired-end 2 file input."""
        return self.get_property_from_samples("fastqs_r2")

    @property
    def input_bams(self):
        """BAM file input."""
        return self.get_property_from_samples("input_bams")

    @property
    def rcktar_file_names(self):
        """Sorted names for created RckTar files input."""
        return self.get_property_from_samples("rcktar_file_names")


class InputPropertiesFromSample:
    """Class for accessing MetaWorkflowRun input arguments from a
    Sample item.
    """

    # Schema constants
    BAM_SAMPLE_ID = "bam_sample_id"
    FILE = "file"
    FILE_FORMAT = "file_format"
    FILES = "files"
    INDIVIDUAL = "individual"
    PAIRED_END = "paired_end"
    PAIRED_END_1 = "1"
    PAIRED_END_2 = "2"
    PAIRED_WITH = "paired with"
    PROCESSED_FILES = "processed_files"
    RELATED_FILES = "related_files"
    RELATIONSHIP_TYPE = "relationship_type"
    SEX = "sex"
    UUID = "uuid"

    # File formats
    FASTQ_FORMAT = "fastq"
    CRAM_FORMAT = "cram"
    BAM_FORMAT = "bam"
    GVCF_FORMAT = "gvcf_gz"

    # Class constants
    RCKTAR_FILE_ENDING = ".rck.gz"

    def __init__(self, sample):
        """Initialize the object and set attributes.

        :param sample: Sample metadata
        :type sample: dict
        """
        self.sample = sample

    def get_processed_files_for_file_format(self, file_format, requirements=None):
        """Get all files matching given file format on given sample
        that meet the given requirements.

        :param file_format: Format of files to get
        :type file_format: str
        :param requirements: Requirements a file must meet in order to
            be acceptable, as key, value pairs of property names, lists
            of acceptable property values
        :type requirements: dict
        :returns: Processed file UUIDs of files meeting file format and
            requirements
        :rtype: list(str)
        """
        result = []
        processed_files = self.sample.get(self.PROCESSED_FILES, [])
        result = get_files_for_file_formats(
            processed_files, [file_format], requirements=requirements
        )
        if not result:
            raise MetaWorkflowRunCreationError(
                "No file with format %s meeting requirements %s found on Sample: %s"
                % (file_format, requirements, self.sample)
            )
        return result

    def get_submitted_files_for_file_format(self, file_format, requirements=None):
        """Get all submitted file UUIDs matching file format and meeting
        requirements.

        :param file_format: Format of files to get
        :type file_format: str
        :param requirements: Requirements a file must meet in order to
            be acceptable, as key, value pairs of property names, lists
            of acceptable property values
        :type requirements: dict
        :returns: Submitted file UUIDs of files meeting file format and
            requirements
        :rtype: list(str)
        """
        submitted_files = self.sample.get(self.FILES, [])
        result = get_files_for_file_formats(
            submitted_files, [file_format], requirements=requirements
        )
        if not result:
            raise MetaWorkflowRunCreationError(
                "No file with format %s meeting requirements %s found on Sample: %s"
                % (file_format, requirements, self.sample)
            )
        return result

    def get_fastqs_for_paired_end(self, paired_end):
        """Get FASTQ files on Sample for given paired end.

        Searches for files first under Sample.files and then, if no
        matches found there, under Sample.processed_files.

        For non-ProcessedFile FASTQ files, enforce more stringent
        related file checks. For ProcessedFile FASTQs, related file
        property generally absent.

        :param paired_end: Desired paired end for FASTQs
        :type paired_end: str
        :return: FASTQ file UUIDs of matching paired end
        :rtype: list(str)
        :raises MetaWorkflowRunCreationError: If FASTQ file without
            related_files property found or no FASTQ files of matching
            paired end found
        """
        paired_end_fastqs = []
        fastq_files = self.sample.get(self.FILES, [])
        for fastq_file in fastq_files:
            file_uuid = fastq_file.get(self.UUID)
            if file_uuid in paired_end_fastqs:  # May have come as related file
                continue
            file_format = fastq_file.get(self.FILE_FORMAT, {}).get(self.FILE_FORMAT)
            if file_format != self.FASTQ_FORMAT:
                continue
            related_files = fastq_file.get(self.RELATED_FILES)
            if related_files is None:
                raise MetaWorkflowRunCreationError(
                    "Sample contains an uploaded FASTQ file without a related file: %s"
                    % self.sample
                )
            file_paired_end = fastq_file.get(self.PAIRED_END)
            if file_paired_end == paired_end:
                paired_end_fastqs.append(file_uuid)
                continue
            else:
                related_paired_end = False
                for related_file in related_files:
                    file_relation = related_file.get(self.RELATIONSHIP_TYPE)
                    if file_relation != self.PAIRED_WITH:
                        continue
                    file_properties = related_file.get(self.FILE, {})
                    related_uuid = file_properties.get(self.UUID)
                    related_paired_end = file_properties.get(self.PAIRED_END)
                    if related_paired_end == paired_end:
                        related_paired_end = True
                        if related_uuid not in paired_end_fastqs:
                            paired_end_fastqs.append(related_uuid)
                        break
                if related_paired_end is False:
                    raise MetaWorkflowRunCreationError(
                        "Found an uploaded FASTQ file without matching paired end (%s)"
                        " on the file or any of its related files: %s."
                        % (paired_end, fastq_file)
                    )

        if not paired_end_fastqs:  # May have come from CRAM conversion
            requirements = {self.PAIRED_END: [paired_end]}
            paired_end_fastqs = self.get_processed_files_for_file_format(
                self.FASTQ_FORMAT, requirements=requirements
            )
        return paired_end_fastqs

    @property
    def input_crams(self):
        """CRAM file input."""
        return [self.get_submitted_files_for_file_format(self.CRAM_FORMAT)]

    @property
    def input_gvcfs(self):
        """gVCF file input."""
        gvcfs = self.get_processed_files_for_file_format(self.GVCF_FORMAT)
        return [keep_last_item(gvcfs)]

    @property
    def fastqs_r1(self):
        """FASTQ paired-end 1 file input."""
        return [self.get_fastqs_for_paired_end(self.PAIRED_END_1)]

    @property
    def fastqs_r2(self):
        """FASTQ paired-end 2 file input."""
        return [self.get_fastqs_for_paired_end(self.PAIRED_END_2)]

    @property
    def fastqs(self):
        """User-uploaded FASTQ files, regardless of paired end."""
        result = []
        fastq_files = self.sample.get(self.FILES, [])
        for fastq_file in fastq_files:
            file_uuid = fastq_file.get(self.UUID)
            file_format = fastq_file.get(self.FILE_FORMAT, {}).get(self.FILE_FORMAT)
            if file_format == self.FASTQ_FORMAT:
                result.append(file_uuid)
        return [result]

    @property
    def input_bams(self):
        """BAM file input."""
        bams = self.get_processed_files_for_file_format(self.BAM_FORMAT)
        return [keep_last_item(bams)]

    @property
    def rcktar_file_names(self):
        """Name for created RckTar files input."""
        return [
            sample_name + self.RCKTAR_FILE_ENDING for sample_name in self.sample_names
        ]

    @property
    def sample_names(self):
        """Sample name from BAM"""
        return [self.sample[self.BAM_SAMPLE_ID]]

    @property
    def input_sample_uuids(self):
        """Sample UUID"""
        return [self.sample[self.UUID]]

    @property
    def individual(self) -> JsonObject:
        return self.sample.get(self.INDIVIDUAL)

    @property
    def sex(self) -> str:
        return self.individual.get(self.SEX)


class InputPropertiesFromCohortAnalysis:

    # Schema constants
    CASE_SAMPLES = "case_samples"
    CONTROL_SAMPLES = "control_samples"

    def __init__(self, cohort_analysis: JsonObject) -> None:
        """Initialize the object and set attributes.

        :param cohort_analysis: CohortAnalysis metadata
        :type cohort_analysis: dict
        """
        self.cohort_analysis = cohort_analysis
        case_samples = cohort_analysis.get(self.CASE_SAMPLES, [])
        self.case_sample_inputs = [
            InputPropertiesFromSample(sample) for sample in case_samples
        ]
        control_samples = cohort_analysis.get(self.CONTROL_SAMPLES, [])
        self.control_sample_inputs = [
            InputPropertiesFromSample(sample) for sample in control_samples
        ]

    def _get_input_property_from_all_samples(
        self, input_property_name: str
    ) -> List[Any]:
        result = []
        result += self._get_input_property_from_case_samples(input_property_name)
        result += self._get_input_property_from_control_samples(input_property_name)
        return result

    def _get_input_property_from_case_samples(
        self, input_property_name: str
    ) -> List[Any]:
        result = []
        for case_sample_input in self.case_sample_inputs:
            result += getattr(case_sample_input, input_property_name)
        return result

    def _get_input_property_from_control_samples(
        self, input_property_name: str
    ) -> List[Any]:
        result = []
        for control_sample_input in self.control_sample_inputs:
            result += getattr(control_sample_input, input_property_name)
        return result

    @property
    def input_gvcfs(self) -> List[List[str]]:
        return self._get_input_property_from_all_samples("input_gvcfs")


class InputPropertiesFromSomaticAnalysis:

    # Schema constants
    FILE_TYPE = "file_type"
    INDIVIDUAL = "individual"
    PROCESSED_FILES = "processed_files"
    SAMPLES = "samples"
    SEX = "sex"
    TISSUE_TYPE = "tissue_type"
    TISSUE_TYPE_NORMAL = "Normal"
    TISSUE_TYPE_TUMOR = "Tumor"

    TNSCOPE_FILE_TYPE = "TNscope VCF"
    FILE_FORMAT_VCF_GZ = "vcf_gz"

    def __init__(self, somatic_analysis: JsonObject) -> None:
        """Initialize the object and set attributes.

        :param sample_analysis: SampleAnalysis metadata
        :type sample_analysis: dict
        """
        self.somatic_analysis = somatic_analysis
        self._validate()

    def _validate(self) -> None:
        if len(self.samples) != len(self.tumor_samples) + len(self.normal_samples):
            raise MetaWorkflowRunCreationError(
                "Not every sample labelled as tumor or normal"
            )

    def _get_sample_type(self, sample: JsonObject) -> str:
        return sample.get(self.TISSUE_TYPE, "")

    def _is_normal_sample(self, sample: JsonObject) -> bool:
        return self._get_sample_type(sample) == self.TISSUE_TYPE_NORMAL

    def _is_tumor_sample(self, sample: JsonObject) -> bool:
        return self._get_sample_type(sample) == self.TISSUE_TYPE_TUMOR

    def _get_input_properties_from_sample_inputs(
        self, sample_inputs: List[InputPropertiesFromSample], input_property_name: str
    ) -> List[Any]:
        result = []
        for sample_input in sample_inputs:
            result += getattr(sample_input, input_property_name)
        return result

    def _get_input_properties_from_all_samples(
        self, input_property_name: str
    ) -> List[Any]:
        return self._get_input_properties_from_sample_inputs(
            self.all_sample_inputs, input_property_name
        )

    def _get_input_properties_from_tumor_samples(
        self, input_property_name: str
    ) -> List[Any]:
        return self._get_input_properties_from_sample_inputs(
            self.tumor_sample_inputs, input_property_name
        )

    def _get_input_properties_from_normal_samples(
        self, input_property_name: str
    ) -> List[Any]:
        return self._get_input_properties_from_sample_inputs(
            self.normal_sample_inputs, input_property_name
        )

    @property
    def individual(self) -> JsonObject:
        return self.somatic_analysis.get(self.INDIVIDUAL, {})

    @property
    def samples(self) -> List[JsonObject]:
        return self.somatic_analysis.get(self.SAMPLES, [])

    @property
    def tumor_samples(self) -> List[JsonObject]:
        return [sample for sample in self.samples if self._is_tumor_sample(sample)]

    @property
    def normal_samples(self) -> List[JsonObject]:
        return [sample for sample in self.samples if self._is_normal_sample(sample)]

    @property
    @lru_cache()
    def tumor_sample_inputs(self) -> List[InputPropertiesFromSample]:
        return [InputPropertiesFromSample(sample) for sample in self.tumor_samples]

    @property
    @lru_cache()
    def normal_sample_inputs(self) -> List[InputPropertiesFromSample]:
        return [InputPropertiesFromSample(sample) for sample in self.normal_samples]

    @property
    @lru_cache()
    def all_sample_inputs(self) -> List[InputPropertiesFromSample]:
        return self.tumor_sample_inputs + self.normal_sample_inputs

    @property
    def input_normal_bam(self) -> List[List[str]]:
        normal_bams = self._get_input_properties_from_normal_samples("input_bams")
        if len(normal_bams) != 1:
            raise MetaWorkflowRunCreationError
        return normal_bams

    @property
    def input_tumor_bam(self) -> List[List[str]]:
        tumor_bams = self._get_input_properties_from_tumor_samples("input_bams")
        if len(tumor_bams) != 1:
            raise MetaWorkflowRunCreationError
        return tumor_bams

    @property
    def sex(self) -> str:
        return self.individual.get(self.SEX, "")

    @property
    def tumor_sample_name(self) -> str:
        tumor_sample_names = self._get_input_properties_from_tumor_samples(
            "sample_names"
        )
        unique_sample_names = [name for name in set(tumor_sample_names) if name]
        if len(unique_sample_names) != 1:
            raise MetaWorkflowRunCreationError(
                "Could not identify only 1 tumor sample name"
            )
        return unique_sample_names[0]

    @property
    def normal_sample_name(self) -> str:
        normal_sample_names = self._get_input_properties_from_normal_samples(
            "sample_names"
        )
        unique_sample_names = [name for name in set(normal_sample_names) if name]
        if len(unique_sample_names) != 1:
            raise MetaWorkflowRunCreationError(
                "Could not identify only 1 normal sample name"
            )
        return unique_sample_names[0]

    @property
    def input_somatic_vcf(self) -> List[List[str]]:
        requirements = {self.FILE_TYPE: self.TNSCOPE_FILE_TYPE}
        processed_files = self.somatic_analysis.get(self.PROCESSED_FILES, [])
        result = get_files_for_file_formats(
            processed_files, self.FILE_FORMAT_VCF_GZ, requirements=requirements
        )
        if not result:
            raise MetaWorkflowRunCreationError(
                f"No file with acceptable VCF file format meeting requirements found on"
                f" SomaticAnalysis: {self.somatic_analysis}"
            )
        return [result]
