"""Tests for the somatic SNV filtering v2 input builder.

`mwfr_somatic_snv_filtering_v2` assembles eight parallel arrays -- six core id /
tissue descriptor / platform parameters alongside the file arguments they label
position by position -- so a reordering or an off-by-one mislabels calls without
changing any shape. The one defence against that is a known-good run.

The fixture under `files/mwfr_from_input/SNV_filtering_longcallD_GRCh38_v2/` is the
MetaWorkflowRun SMAMRQQHRKLW, posted from this pipeline for real. Its input is read
back here twice over: to build the portal responses the function is driven with, and
to compare what the function produces against what was actually posted.

That makes this a round trip, not an independent oracle -- it cannot tell whether the
posted run was right. What it does pin is the mapping from portal data to payload:
which MWFR tag each file is looked up under, which array labels which file argument,
and the order and `dimension` of every entry.
"""

import contextlib
import copy
import json
import os

import mock
import pytest

import magma_smaht.create_metawfr_variant_calling as variant_calling_module
import magma_smaht.utils as utils_module

CASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "files",
    "mwfr_from_input",
    "SNV_filtering_longcallD_GRCh38_v2",
)

AUTH_KEY = {"key": "mocked", "secret": "out", "server": "https://example.org"}

TISSUE_ACCESSION = "SMATIEQQW8PX"
TISSUE_CODE = "SMHT005-3AK"
DONOR_CODE = "SMHT005"
ANALYSIS_RUN = "SMAARMOCKED1"

# `input_files_core_ids_all_long_read` in SMAMRQQHRKLW holds the "XX" core
# at these two indices, and nowhere else in any core id array. The builder resolves
# them from the file's samples, so the payload can no longer match the recording
# there -- see test_long_read_core_ids_resolve_XX.
RESOLVED_LONG_READ_CORES = {3: "001R1", 4: "001R2"}

PLATFORM_BY_LABEL = {"PB": "PacBio Revio", "ONT": "ONT PromethION 24"}


def load_posted_input():
    """The `input` of SMAMRQQHRKLW, by argument name."""
    with open(os.path.join(CASE_DIR, "expected_mwfr.json")) as json_file:
        posted = json.load(json_file)
    return {argument["argument_name"]: argument for argument in posted["input"]}


def load_metaworkflow():
    with open(os.path.join(CASE_DIR, "metaworkflow.json")) as json_file:
        return json.load(json_file)


def file_uuids(posted_input, argument_name):
    return [file["file"] for file in posted_input[argument_name]["files"]]


def annotated_filename(tissue_descriptor, core, accession):
    r"""A full annotated filename, in the layout the parser expects.

        SMHT019-3I-002D4-F78-A001-uwsc-SMAFIV8NKK4D-sentieon_bwamem_...cram
        \_______/ \___/                \__________/
         sample     core                 accession

    The tissue descriptor is itself `<donor>-<tissue>`, so it fills the first two
    fields. The three fields between the core and the accession are never read.
    """
    return "{0}-{1}-F78-A001-uwsc-{2}-sentieon_bwamem_GRCh38.aligned.cram".format(
        tissue_descriptor, core, accession
    )


def file_item(
    uuid, tissue_descriptor, core, platform=None, accession=None, samples=None
):
    """A file item carrying an annotated filename the sort and the parser can read.

    The display title is carried too but is only ever printed -- keeping the core
    out of it is what pins that the core is read off the annotated filename.
    """
    accession = accession or "SMAFI{0}".format(uuid[:6])
    item = {
        "uuid": uuid,
        "accession": accession,
        "annotated_filename": annotated_filename(tissue_descriptor, core, accession),
        "display_title": "{0}.cram".format(accession),
        "sample_sources": [{"display_title": tissue_descriptor}],
    }
    if platform:
        item["data_generation_summary"] = {"sequencing_platforms": [platform]}
    if samples is not None:
        item["samples"] = samples
    return item


# The workflow whose output each caller argument is read from, and the parameter
# holding the core id of each of its entries.
CALLER_ARGUMENTS = (
    ("tnhaplotyper2", "sentieon_merge_TNfilter", "input_files_TNhaplotyper2_vcf_gz"),
    ("strelka2", "bcftools_concat@SNV", "input_files_Strelka2_vcf_gz"),
    ("strelka2", "bcftools_concat@Indel", "additional_files_vcf_gz"),
    ("rufus", "bcftools_concat", "input_files_RUFUS_vcf_gz"),
)


def build_portal(posted_input):
    """Portal responses that would have produced SMAMRQQHRKLW.

    Caller outputs are keyed by the (identifier, caller tag, workflow) triple
    `get_variant_calling_output` builds its search from, so the function has to ask
    under the right tag to find anything.
    """
    core_ids_short_read = posted_input["input_files_core_ids_sr"]["value"]
    # The builder sorts the Illumina lists by (sample, core, accession). This
    # recording is already sorted on both of the first two keys, so the sort is a
    # no-op here -- provided the fabricated accessions ascend with position, hence
    # the zero padded index ("SMAFI10" sorts before "SMAFI9" without it).
    # This fixture therefore cannot validate the sort; TestLabelsMatchTheirFiles does.
    tissue_files_illumina = [
        file_item(uuid, TISSUE_CODE, core, accession="SMAFI{0:04d}".format(index))
        for index, (uuid, core) in enumerate(
            zip(
                file_uuids(posted_input, "input_files_sr_cram_tissue_specific"),
                core_ids_short_read,
            )
        )
    ]
    # Only the tissue descriptors of the donor pooled short read files are used, the
    # core is never read off them -- hence the constant here.
    donor_files_illumina = [
        file_item(
            uuid,
            tissue_descriptor,
            core_ids_short_read[0],
            accession="SMAFI{0:04d}".format(index),
        )
        for index, (uuid, tissue_descriptor) in enumerate(
            zip(
                file_uuids(posted_input, "input_files_sr_cram_donor_pooled"),
                posted_input["input_files_tissue_descriptors_sr"]["value"],
            )
        )
    ]
    donor_files_pacbio = [
        file_item(uuid, TISSUE_CODE, core_ids_short_read[0], "PacBio Revio")
        for uuid in file_uuids(posted_input, "input_files_pb_cram_donor_pooled")
    ]
    # Two of the recorded long read files carry the "XX" core (see
    # RESOLVED_LONG_READ_CORES). Give them a sample so the builder can resolve it.
    samples_by_identifier = {}
    donor_files_long_read = []
    for index, (uuid, tissue_descriptor, core, label) in enumerate(
        zip(
            file_uuids(posted_input, "input_files_all_long_read_cram_donor_pooled"),
            posted_input["input_files_tissue_descriptors_all_long_read"]["value"],
            posted_input["input_files_core_ids_all_long_read"]["value"],
            posted_input["input_files_types_all_long_read"]["value"],
        )
    ):
        samples = None
        if core == utils_module.ANNOTATED_FILENAME_CORE_XX:
            sample_identifier = "sample-for-long-read-{0}".format(index)
            samples = [{"uuid": sample_identifier}]
            samples_by_identifier[sample_identifier] = {
                "external_id": "{0}-{1}".format(
                    tissue_descriptor, RESOLVED_LONG_READ_CORES[index]
                )
            }
        donor_files_long_read.append(
            file_item(
                uuid, tissue_descriptor, core, PLATFORM_BY_LABEL[label], samples=samples
            )
        )

    core_ids_pacbio = posted_input["input_files_core_ids_longcallD"]["value"]
    tissue_files_pacbio = [
        file_item("pacbio-tissue", TISSUE_CODE, core_ids_pacbio[0], "PacBio Revio")
    ]

    # The last entry of each caller array is the run over all files of the tissue,
    # labelled with the core ids joined by "-"; the rest are core specific.
    outputs = {}
    labels = posted_input["input_files_core_ids_TNhaplotyper2"]["value"]
    for caller, workflow_name, argument_name in CALLER_ARGUMENTS:
        for label, uuid in zip(labels, file_uuids(posted_input, argument_name)):
            tag = caller if "-" in label else "core_{0}_{1}".format(label, caller)
            outputs[(TISSUE_CODE, tag, workflow_name)] = {
                "uuid": uuid,
                "accession": "SMAFI{0}".format(uuid[:6]),
            }
    longcalld_uuid = file_uuids(posted_input, "input_files_longcallD_vcf_gz")[0]
    outputs[
        (
            TISSUE_CODE,
            "core_{0}_longcalld".format(core_ids_pacbio[0]),
            "longcallD_compress_index_single_cram",
        )
    ] = {"uuid": longcalld_uuid, "accession": "SMAFIlongcalld"}
    germline_uuid = file_uuids(posted_input, "germline_input_file_vcf_gz")[0]
    outputs[(DONOR_CODE, "dnascopehybrid", "sentieon_DNAscopeHybrid")] = {
        "uuid": germline_uuid,
        "accession": "SMAFIgermline",
    }

    def get_item_es(identifier, key, frame="raw"):
        # Explicit dispatch with no catch-all: a lookup this fake did not plan
        # for is a bug in the test, and saying so here beats handing back a
        # plausible Donor and failing somewhere downstream.
        if identifier == TISSUE_ACCESSION:
            return {"external_id": TISSUE_CODE, "donor": {"uuid": "donor-uuid"}}
        if identifier == "donor-uuid":
            return {"external_id": DONOR_CODE, "sex": "Male"}
        if identifier in samples_by_identifier:
            return samples_by_identifier[identifier]
        raise AssertionError(
            "unexpected get_item_es identifier: {0!r}".format(identifier)
        )

    def get_variant_calling_output(identifier, caller, workflow_name, argument, key):
        return outputs.get((identifier, caller, workflow_name))

    return {
        "get_item_es": get_item_es,
        "get_variant_calling_output": get_variant_calling_output,
        "get_existing_analysis_run": lambda *args: {"accession": ANALYSIS_RUN},
        "get_released_illumina_wgs_files_for_tissue": lambda *a: tissue_files_illumina,
        "get_released_pacbio_wgs_files_for_tissue": lambda *a: tissue_files_pacbio,
        "get_released_illumina_wgs_files_for_donor": lambda *a: donor_files_illumina,
        "get_released_pacbio_wgs_files_for_donor": lambda *a: donor_files_pacbio,
        "get_released_long_read_wgs_files_for_donor": lambda *a: donor_files_long_read,
    }


# Helpers that moved to magma_smaht.utils resolve their own collaborators from
# *utils'* globals, so patching the builder's module is not enough for those.
# `get_variant_calling_output` is called from both sides: directly by the builder
# for the germline vcf, and by `get_core_specific_caller_outputs` inside utils.
SHARED_WITH_UTILS = ("get_variant_calling_output", "get_item_es")


@contextlib.contextmanager
def patched_portal(patches):
    """Apply the portal fakes to every module that looks them up."""
    shared = {
        name: patches[name] for name in SHARED_WITH_UTILS if name in patches
    }
    with mock.patch.multiple(variant_calling_module, **patches):
        with mock.patch.multiple(utils_module, **shared):
            yield


@pytest.fixture
def built(capsys):
    """Run the builder against the mocked portal and return what it assembled.

    `create_and_validate_analysis_mwfr` is replaced rather than mocked away: it still
    builds the run through `mwfr_from_input`, only the server side validation and the
    POST are cut out.
    """
    posted_input = load_posted_input()
    metaworkflow = load_metaworkflow()
    captured = {}

    def create_and_validate(mwf_uuid, analysis_run, input_arg, mwfr_input, tag, key):
        captured["mwf_uuid"] = mwf_uuid
        captured["analysis_run"] = analysis_run
        captured["input"] = mwfr_input
        captured["tag"] = tag
        with mock.patch.object(utils_module, "get_item", return_value=metaworkflow):
            # Captured here rather than off post_analysis_mwfrs, so the tests hold
            # whether or not the POST at the end of the builder is enabled.
            captured["run"] = utils_module.mwfr_from_input(
                mwf_uuid, mwfr_input, input_arg, key
            )
        return captured["run"]

    def get_latest_mwf(name, key):
        captured["mwf_name"] = name
        return metaworkflow

    patches = build_portal(posted_input)
    patches["get_latest_mwf"] = get_latest_mwf
    patches["create_and_validate_analysis_mwfr"] = create_and_validate
    patches["post_analysis_mwfrs"] = lambda mwfrs, key: None

    with patched_portal(patches):
        variant_calling_module.mwfr_somatic_snv_filtering_v2(
            TISSUE_ACCESSION, None, AUTH_KEY
        )

    capsys.readouterr()
    captured["arguments"] = {
        argument["argument_name"]: argument for argument in captured["input"]
    }
    captured["posted_input"] = posted_input
    return captured


def test_argument_names_match_posted_run(built):
    """The v2 MetaWorkflow takes 22 arguments and all of them are supplied."""
    assert set(built["arguments"]) == set(built["posted_input"])


# The one argument the payload can no longer match entry for entry: the recording
# carries the "XX" core where the builder now resolves a real one.
RESOLVED_ARGUMENT = "input_files_core_ids_all_long_read"


@pytest.mark.parametrize(
    "argument_name", sorted(set(load_posted_input()) - {RESOLVED_ARGUMENT})
)
def test_argument_matches_posted_run(built, argument_name):
    """Every file list and every label array, entry for entry.

    Compared including `dimension` and in order: `additional_files_vcf_gz` is 2d
    (`"{index},0"`) while its sibling Strelka2 argument is flat, and the label arrays
    are positional.
    """
    generated = built["arguments"][argument_name]
    posted = built["posted_input"][argument_name]

    assert generated["argument_type"] == posted["argument_type"]
    if generated["argument_type"] == "file":
        assert [
            (file["file"], file.get("dimension")) for file in generated["files"]
        ] == [(file["file"], file.get("dimension")) for file in posted["files"]]
    else:
        assert generated["value"] == posted["value"]


def test_long_read_core_ids_resolve_XX(built):
    """The recorded array, with the XX entries replaced and nothing else.

    SMAMRQQHRKLW was posted before the resolution existed, so it carries "XX" at
    RESOLVED_LONG_READ_CORES' indices. Every other entry still has to match it
    exactly -- this is the recorded comparison for this argument, just with the two
    entries the change is about substituted.
    """
    posted = built["posted_input"][RESOLVED_ARGUMENT]["value"]
    expected = [
        RESOLVED_LONG_READ_CORES.get(index, core) for index, core in enumerate(posted)
    ]

    # Guard the fixture itself: if a re-capture ever removes the XX entries, this
    # test silently stops testing anything.
    assert set(RESOLVED_LONG_READ_CORES) == {
        index
        for index, core in enumerate(posted)
        if core == utils_module.ANNOTATED_FILENAME_CORE_XX
    }
    assert expected != posted

    assert built["arguments"][RESOLVED_ARGUMENT]["value"] == expected


def test_shard_graph_matches_posted_run(built):
    """The payload shards the way the posted run did.

    Three callers at 3 shards and longcallD at 1 -- the per-argument scatter the
    v1 pipeline could not express.
    """
    shards = {}
    for workflow_run in built["run"]["workflow_runs"]:
        shards.setdefault(workflow_run["name"], []).append(workflow_run["shard"])
    assert {name: sorted(s) for name, s in shards.items()} == {
        "bcftools_PASS_norm_dedup@TNhaplotyper2": ["0", "1", "2"],
        "bcftools_PASS_norm_dedup_extra_inputs@Strelka2": ["0", "1", "2"],
        "bcftools_PASS_norm_dedup@RUFUS": ["0", "1", "2"],
        "bcftools_PASS_norm_dedup@longcallD": ["0"],
        "snvs_step-1_filter_longcallD_v2": ["0"],
        "snvs_step-2_filter_v2": ["0"],
    }


def test_longcalld_metaworkflow_and_tag_are_selected(built):
    """With longcallD calls present the longcallD MetaWorkflow is the one asked for."""
    assert built["mwf_name"] == "SNV_filtering_longcallD_GRCh38_v2"
    assert built["mwf_uuid"] == load_metaworkflow()["uuid"]
    assert built["tag"] == "{0}_snv_filtering_v2".format(TISSUE_CODE)
    assert built["analysis_run"] == ANALYSIS_RUN


def test_without_longcalld_the_plain_metaworkflow_is_used(capsys):
    """No PacBio calls means no longcallD argument, no core ids for it, and the
    MetaWorkflow without the longcallD branch."""
    posted_input = load_posted_input()
    captured = {}

    def create_and_validate(mwf_uuid, analysis_run, input_arg, mwfr_input, tag, key):
        captured["mwf_uuid"] = mwf_uuid
        captured["input"] = mwfr_input
        return {"workflow_runs": []}

    def get_latest_mwf(name, key):
        captured["mwf_name"] = name
        return {"uuid": "mwf-uuid"}

    patches = build_portal(posted_input)
    patches["get_released_pacbio_wgs_files_for_tissue"] = lambda *a: []
    patches["get_latest_mwf"] = get_latest_mwf
    patches["create_and_validate_analysis_mwfr"] = create_and_validate
    patches["post_analysis_mwfrs"] = lambda mwfrs, key: None

    with patched_portal(patches):
        variant_calling_module.mwfr_somatic_snv_filtering_v2(
            TISSUE_ACCESSION, None, AUTH_KEY
        )
    capsys.readouterr()

    assert captured["mwf_name"] == "SNV_filtering_GRCh38_v2"
    names = {argument["argument_name"] for argument in captured["input"]}
    assert "input_files_longcallD_vcf_gz" not in names
    assert "input_files_core_ids_longcallD" not in names
    # Everything else is still supplied.
    assert names == set(posted_input) - {
        "input_files_longcallD_vcf_gz",
        "input_files_core_ids_longcallD",
    }


def test_missing_core_specific_run_is_fatal(capsys):
    """A core whose caller MWFR never completed stops the build.

    Skipping it would post a quietly smaller payload -- indistinguishable, from
    the MWFR alone, from a tissue that genuinely has fewer cores.
    """
    posted_input = load_posted_input()
    patches = build_portal(posted_input)
    inner_get_output = patches["get_variant_calling_output"]

    def get_variant_calling_output(identifier, caller, workflow_name, argument, key):
        if caller.startswith("core_001B3_"):
            return None
        return inner_get_output(identifier, caller, workflow_name, argument, key)

    patches["get_variant_calling_output"] = get_variant_calling_output
    patches["get_latest_mwf"] = lambda name, key: {"uuid": "mwf-uuid"}
    created = mock.Mock()
    patches["create_and_validate_analysis_mwfr"] = created
    patches["post_analysis_mwfrs"] = lambda mwfrs, key: None

    with patched_portal(patches):
        with pytest.raises(
            ValueError,
            match="No completed tnhaplotyper2 MWFR found for core 001B3",
        ):
            variant_calling_module.mwfr_somatic_snv_filtering_v2(
                TISSUE_ACCESSION, None, AUTH_KEY
            )
    # It fails before anything is assembled, so there is no partial payload.
    created.assert_not_called()
    capsys.readouterr()


def test_mislabelled_arrays_are_rejected():
    """The length check fires rather than posting silently mismatched labels."""
    posted_input = load_posted_input()
    patches = build_portal(posted_input)
    inner_labels = utils_module.get_long_read_labels

    def get_long_read_labels(files):
        tissue_labels, sequencer_labels = inner_labels(files)
        return tissue_labels[:-1], sequencer_labels

    patches["get_long_read_labels"] = get_long_read_labels
    patches["get_latest_mwf"] = lambda name, key: {"uuid": "mwf-uuid"}
    patches["create_and_validate_analysis_mwfr"] = mock.Mock()
    patches["post_analysis_mwfrs"] = mock.Mock()

    with patched_portal(patches):
        with pytest.raises(Exception, match="input_files_tissue_descriptors_all_long_read"):
            variant_calling_module.mwfr_somatic_snv_filtering_v2(
                TISSUE_ACCESSION, None, AUTH_KEY
            )


# --------------------------------------------------------------------------
# Ordering
# --------------------------------------------------------------------------
# The label arrays are positional: the filtering workflows read core id `i` as
# describing file `i`. The fixture above pins the ordering against one real run,
# which cannot distinguish "correct" from "consistently wrong", and its two
# short read cores happen to be alphabetical. What follows drives the builder
# with cores that are deliberately NOT alphabetical and with uuids that encode
# the core each file really belongs to, so a label can be checked against its
# own file rather than against a recording.

# Supplied in an order that is neither sorted nor grouped, with unequal file
# counts per core. The Illumina lists get sorted, so the short read cores come out
# sorted; the PacBio list does not, so its cores stay in first-seen order.
ORDER_SR_CORES = ("001B3", "001A2", "001C9")
# The core one tissue file only reveals through its samples. Chosen to sort into
# the middle of ORDER_SR_CORES, so a failure to resolve cannot pass by luck.
ORDER_RESOLVED_CORE = "001B7"
ORDER_SAMPLE_IDENTIFIER = "sample-for-XX-core"
ORDER_SR_CORES_SORTED = tuple(sorted(ORDER_SR_CORES + (ORDER_RESOLVED_CORE,)))
ORDER_SAMPLES = {
    ORDER_SAMPLE_IDENTIFIER: {
        "external_id": "{0}-{1}".format(TISSUE_CODE, ORDER_RESOLVED_CORE)
    }
}
ORDER_PB_CORES = ("001Z1", "001C4")

ORDER_CALLER_WORKFLOWS = {
    "tnhaplotyper2": "sentieon_merge_TNfilter",
    "strelka2": "bcftools_concat@SNV",
    "rufus": "bcftools_concat",
}
LONGCALLD_WORKFLOW = "longcallD_compress_index_single_cram"


def encoded(kind, core):
    """A file uuid that carries the core it belongs to."""
    return "{0}::{1}".format(kind, core)


def caller_output(kind, core):
    """A caller output file. The accession is only printed, the uuid carries the core."""
    uuid = encoded(kind, core)
    return {"uuid": uuid, "accession": "SMAFI{0}".format(abs(hash(uuid)) % 10 ** 6)}


def decoded(uuid):
    return uuid.split("::")[1]


def build_ordered_portal():
    """Portal responses whose every file announces its own core."""
    # Interleaved cores, and accessions descending within a core, so sorting has
    # to do real work on both the 2nd and the 3rd key.
    tissue_files_illumina = [
        file_item(
            encoded("srt", "{0}-{1}".format(core, index)),
            TISSUE_CODE,
            core,
            accession="SMAFI{0:04d}".format(9999 - index),
        )
        for index in range(2)
        for core, count in zip(ORDER_SR_CORES, (2, 1, 2))
        if index < count
    ]
    # One more tissue file whose filename "XX" instead of its
    # core. It has to sort, group and get labelled under ORDER_RESOLVED_CORE, which
    # only holds if all three call sites resolve it the same way.
    samples_by_identifier = dict(ORDER_SAMPLES)
    tissue_files_illumina.append(
        file_item(
            encoded("srt", "xx"),
            TISSUE_CODE,
            utils_module.ANNOTATED_FILENAME_CORE_XX,
            accession="SMAFI0500",
            samples=[{"uuid": ORDER_SAMPLE_IDENTIFIER}],
        )
    )
    tissue_files_pacbio = [
        file_item(encoded("pbt", core), TISSUE_CODE, core, "PacBio Revio")
        for core in ORDER_PB_CORES
    ]
    # Donor pooled files span samples and cores. Accessions are unique per file on
    # the portal, so the three keys together are a total order -- supplied here in
    # none of them.
    donor_files_illumina = [
        file_item(
            encoded("srd", "{0}-{1}-{2}".format(sample, core, accession_index)),
            sample,
            core,
            accession="SMAFI{0:04d}".format(accession_index),
        )
        for sample, core, accession_index in (
            ("SMHT005-3C", "001A2", 3),
            ("SMHT005-3A", "001B3", 1),
            ("SMHT005-3C", "001A2", 2),
            ("SMHT005-3A", "001A2", 4),
        )
    ]
    donor_files_pacbio = [
        file_item(encoded("pbd", "001A2"), TISSUE_CODE, "001A2", "PacBio Revio")
    ]
    donor_files_long_read = [
        file_item(encoded("lrd", core), tissue, core, PLATFORM_BY_LABEL[label])
        for core, tissue, label in (
            ("001Q1", "SMHT005-3A", "PB"),
            ("001R2", "SMHT005-3B", "ONT"),
            ("001S3", "SMHT005-3C", "PB"),
        )
    ]

    outputs = {}
    for caller, workflow_name in ORDER_CALLER_WORKFLOWS.items():
        for core in ORDER_SR_CORES_SORTED:
            outputs[
                (TISSUE_CODE, "core_{0}_{1}".format(core, caller), workflow_name)
            ] = caller_output(caller, core)
        outputs[(TISSUE_CODE, caller, workflow_name)] = caller_output(
            caller, "-".join(ORDER_SR_CORES_SORTED)
        )
    for core in ORDER_SR_CORES_SORTED:
        outputs[
            (TISSUE_CODE, "core_{0}_strelka2".format(core), "bcftools_concat@Indel")
        ] = caller_output("indel", core)
    outputs[(TISSUE_CODE, "strelka2", "bcftools_concat@Indel")] = caller_output(
        "indel", "-".join(ORDER_SR_CORES_SORTED)
    )
    for core in ORDER_PB_CORES:
        outputs[
            (TISSUE_CODE, "core_{0}_longcalld".format(core), LONGCALLD_WORKFLOW)
        ] = caller_output("longcalld", core)
    outputs[(TISSUE_CODE, "longcalld", LONGCALLD_WORKFLOW)] = caller_output(
        "longcalld", "-".join(ORDER_PB_CORES)
    )
    outputs[(DONOR_CODE, "dnascopehybrid", "sentieon_DNAscopeHybrid")] = caller_output(
        "germline", "none"
    )

    def get_item_es(identifier, key, frame="raw"):
        # Explicit dispatch with no catch-all: a lookup this fake did not plan
        # for is a bug in the test, and saying so here beats handing back a
        # plausible Donor and failing somewhere downstream.
        if identifier == TISSUE_ACCESSION:
            return {"external_id": TISSUE_CODE, "donor": {"uuid": "donor-uuid"}}
        if identifier == "donor-uuid":
            return {"external_id": DONOR_CODE, "sex": "Male"}
        if identifier in samples_by_identifier:
            return samples_by_identifier[identifier]
        raise AssertionError(
            "unexpected get_item_es identifier: {0!r}".format(identifier)
        )

    def get_variant_calling_output(identifier, caller, workflow_name, argument, key):
        return outputs.get((identifier, caller, workflow_name))

    files_by_uuid = {
        item["uuid"]: item
        for item in tissue_files_illumina
        + donor_files_illumina
        + donor_files_pacbio
        + donor_files_long_read
    }
    return files_by_uuid, {
        "get_item_es": get_item_es,
        "get_variant_calling_output": get_variant_calling_output,
        "get_existing_analysis_run": lambda *args: {"accession": ANALYSIS_RUN},
        "get_released_illumina_wgs_files_for_tissue": lambda *a: tissue_files_illumina,
        "get_released_pacbio_wgs_files_for_tissue": lambda *a: tissue_files_pacbio,
        "get_released_illumina_wgs_files_for_donor": lambda *a: donor_files_illumina,
        "get_released_pacbio_wgs_files_for_donor": lambda *a: donor_files_pacbio,
        "get_released_long_read_wgs_files_for_donor": lambda *a: donor_files_long_read,
        "get_latest_mwf": lambda name, key: {"uuid": "mwf-uuid"},
        "post_analysis_mwfrs": lambda mwfrs, key: None,
    }


def run_ordered(capsys, missing_caller_tag=None, reverse_illumina=False):
    """Build the payload against the self-describing portal."""
    files_by_uuid, patches = build_ordered_portal()
    captured = {}
    if reverse_illumina:
        for name in (
            "get_released_illumina_wgs_files_for_tissue",
            "get_released_illumina_wgs_files_for_donor",
        ):
            inner_files = patches[name]()
            patches[name] = lambda *a, _f=list(reversed(inner_files)): _f
    if missing_caller_tag:
        inner = patches["get_variant_calling_output"]

        def get_variant_calling_output(identifier, caller, workflow_name, argument, key):
            if caller == missing_caller_tag:
                return None
            return inner(identifier, caller, workflow_name, argument, key)

        patches["get_variant_calling_output"] = get_variant_calling_output

    patches["create_and_validate_analysis_mwfr"] = (
        lambda mwf_uuid, ar, input_arg, mwfr_input, tag, key: captured.setdefault(
            "input", mwfr_input
        )
        or {"workflow_runs": []}
    )
    with patched_portal(patches):
        variant_calling_module.mwfr_somatic_snv_filtering_v2(
            TISSUE_ACCESSION, None, AUTH_KEY
        )
    capsys.readouterr()
    return files_by_uuid, {
        argument["argument_name"]: argument for argument in captured["input"]
    }


def posted_caller_tags(capsys):
    """The MWFR tags the caller step posts for the self-describing portal.

    The AnalysisRun is supplied so `post_analysis_run` is never reached.
    """
    _, patches = build_ordered_portal()
    tags = []
    patches["create_and_validate_analysis_mwfr"] = (
        lambda mwf_uuid, ar, input_arg, mwfr_input, tag, key: tags.append(tag)
        or {"workflow_runs": []}
    )
    with patched_portal(patches):
        variant_calling_module.mwfrs_somatic_snv_callers_by_core(
            TISSUE_ACCESSION, ANALYSIS_RUN, AUTH_KEY
        )
    capsys.readouterr()
    return tags


def requested_caller_tags(capsys):
    """The MWFR tags the filtering step looks its caller outputs up under.

    Recorded off the search rather than re-derived from the cores, so this is what
    the builder actually asked for. `get_variant_calling_output` takes the tissue
    apart from the caller and joins them into the tag itself, so the two are joined
    back here -- what comes out is directly comparable to a posted tag.
    """
    _, patches = build_ordered_portal()
    inner = patches["get_variant_calling_output"]
    tags = []

    def get_variant_calling_output(identifier, caller, workflow_name, argument, key):
        tags.append("{0}_{1}".format(identifier, caller))
        return inner(identifier, caller, workflow_name, argument, key)

    patches["get_variant_calling_output"] = get_variant_calling_output
    patches["create_and_validate_analysis_mwfr"] = (
        lambda mwf_uuid, ar, input_arg, mwfr_input, tag, key: {"workflow_runs": []}
    )
    with patched_portal(patches):
        variant_calling_module.mwfr_somatic_snv_filtering_v2(
            TISSUE_ACCESSION, None, AUTH_KEY
        )
    capsys.readouterr()
    return tags


def test_the_caller_step_posts_the_tags_the_filtering_step_looks_up(capsys):
    """The two steps agree on the sequencing core of a file.

    The caller step turns a file's core into the MWFR tag
    `{tissue}_core_{core}_{caller}` and the filtering step searches for that tag.
    Nothing in the payload can recover from the two disagreeing, and reading the core
    through the same parser on both sides is what makes them agree by construction.

    The combined `{tissue}_{caller}` tags the filtering step also asks for come from
    `mwfrs_somatic_snv_callers`, over all files of the tissue, so only the per-core
    tags cross between these two builders.
    """
    posted = posted_caller_tags(capsys)
    requested = requested_caller_tags(capsys)

    per_core_requested = {tag for tag in requested if "_core_" in tag}
    assert per_core_requested
    assert per_core_requested <= set(posted)

    # The file whose filename carries "XX" is tagged with the core its samples give,
    # which is the only core the filtering step will ever ask for.
    assert (
        "{0}_core_{1}_tnhaplotyper2".format(TISSUE_CODE, ORDER_RESOLVED_CORE) in posted
    )
    assert not [tag for tag in posted if utils_module.ANNOTATED_FILENAME_CORE_XX in tag]

    # The Illumina files are sorted before they are grouped, so the cores come out
    # sorted rather than in whatever order the search returned. Only the tags above
    # have to match; this is what keeps a re-run of one tissue posting the same
    # MWFRs with the same files in the same shards.
    posted_short_read_cores = [
        tag.split("_core_")[1].rsplit("_", 1)[0]
        for tag in posted
        if tag.endswith("_tnhaplotyper2")
    ]
    assert posted_short_read_cores == sorted(ORDER_SR_CORES_SORTED)


# (file argument, label argument, how the file's own truth is read).
# additional_files_vcf_gz has no core id array of its own: the Strelka2 step
# scatters over it and its sibling together, so shard i pairs indel i with SNV i
# and the Strelka2 core ids label both.
CALLER_PAIRS = (
    ("input_files_TNhaplotyper2_vcf_gz", "input_files_core_ids_TNhaplotyper2"),
    ("input_files_Strelka2_vcf_gz", "input_files_core_ids_Strelka2"),
    ("additional_files_vcf_gz", "input_files_core_ids_Strelka2"),
    ("input_files_RUFUS_vcf_gz", "input_files_core_ids_RUFUS"),
    ("input_files_longcallD_vcf_gz", "input_files_core_ids_longcallD"),
)

CRAM_PAIRS = (
    ("input_files_sr_cram_tissue_specific", "input_files_core_ids_sr", "core"),
    ("input_files_all_long_read_cram_donor_pooled", "input_files_core_ids_all_long_read", "core"),
    ("input_files_all_long_read_cram_donor_pooled", "input_files_tissue_descriptors_all_long_read", "tissue"),
    ("input_files_all_long_read_cram_donor_pooled", "input_files_types_all_long_read", "platform"),
    ("input_files_sr_cram_donor_pooled", "input_files_tissue_descriptors_sr", "tissue"),
)


def truth_for(file_item_, kind):
    if kind == "core":
        core = file_item_["annotated_filename"].split("-")[2]
        if core == utils_module.ANNOTATED_FILENAME_CORE_XX:
            # Re-derived here rather than reused from production, so the two can
            # be compared: sample external_id -> third dash separated field.
            external_id = ORDER_SAMPLES[file_item_["samples"][0]["uuid"]][
                "external_id"
            ]
            return external_id.split("-")[2]
        return core
    if kind == "tissue":
        return file_item_["sample_sources"][0]["display_title"]
    return PLATFORM_BY_LABEL_INVERSE[
        file_item_["data_generation_summary"]["sequencing_platforms"][0]
    ]


PLATFORM_BY_LABEL_INVERSE = {
    platform: label for label, platform in PLATFORM_BY_LABEL.items()
}


class TestLabelsMatchTheirFiles:
    """Each label describes the file at its own index, not a neighbour's."""

    @pytest.mark.parametrize("file_argument,label_argument", CALLER_PAIRS)
    def test_caller_core_ids(self, capsys, file_argument, label_argument):
        _, arguments = run_ordered(capsys)
        files = arguments[file_argument]["files"]
        labels = arguments[label_argument]["value"]

        assert len(files) == len(labels)
        assert [decoded(file["file"]) for file in files] == labels

    @pytest.mark.parametrize("file_argument,label_argument,kind", CRAM_PAIRS)
    def test_cram_labels(self, capsys, file_argument, label_argument, kind):
        files_by_uuid, arguments = run_ordered(capsys)
        files = arguments[file_argument]["files"]
        labels = arguments[label_argument]["value"]

        assert len(files) == len(labels)
        assert [
            truth_for(files_by_uuid[file["file"]], kind) for file in files
        ] == labels

    @pytest.mark.parametrize(
        "file_argument,suffix",
        [
            pytest.param("input_files_TNhaplotyper2_vcf_gz", "", id="tnhaplotyper2"),
            pytest.param("input_files_Strelka2_vcf_gz", "", id="strelka2"),
            pytest.param("additional_files_vcf_gz", ",0", id="strelka2_indel_2d"),
            pytest.param("input_files_RUFUS_vcf_gz", "", id="rufus"),
            pytest.param("input_files_longcallD_vcf_gz", "", id="longcalld"),
            pytest.param("input_files_sr_cram_tissue_specific", "", id="sr_tissue"),
            pytest.param("input_files_sr_cram_donor_pooled", "", id="sr_donor"),
            pytest.param("input_files_pb_cram_donor_pooled", "", id="pb_donor"),
            pytest.param("input_files_all_long_read_cram_donor_pooled", "", id="long_read"),
        ],
    )
    def test_dimension_equals_list_position(self, capsys, file_argument, suffix):
        """Position and `dimension` have to agree.

        The labels are consumed by position while ParserFF places each file at the
        index its `dimension` names. A permutation would satisfy
        `generate_input_structure` -- it only checks the indices are complete --
        and then pair every label with the wrong file.
        """
        _, arguments = run_ordered(capsys)
        assert [
            file.get("dimension") for file in arguments[file_argument]["files"]
        ] == [
            "{0}{1}".format(index, suffix)
            for index in range(len(arguments[file_argument]["files"]))
        ]

    def test_short_read_cores_are_sorted_but_pacbio_is_not(self, capsys):
        """Both halves of the "Illumina only" decision, in one test.

        The short read cores are grouped off the sorted Illumina list, so they come
        out sorted and the combined label reads in sorted order too. The PacBio list
        is not sorted, so its cores stay in the order the portal returned them --
        supplied here as 001Z1 before 001C4.
        """
        _, arguments = run_ordered(capsys)
        assert arguments["input_files_core_ids_TNhaplotyper2"]["value"] == [
            "001A2",
            "001B3",
            "001B7",
            "001C9",
            "001A2-001B3-001B7-001C9",
        ]
        assert arguments["input_files_core_ids_longcallD"]["value"] == [
            "001Z1",
            "001C4",
            "001Z1-001C4",
        ]

    def test_XX_core_is_resolved_everywhere(self, capsys):
        """One file reveals its core only through its samples.

        The payoff of the whole change: 001B7 has to appear as a core of its own,
        sorted into the middle rather than clustered under "XX", and it
        has to be the same value at all three call sites -- the sort, the grouping
        and the label array. No "XX" may appear anywhere.
        """
        _, arguments = run_ordered(capsys)

        assert ORDER_RESOLVED_CORE in arguments["input_files_core_ids_sr"]["value"]
        assert ORDER_RESOLVED_CORE in arguments["input_files_core_ids_TNhaplotyper2"]["value"]
        for name, argument in arguments.items():
            if argument["argument_type"] == "parameter" and "core_ids" in name:
                assert utils_module.ANNOTATED_FILENAME_CORE_XX not in (
                    argument["value"]
                ), name

        # Sorted into position, not appended: 001B7 sits between 001B3 and 001C9.
        assert arguments["input_files_core_ids_sr"]["value"] == [
            "001A2",
            "001B3",
            "001B3",
            "001B7",
            "001C9",
            "001C9",
        ]

    def test_illumina_files_are_sorted_by_sample_core_accession(self, capsys):
        """The tissue crams come out in the order the sort key defines.

        The portal returns them interleaved by core and descending by accession
        (see build_ordered_portal), so a missing sort, a wrong key order or a
        dropped accession key all show up here.
        """
        files_by_uuid, arguments = run_ordered(capsys)
        files = arguments["input_files_sr_cram_tissue_specific"]["files"]
        keys = [
            utils_module.get_sample_core_accession(
                files_by_uuid[file["file"]], AUTH_KEY
            )
            for file in files
        ]
        assert keys == sorted(keys)
        # All one sample here, so the core is what orders it, accession within.
        # 001B7 is the core resolved from "XX", ordered by its real value.
        assert [core for _, core, _ in keys] == [
            "001A2",
            "001B3",
            "001B3",
            "001B7",
            "001C9",
            "001C9",
        ]
        assert [accession for _, _, accession in keys] == [
            "SMAFI9999",
            "SMAFI9998",
            "SMAFI9999",
            "SMAFI0500",
            "SMAFI9998",
            "SMAFI9999",
        ]

    def test_donor_pooled_files_are_sorted_by_sample(self, capsys):
        """The donor pooled crams span samples, so the 1st key orders them."""
        files_by_uuid, arguments = run_ordered(capsys)
        keys = [
            utils_module.get_sample_core_accession(
                files_by_uuid[file["file"]], AUTH_KEY
            )
            for file in arguments["input_files_sr_cram_donor_pooled"]["files"]
        ]
        assert keys == sorted(keys)

    def test_payload_does_not_depend_on_the_search_order(self, capsys):
        """Reversing what the portal returns changes nothing.

        The property that actually matters: the payload is a function of the files,
        not of the order the search happened to yield them in.
        """
        _, forward = run_ordered(capsys)
        _, reversed_ = run_ordered(capsys, reverse_illumina=True)
        assert forward == reversed_

    @pytest.mark.parametrize(
        "missing_tag,expected",
        [
            pytest.param(
                "core_001A2_tnhaplotyper2",
                "No completed tnhaplotyper2 MWFR found for core 001A2",
                id="one_core",
            ),
            pytest.param(
                "rufus",
                "No completed rufus MWFR found over all files",
                id="the_combined_run",
            ),
        ],
    )
    def test_a_missing_caller_output_raises(self, capsys, missing_tag, expected):
        """Neither a per-core run nor the run over all files may be missing.

        The label arrays are positional, so a gap in either one silently shortens
        every array that pairs with it.
        """
        with pytest.raises(ValueError, match=expected):
            run_ordered(capsys, missing_caller_tag=missing_tag)
        capsys.readouterr()

    def test_a_missing_indel_output_raises_too(self, capsys):
        """The indel VCFs ride on the Strelka2 core ids, so a gap in them is fatal.

        Dropping the indel output for one core alone would leave
        additional_files_vcf_gz a shard short of the argument labelling it. Since
        `get_core_specific_caller_outputs` raises, this is caught there rather
        than by the SNV/Indel cross check in the builder.
        """
        files_by_uuid, patches = build_ordered_portal()
        inner = patches["get_variant_calling_output"]

        def get_variant_calling_output(identifier, caller, workflow_name, argument, key):
            if workflow_name == "bcftools_concat@Indel" and caller.startswith(
                "core_001A2_"
            ):
                return None
            return inner(identifier, caller, workflow_name, argument, key)

        patches["get_variant_calling_output"] = get_variant_calling_output
        patches["create_and_validate_analysis_mwfr"] = mock.Mock()

        with patched_portal(patches):
            with pytest.raises(
                ValueError,
                match="No completed strelka2 MWFR found for core 001A2",
            ):
                variant_calling_module.mwfr_somatic_snv_filtering_v2(
                    TISSUE_ACCESSION, None, AUTH_KEY
                )
        capsys.readouterr()


class TestCoreFromAnnotatedFilename:
    """The core id is the third dash separated field of the annotated filename."""

    @pytest.mark.parametrize(
        "annotated_filename,expected",
        [
            pytest.param(
                annotated_filename("SMHT005-3AK", "001A2", "SMAFI1"),
                "001A2",
                id="illumina",
            ),
            pytest.param(
                "SMHT005-3AK-001A2-a-b-c-SMAFI1-d-e-f", "001A2", id="extra_fields"
            ),
        ],
    )
    def test_get_core_from_annotated_filename(self, annotated_filename, expected):
        assert (
            utils_module.get_core_from_annotated_filename(
                {"annotated_filename": annotated_filename}, AUTH_KEY
            )
            == expected
        )

    @pytest.mark.parametrize(
        "file_item",
        [
            pytest.param({"accession": "SMAFIBAD"}, id="property_absent"),
            pytest.param(
                {"accession": "SMAFIBAD", "annotated_filename": None},
                id="property_null",
            ),
            pytest.param(
                {"accession": "SMAFIBAD", "annotated_filename": ""}, id="empty"
            ),
            pytest.param(
                {"accession": "SMAFIBAD", "annotated_filename": "SMHT005-3AK"},
                id="too_few_fields",
            ),
            pytest.param(
                {
                    "accession": "SMAFIBAD",
                    "display_title": "SMHT005-3AK-001A2-x.cram",
                },
                id="display_title_is_not_a_fallback",
            ),
        ],
    )
    def test_unidentifiable_core_raises(self, file_item):
        """No core at all: grouping unrelated files together under one would
        mislabel every call derived from them."""
        with pytest.raises(ValueError, match="at least 7 are expected"):
            utils_module.get_core_from_annotated_filename(file_item, AUTH_KEY)

    def test_error_names_the_file(self):
        """The message has to identify which file to go and fix."""
        with pytest.raises(ValueError, match="SMAFI12345"):
            utils_module.get_core_from_annotated_filename(
                {"accession": "SMAFI12345", "annotated_filename": "no-core"}, AUTH_KEY
            )

    def test_group_files_by_core_propagates_the_error(self):
        """One unidentifiable file fails the grouping rather than being pooled."""
        files = [
            {
                "accession": "SMAFI1",
                "annotated_filename": annotated_filename(
                    "SMHT005-3AK", "001A2", "SMAFI1"
                ),
            },
            {"annotated_filename": "unannotated.cram", "accession": "SMAFIBAD"},
        ]
        with pytest.raises(ValueError, match="SMAFIBAD"):
            utils_module.group_files_by_core(files, AUTH_KEY)

    def test_group_files_by_core_keeps_first_seen_order(self):
        """Grouping order decides the order of the per-core arrays."""
        files = [
            {
                "accession": accession,
                "annotated_filename": annotated_filename(
                    "SMHT005-3AK", core, accession
                ),
            }
            for core, accession in (
                ("001B3", "SMAFI1"),
                ("001A2", "SMAFI2"),
                ("001B3", "SMAFI3"),
            )
        ]
        grouped = utils_module.group_files_by_core(files, AUTH_KEY)
        assert list(grouped) == ["001B3", "001A2"]
        assert len(grouped["001B3"]) == 2


class TestXXCoreResolution:
    """Where the annotated filename says "XX", the core comes off the Samples."""

    XX_FILE = {
        "accession": "SMAFIPLACE",
        "annotated_filename": annotated_filename(
            "SMHT020-3AC", utils_module.ANNOTATED_FILENAME_CORE_XX, "SMAFI1"
        ),
    }

    @staticmethod
    def with_samples(samples, **extra):
        file_item_ = dict(TestXXCoreResolution.XX_FILE, **extra)
        if samples is not None:
            file_item_["samples"] = samples
        return file_item_

    @staticmethod
    def portal(items, calls=None):
        """A get_item_es fake that only answers for the sample items it was given."""

        def get_item_es(identifier, key, frame="raw"):
            if calls is not None:
                calls.append(identifier)
            if identifier not in items:
                raise AssertionError(
                    "unexpected get_item_es identifier: {0!r}".format(identifier)
                )
            return items[identifier]

        return mock.patch.object(utils_module, "get_item_es", get_item_es)

    # ---- the happy paths -------------------------------------------------

    def test_samples_as_embedded_objects(self):
        """The embedded frame shape: fetched by uuid when external_id is absent."""
        with self.portal({"s1": {"external_id": "SMHT020-3AC-001X"}}):
            assert (
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "s1"}]), AUTH_KEY
                )
                == "001X"
            )

    def test_several_samples_agreeing(self):
        with self.portal(
            {
                "s1": {"external_id": "SMHT020-3AC-001X"},
                "s2": {"external_id": "SMHT020-3AD-001X"},
            }
        ):
            assert (
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "s1"}, {"uuid": "s2"}]), AUTH_KEY
                )
                == "001X"
            )

    def test_longer_external_id_still_reads_field_three(self):
        with self.portal({"s1": {"external_id": "SMHT020-3AC-001X-extra-bits"}}):
            assert (
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "s1"}]), AUTH_KEY
                )
                == "001X"
            )

    # ---- a real core never costs a request -------------------------------

    def test_a_real_core_never_touches_the_portal(self):
        """The 84 file donor pooled list must not turn into 84 requests."""
        file_item_ = {
            "accession": "SMAFIOK",
            "annotated_filename": annotated_filename("SMHT020-3AC", "001A2", "SMAFI1"),
            "samples": [{"uuid": "s1"}],
        }
        with self.portal({}):  # any fetch raises
            assert (
                utils_module.get_core_from_annotated_filename(file_item_, AUTH_KEY)
                == "001A2"
            )
            assert utils_module.get_sample_core_accession(file_item_, AUTH_KEY) == (
                "SMHT020-3AC",
                "001A2",
                "SMAFI1",
            )

    # ---- the refusals ----------------------------------------------------

    @pytest.mark.parametrize(
        "samples",
        [
            pytest.param(None, id="property_absent"),
            pytest.param([], id="empty_list"),
            pytest.param(None, id="property_null"),
        ],
    )
    def test_no_samples_raises(self, samples):
        """The message has to name the file, so the reader knows which one to fix."""
        file_item_ = self.with_samples(samples)
        if samples is None:
            file_item_["samples"] = None
        with self.portal({}):
            with pytest.raises(ValueError, match="have 0 cores"):
                utils_module.get_core_from_annotated_filename(file_item_, AUTH_KEY)

    def test_disagreeing_samples_raise(self):
        """Naming the file and the cores is the whole point."""
        with self.portal(
            {
                "s1": {"external_id": "SMHT020-3AC-001X"},
                "s2": {"external_id": "SMHT020-3AD-002Y"},
            }
        ):
            with pytest.raises(ValueError) as raised:
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "s1"}, {"uuid": "s2"}]), AUTH_KEY
                )
        message = str(raised.value)
        assert "SMAFIPLACE" in message
        assert "001X" in message and "002Y" in message

    def test_sample_without_external_id_raises(self):
        with self.portal({"s1": {"accession": "SMASAMPLE"}}):
            with pytest.raises(ValueError, match="has no external_id"):
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "s1"}]), AUTH_KEY
                )

    # ---- memoization -----------------------------------------------------

    def test_the_sample_is_fetched_once_per_run(self):
        """Each file is asked for its core three times per build -- sort, group,
        label array -- and that must be one request, not three."""
        calls = []
        file_item_ = self.with_samples([{"uuid": "s1"}])
        with self.portal({"s1": {"external_id": "SMHT020-3AC-001X"}}, calls=calls):
            utils_module.sort_files_by_sample_core_accession([file_item_], AUTH_KEY)
            utils_module.group_files_by_core([file_item_], AUTH_KEY)
            utils_module.get_core_from_annotated_filename(file_item_, AUTH_KEY)
        assert calls == ["s1"]

    def test_distinct_samples_are_fetched_separately(self):
        calls = []
        with self.portal(
            {
                "s1": {"external_id": "SMHT020-3AC-001X"},
                "s2": {"external_id": "SMHT020-3AC-001X"},
            },
            calls=calls,
        ):
            utils_module.get_core_from_annotated_filename(
                self.with_samples([{"uuid": "s1"}, {"uuid": "s2"}]), AUTH_KEY
            )
        assert sorted(calls) == ["s1", "s2"]

    # These two reuse identifier "shared" with different external ids. Without the
    # autouse cache_clear in test/conftest.py, whichever runs second reads the
    # other's value and one of them fails -- on test order alone.
    def test_cache_isolation_first(self):
        with self.portal({"shared": {"external_id": "SMHT020-3AC-001AAA"}}):
            assert (
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "shared"}]), AUTH_KEY
                )
                == "001AAA"
            )

    def test_cache_isolation_second(self):
        with self.portal({"shared": {"external_id": "SMHT020-3AC-001BBB"}}):
            assert (
                utils_module.get_core_from_annotated_filename(
                    self.with_samples([{"uuid": "shared"}]), AUTH_KEY
                )
                == "001BBB"
            )


class TestSampleCoreAccession:
    """The three sort keys, read out of the annotated filename."""

    # The example from the request, verbatim.
    REAL = (
        "SMHT019-3I-002D4-F78-A001-uwsc-SMAFIV8NKK4D"
        "-sentieon_bwamem_202308.01_GRCh38.aligned.sorted.cram"
    )

    def test_real_filename(self):
        assert utils_module.get_sample_core_accession(
            {"annotated_filename": self.REAL}, AUTH_KEY
        ) == ("SMHT019-3I", "002D4", "SMAFIV8NKK4D")

    def test_core_agrees_with_the_core_parser(self):
        """Both parsers read field 3, so they must not drift apart."""
        file_item_ = {"annotated_filename": self.REAL}
        _, core, _ = utils_module.get_sample_core_accession(file_item_, AUTH_KEY)
        assert core == utils_module.get_core_from_annotated_filename(file_item_, AUTH_KEY)

    def test_trailing_dashes_are_ignored(self):
        """Only the first 7 fields are read; the tool suffix may hold dashes."""
        assert utils_module.get_sample_core_accession(
            {"annotated_filename": "D-T-CORE-a-b-c-SMAFI1-x-y-z.cram"}, AUTH_KEY
        ) == ("D-T", "CORE", "SMAFI1")

    @pytest.mark.parametrize(
        "annotated_filename",
        [
            pytest.param("SMHT005-3AK-001A2-x.cram", id="core_only_layout"),
            pytest.param("SMHT019-3I-002D4-F78-A001-uwsc", id="one_field_short"),
            pytest.param("", id="empty"),
        ],
    )
    def test_too_short_raises(self, annotated_filename):
        """No accession to sort by means no guess -- the build stops."""
        with pytest.raises(ValueError, match="at least 7 are expected"):
            utils_module.get_sample_core_accession(
                {"annotated_filename": annotated_filename, "accession": "SMAFI9"},
                AUTH_KEY,
            )

    def test_seven_fields_is_enough(self):
        """The boundary: 7 fields carry everything that is read."""
        assert utils_module.get_sample_core_accession(
            {"annotated_filename": "SMHT019-3I-002D4-F78-A001-uwsc-SMAFIV8NKK4D"},
            AUTH_KEY,
        ) == ("SMHT019-3I", "002D4", "SMAFIV8NKK4D")

    def test_error_names_the_file(self):
        with pytest.raises(ValueError, match="SMAFI12345"):
            utils_module.get_sample_core_accession(
                {"accession": "SMAFI12345", "annotated_filename": "too-short"}, AUTH_KEY
            )


class TestSortFilesBySampleCoreAccession:
    """Sample first, then core, then accession."""

    @staticmethod
    def named(sample, core, accession):
        return {
            "annotated_filename": annotated_filename(sample, core, accession),
            "accession": accession,
        }

    def order(self, files):
        return [
            utils_module.get_sample_core_accession(file, AUTH_KEY)
            for file in utils_module.sort_files_by_sample_core_accession(files, AUTH_KEY)
        ]

    def test_sample_takes_precedence_over_core(self):
        """A low core under a high sample still sorts last."""
        assert self.order(
            [
                self.named("SMHT005-3B", "001A1", "SMAFI0001"),
                self.named("SMHT005-3A", "001Z9", "SMAFI0002"),
            ]
        ) == [
            ("SMHT005-3A", "001Z9", "SMAFI0002"),
            ("SMHT005-3B", "001A1", "SMAFI0001"),
        ]

    def test_core_breaks_a_sample_tie(self):
        assert [
            core
            for _, core, _ in self.order(
                [
                    self.named("SMHT005-3A", "001C3", "SMAFI0001"),
                    self.named("SMHT005-3A", "001A1", "SMAFI0002"),
                    self.named("SMHT005-3A", "001B2", "SMAFI0003"),
                ]
            )
        ] == ["001A1", "001B2", "001C3"]

    def test_accession_breaks_a_sample_and_core_tie(self):
        assert [
            accession
            for _, _, accession in self.order(
                [
                    self.named("SMHT005-3A", "001A1", "SMAFI0003"),
                    self.named("SMHT005-3A", "001A1", "SMAFI0001"),
                    self.named("SMHT005-3A", "001A1", "SMAFI0002"),
                ]
            )
        ] == ["SMAFI0001", "SMAFI0002", "SMAFI0003"]

    def test_input_is_not_mutated(self):
        """`sorted` returns a new list -- callers may still hold the original."""
        files = [
            self.named("SMHT005-3B", "001A1", "SMAFI0001"),
            self.named("SMHT005-3A", "001A1", "SMAFI0002"),
        ]
        before = list(files)
        utils_module.sort_files_by_sample_core_accession(files, AUTH_KEY)
        assert files == before

    def test_one_unparseable_file_fails_the_sort(self):
        files = [
            self.named("SMHT005-3A", "001A1", "SMAFI0001"),
            {"annotated_filename": "too-short", "accession": "SMAFIBAD"},
        ]
        with pytest.raises(ValueError, match="SMAFIBAD"):
            utils_module.sort_files_by_sample_core_accession(files, AUTH_KEY)


class TestLongReadLabels:
    def test_platform_labels(self):
        files = [
            {
                "display_title": "SMHT005-3AK-001A2-x.cram",
                "sample_sources": [{"display_title": "SMHT005-3AK"}],
                "data_generation_summary": {"sequencing_platforms": ["PacBio Revio"]},
            },
            {
                "display_title": "SMHT005-3A-XX-x.cram",
                "sample_sources": [{"display_title": "SMHT005-3A"}],
                "data_generation_summary": {
                    "sequencing_platforms": ["ONT PromethION 24"]
                },
            },
        ]
        assert utils_module.get_long_read_labels(files) == (
            ["SMHT005-3AK", "SMHT005-3A"],
            ["PB", "ONT"],
        )

    @pytest.mark.parametrize(
        "platforms",
        [
            pytest.param([], id="none"),
            pytest.param(["PacBio Revio", "ONT PromethION 24"], id="two"),
            pytest.param(["Illumina NovaSeq X Plus"], id="short_read"),
        ],
    )
    def test_unexpected_platforms_raise(self, platforms):
        files = [
            {
                "display_title": "SMHT005-3AK-001A2-x.cram",
                "sample_sources": [{"display_title": "SMHT005-3AK"}],
                "data_generation_summary": {"sequencing_platforms": platforms},
            }
        ]
        with pytest.raises(Exception, match="unexpected sequencers"):
            utils_module.get_long_read_labels(files)

    def test_multiple_sample_sources_raise(self):
        files = [
            {
                "display_title": "SMHT005-3AK-001A2-x.cram",
                "sample_sources": [{"display_title": "a"}, {"display_title": "b"}],
            }
        ]
        with pytest.raises(Exception, match="sample sources, expected 1"):
            utils_module.get_tissue_labels(files)
