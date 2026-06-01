#!/usr/bin/env python3

################################################
#
#   Functions to create a MetaWorkflowRun
#
################################################

################################################
#   Libraries
################################################

import json, uuid
from dcicutils import ff_utils
from magma_smaht.utils import mwfr_from_input
import pprint

pp = pprint.PrettyPrinter(indent=2)

# magma
from magma_smaht.utils import (
    get_latest_mwf,
    get_mwfr_file_input_arg,
    get_mwfr_parameter_input_arg,
    get_item,
    get_item_es,
    get_illumina_wgs_filesets_for_tissue,
    get_pacbio_wgs_filesets_for_tissue,
    get_released_illumina_wgs_files_for_tissue,
    get_released_illumina_wgs_files_for_donor,
    get_released_pacbio_wgs_files_for_tissue,
    get_released_pacbio_wgs_files_for_donor,
    get_released_long_read_wgs_files_for_donor,
    get_released_ont_wgs_files_for_tissue,
    get_analysis_runs_from_tissue,
    get_variant_calling_output,
    get_tissue_from_external_id,
    post_analysis_run,
    warning_text,
)

from magma_smaht.constants import (
    MWF_NAME_LONGCALLD,
    MWF_NAME_LONGCALLD_SINGLE_FILE,
    MWF_NAME_SNV_FILTERING_LONGCALLD,
    MWF_NAME_SNV_FILTERING,
    MWF_NAME_SNIFFLES,
    MWF_NAME_SEVERUS,
    MWF_NAME_DELLY,
    MWF_NAME_DELLY_SR,
    MWF_NAME_STRELKA2,
    MWF_NAME_RUFUS,
    MWF_NAME_DNASCOPEHYBRID,
    MWF_NAME_TNHAPLOTYPER,
    INPUT_FILES_CRAM,
    INPUT_FILE_CRAM,
    ADDITIONAL_INPUT_FILES_CRAM,
    SAMPLE_NAME,
    PLATFORM,
    COMMON_FIELDS,
    UUID,
    SEQUENCING_CENTER,
    ANALYSIS_RUNS,
    META_WORKFLOW_RUN,
    ACCESSION,
    DISPLAY_TITLE,
    TAGS,
    SOMATIC_SNV_CALLING,
    SOMATIC_SNV_CALLING_CORE_SPECIFIC,
    SOMATIC_SV_CALLING,
    GERMLINE_SNV_CALLING,
    HMS_DAC_UUID
)


################################################
#   Variant calling
################################################

def mwfr_germline_snv_caller(donor_accession, tissue_accession, analysis_run, smaht_key):

    donor = get_item_es(donor_accession, smaht_key, frame="embedded")
    donor_code = donor["external_id"]

    if tissue_accession:
        tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
        tissue_code = tissue["external_id"]
        files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
        files_pacbio = get_released_pacbio_wgs_files_for_tissue(tissue_code, smaht_key)
        print(f"Using tissue {tissue_code} for germline SNV calling. {len(files_pacbio)} Pacbio files found. {len(files_illumina)} Illumina files found.")
    else:   
        # We need to find a tissue where we have Pacbio and Illumina data for the donor

        # We are search for data in the following tissue priority order
        # Heart LV, Muscle, Colon Desc, Colon Asc, Skin Calf, Cereb. Brain, Frontal Lobe Brain, Temp. Lobe Brain, Hipp. Brain L, Hipp. Brain R, Skin Abdomen, Adrenal Gland L, Aorta Abdominal, Adrenal Gland R, Testis Left, Ovary Left, Esophagus, Lung, Liver Sample, Buccal Swab, Blood Whole, Dermal Fibroblast
        tissue_priority_list = [
            "3S",
            "3AH",
            "3G",
            "3E",
            "3AD",
            "3AM",
            "3AK",
            "3AL",
            "3AN",
            "3AO",
            "3AF",
            "3K",
            "3O",
            "3M",
            "3U",
            "3Y",
            "3C",
            "3Q",
            "3I",
            "3B",
            "3A",
            "3AC",
        ]
        for tissue_suffix in tissue_priority_list:
            tissue_code = f"{donor_code}-{tissue_suffix}"
            files_illumina = get_released_illumina_wgs_files_for_tissue(
                tissue_code, smaht_key
            )
            files_pacbio = get_released_pacbio_wgs_files_for_tissue(
                tissue_code, smaht_key
            )
            if len(files_pacbio) > 0 and len(files_illumina) > 0:
                tissue = get_tissue_from_external_id(tissue_code, smaht_key)
                tissue_accession = tissue[ACCESSION]
                print(
                    f"Using tissue {tissue_code} for germline SNV calling. {len(files_pacbio)} Pacbio files found. {len(files_illumina)} Illumina files found."
                )
                break

    print("\nIllumina files to choose from:")
    for f in files_illumina:
        coverage = f["quality_metrics"][-1]["coverage"]
        print(f"- Illumina file: {f[DISPLAY_TITLE]}. Coverage: {coverage}X")

    files_with_coverage = [
        f
        for f in files_illumina
        if f.get("quality_metrics") and f["quality_metrics"][-1].get("coverage")
    ]
    if files_with_coverage:
        file_with_highest_coverage = max(
            files_with_coverage, key=lambda f: f["quality_metrics"][-1]["coverage"]
        )
    print("\nIllumina file with highest coverage to be used:")
    print(f"- Illumina file: {file_with_highest_coverage[DISPLAY_TITLE]}")
    files_illumina = [file_with_highest_coverage]

    print("\nPacbio files to be used:")
    for f in files_pacbio:
        print(f"- Pacbio file: {f[DISPLAY_TITLE]}")

    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"Using provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            GERMLINE_SNV_CALLING,
            f"Germline SNV Calling: {donor_code}",
            donors=[donor_accession],
            tissues=[tissue_accession],
            smaht_key=smaht_key,
        )
        print(f"Created AnalysisRun {analysis_run_accession}.")

    # Create inputs for DNAScopeHybrid MWFR
    illumina_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_illumina)
    ]
    pacbio_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_pacbio)
    ]
    mwfr_dnascopehybrid_input = [
        get_mwfr_file_input_arg("input_files_short_cram", illumina_crams),
        get_mwfr_file_input_arg("input_files_long_cram", pacbio_crams),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, donor_accession),
    ]

    mwf_dnascopehybrid = get_latest_mwf(MWF_NAME_DNASCOPEHYBRID, smaht_key)

    mwfrs_to_post = []

    print("Validating DNAScopeHybrid MWFR.")
    mwfr_dnascopehybrid = create_and_validate_analysis_mwfr(
        mwf_dnascopehybrid[UUID],
        analysis_run_accession,
        "input_files_short_cram",
        mwfr_dnascopehybrid_input,
        f"{donor_code}_dnascopehybrid",
        smaht_key,
    )
    mwfrs_to_post.append(mwfr_dnascopehybrid)
    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def mwfrs_somatic_snv_callers_by_core(tissue_accession, analysis_run, smaht_key):
    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_uuid = tissue["donor"][UUID]

    # Get all released short read WGS files for the tissue
    files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Illumina WGS files: {len(files_illumina)}")
    # Get all released Pacbio WGS files for the tissue
    files_pacbio = get_released_pacbio_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Pacbio WGS files: {len(files_pacbio)}")

    print("\nIllumina files associated with the tissue:")
    for f in files_illumina:
        print(f"- Illumina file: {f[DISPLAY_TITLE]}")

    print("\nPacbio files associated with the tissue:")
    for f in files_pacbio:
        print(f"- Pacbio file: {f[DISPLAY_TITLE]}")

    print("\nThey will be processed in the following groups:")

    def get_core_from_display_title(file_item):
        display_title = file_item.get(DISPLAY_TITLE, "")
        parts = display_title.split("-")
        if len(parts) >= 3:
            return parts[2]
        return "unknown"

    grouped_files_illumina = {}
    for f in files_illumina:
        core = get_core_from_display_title(f)
        grouped_files_illumina.setdefault(core, [])
        grouped_files_illumina[core].append(f)

    grouped_files_pacbio = {}
    for f in files_pacbio:
        core = get_core_from_display_title(f)
        grouped_files_pacbio.setdefault(core, [])
        grouped_files_pacbio[core].append(f)

    for core in grouped_files_illumina:
        print(f"\nCore {core} - Illumina files:")
        for f in grouped_files_illumina[core]:
            print(f"- {f[DISPLAY_TITLE]}")
    for core in grouped_files_pacbio:
        print(f"\nCore {core} - Pacbio files:")
        for f in grouped_files_pacbio[core]:
            print(f"- {f[DISPLAY_TITLE]}")

    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"\nUsing provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            SOMATIC_SNV_CALLING_CORE_SPECIFIC,
            f"Somatic SNV Calling (core specific): {tissue_code}",
            [donor_uuid],
            [tissue_accession],
            smaht_key,
        )
        print(f"\nCreated AnalysisRun {analysis_run_accession}.")

    mwfrs_to_post = []
    for core in grouped_files_illumina:
        core_specific_illumina = grouped_files_illumina[core]
        mwfr_tnhaplotyper = create_tnhaplotyper2_mwfr(
            core_specific_illumina, tissue_accession, f"{tissue_code}_core_{core}_tnhaplotyper2", analysis_run_accession, smaht_key
        )
        mwfrs_to_post.append(mwfr_tnhaplotyper)

        mwfr_strelka2 = create_strelka2_mwfr(
            core_specific_illumina, f"{tissue_code}_core_{core}_strelka2", analysis_run_accession, smaht_key
        )
        mwfrs_to_post.append(mwfr_strelka2)

        mwfr_rufus = create_rufus_mwfr(
            core_specific_illumina, f"{tissue_code}_core_{core}_rufus", analysis_run_accession, smaht_key
        )
        mwfrs_to_post.append(mwfr_rufus)

    for core in grouped_files_pacbio:
        core_specific_pacbio = grouped_files_pacbio[core]
        mwfr_longcalld = create_longcalld_mwfr(
            core_specific_pacbio,
            "--hifi",
            tissue_accession,
            f"{tissue_code}_core_{core}_longcalld",
            analysis_run_accession,
            smaht_key,
        )
        mwfrs_to_post.append(mwfr_longcalld)
    # pprint.pprint(mwfrs_to_post)
    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def mwfrs_somatic_snv_callers_by_analyte(tissue_accession, analysis_run, smaht_key):
    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_uuid = tissue["donor"][UUID]

    # Get all released short read WGS files for the tissue
    files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Illumina WGS files: {len(files_illumina)}")
    # Get all released Pacbio WGS files for the tissue
    files_pacbio = get_released_pacbio_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Pacbio WGS files: {len(files_pacbio)}")

    print("\nIllumina files associated with the tissue:")
    for f in files_illumina:
        print(f"- Illumina file: {f[DISPLAY_TITLE]}")

    print("\nPacbio files associated with the tissue:")
    for f in files_pacbio:
        print(f"- Pacbio file: {f[DISPLAY_TITLE]}")

    def get_analyte_accession(file_item):
        analytes = file_item.get("analytes", [])
        if len(analytes) != 1:
            raise ValueError(
                f"File {file_item.get(DISPLAY_TITLE)} has {len(analytes)} analytes, expected exactly 1"
            )
        analyte = get_item(analytes[0][UUID], smaht_key)
        return analyte[ACCESSION]

    grouped_files_illumina = {}
    for f in files_illumina:
        analyte = get_analyte_accession(f)
        grouped_files_illumina.setdefault(analyte, [])
        grouped_files_illumina[analyte].append(f)

    grouped_files_pacbio = {}
    for f in files_pacbio:
        analyte = get_analyte_accession(f)
        grouped_files_pacbio.setdefault(analyte, [])
        grouped_files_pacbio[analyte].append(f)

    print("\nThey will be processed in the following groups:")
    for analyte in grouped_files_illumina:
        print(f"\nAnalyte {analyte} - Illumina files:")
        for f in grouped_files_illumina[analyte]:
            print(f"- {f[DISPLAY_TITLE]}")
    for analyte in grouped_files_pacbio:
        print(f"\nAnalyte {analyte} - Pacbio files:")
        for f in grouped_files_pacbio[analyte]:
            print(f"- {f[DISPLAY_TITLE]}")

    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"\nUsing provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            SOMATIC_SNV_CALLING_CORE_SPECIFIC,
            f"Somatic SNV Calling (analyte specific): {tissue_code}",
            [donor_uuid],
            [tissue_accession],
            smaht_key,
        )
        print(f"\nCreated AnalysisRun {analysis_run_accession}.")

    mwfrs_to_post = []
    for analyte in grouped_files_illumina:
        analyte_specific_illumina = grouped_files_illumina[analyte]
        mwfr_tnhaplotyper = create_tnhaplotyper2_mwfr(
            analyte_specific_illumina, tissue_accession, f"{tissue_code}_analyte_{analyte}_tnhaplotyper2", analysis_run_accession, smaht_key
        )
        mwfrs_to_post.append(mwfr_tnhaplotyper)

        mwfr_strelka2 = create_strelka2_mwfr(
            analyte_specific_illumina, f"{tissue_code}_analyte_{analyte}_strelka2", analysis_run_accession, smaht_key
        )
        mwfrs_to_post.append(mwfr_strelka2)

        mwfr_rufus = create_rufus_mwfr(
            analyte_specific_illumina, f"{tissue_code}_analyte_{analyte}_rufus", analysis_run_accession, smaht_key
        )
        mwfrs_to_post.append(mwfr_rufus)

    for analyte in grouped_files_pacbio:
        analyte_specific_pacbio = grouped_files_pacbio[analyte]
        mwfr_longcalld = create_longcalld_mwfr(
            analyte_specific_pacbio,
            "--hifi",
            tissue_accession,
            f"{tissue_code}_analyte_{analyte}_longcalld",
            analysis_run_accession,
            smaht_key,
        )
        mwfrs_to_post.append(mwfr_longcalld)

    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def mwfrs_somatic_snv_callers(tissue_accession, analysis_run, smaht_key):

    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_uuid = tissue["donor"][UUID]

    # Get all released short read WGS files for the tissue
    files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Illumina WGS files: {len(files_illumina)}")
    # Get all released Pacbio WGS files for the tissue
    files_pacbio = get_released_pacbio_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Pacbio WGS files: {len(files_pacbio)}")

    file_ont = get_released_ont_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of ONT WGS files: {len(file_ont)}")

    # We are getting the filesets as well to double check if the numbers match
    file_sets_illumina_wgs = get_illumina_wgs_filesets_for_tissue(
        tissue_code, smaht_key
    )
    file_sets_pacbio_wgs = get_pacbio_wgs_filesets_for_tissue(tissue_code, smaht_key)

    if len(files_illumina) != len(file_sets_illumina_wgs):
        warning_msg = f"{warning_text('Warning:')} Number of released Illumina WGS files ({len(files_illumina)}) does not match number of Illumina WGS filesets ({len(file_sets_illumina_wgs)})."
        print(warning_msg)
        response = input("Do you want to continue? (yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            print("Operation cancelled.")
            return
    if len(files_pacbio) != len(file_sets_pacbio_wgs):
        warning_msg = f"{warning_text('Warning:')} Number of released Pacbio WGS files ({len(files_pacbio)}) does not match number of Pacbio WGS filesets ({len(file_sets_pacbio_wgs)})."
        print(warning_msg)
        response = input("Do you want to continue? (yes/no): ").strip().lower()
        if response not in ['yes', 'y']:
            print("Operation cancelled.")
            return

    print("\nIllumina files to be used:")
    for f in files_illumina:
        print(f"- Illumina file: {f[DISPLAY_TITLE]}")

    print("\nPacbio files to be used:")
    for f in files_pacbio:
        print(f"- Pacbio file: {f[DISPLAY_TITLE]}")

    print("\nONT files to be used:")
    for f in file_ont:
        print(f"- ONT file: {f[DISPLAY_TITLE]}")

    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"\nUsing provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            SOMATIC_SNV_CALLING,
            f"Somatic SNV Calling: {tissue_code}",
            [donor_uuid],
            [tissue_accession],
            smaht_key,
        )
        print(f"\nCreated AnalysisRun {analysis_run_accession}.")

    mwfrs_to_post = []

    mwfr_tnhaplotyper = create_tnhaplotyper2_mwfr(
        files_illumina,
        tissue_accession,
        f"{tissue_code}_tnhaplotyper2",
        analysis_run_accession,
        smaht_key,
    )
    mwfrs_to_post.append(mwfr_tnhaplotyper)

    mwfr_strelka2 = create_strelka2_mwfr(
        files_illumina, f"{tissue_code}_strelka2", analysis_run_accession, smaht_key
    )
    mwfrs_to_post.append(mwfr_strelka2)

    mwfr_rufus = create_rufus_mwfr(
        files_illumina, f"{tissue_code}_rufus", analysis_run_accession, smaht_key
    )
    mwfrs_to_post.append(mwfr_rufus)

    mwfr_longcalld = create_longcalld_mwfr(
        files_pacbio,
        "--hifi",
        tissue_accession,
        f"{tissue_code}_longcalld",
        analysis_run_accession,
        smaht_key,
    )
    if mwfr_longcalld:
        mwfrs_to_post.append(mwfr_longcalld)

    # mwfr_longcalld_ont = create_longcalld_mwfr(
    #     file_ont,
    #     "--ont",
    #     tissue_accession,
    #     f"{tissue_code}_longcalld_ont",
    #     analysis_run_accession,
    #     smaht_key,
    # )
    # if mwfr_longcalld_ont:
    #     mwfrs_to_post.append(mwfr_longcalld_ont)

    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def mwfr_somatic_snv_filtering(tissue_accession, analysis_run, smaht_key):

    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    donor = get_item_es(tissue["donor"][UUID], smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_code = donor["external_id"]
    analysis_run_accession = analysis_run

    if not analysis_run_accession:
        ar = get_analysis_runs_from_tissue(tissue_code, smaht_key)
        if ar and len(ar) == 1:
            analysis_run_accession = ar[0][ACCESSION]   
            print(f"\nUsing Analysis Run {analysis_run_accession} for tissue {tissue_code}.")
        else:
            raise Exception(f"Could not determine a unique analysis run for tissue {tissue_code}. Please provide an analysis run accession to the function.")

    print(f"\nGathering input data for somatic SNV filtering for tissue {tissue_code}.")

    sample_name = tissue_accession
    print(f"\nSample name: {sample_name} ({tissue_code})")
    print(f"\nDonor sex: {donor['sex']}")


    tnhaplotyper2_result = get_variant_calling_output(
        tissue_code, "tnhaplotyper2", "sentieon_merge_TNfilter", "output_file_vcf_gz", smaht_key
    )
    print(f"\nTNhaplotyper2 VCF: {tnhaplotyper2_result[ACCESSION]}")

    strelka2_result_snv = get_variant_calling_output(
        tissue_code, "strelka2", "bcftools_concat@SNV", "output_file_vcf_gz", smaht_key
    )
    print(f"Strelka2 SNV VCF: {strelka2_result_snv[ACCESSION]}")

    strelka2_result_indel = get_variant_calling_output(
        tissue_code, "strelka2", "bcftools_concat@Indel", "output_file_vcf_gz", smaht_key
    )
    print(f"Strelka2 Indel VCF: {strelka2_result_indel[ACCESSION]}")

    rufus_result = get_variant_calling_output(
        tissue_code, "rufus", "bcftools_concat", "output_file_vcf_gz", smaht_key
    )
    print(f"\nRufus VCF: {rufus_result[ACCESSION]}")

    longcalld_result_single_cram = get_variant_calling_output(
        tissue_code, "longcalld", "longcallD_compress_index_single_cram", "output_file_vcf_gz", smaht_key
    )
    longcalld_result_multi_cram = get_variant_calling_output(
        tissue_code, "longcalld", "longcallD_compress_index", "output_file_vcf_gz", smaht_key
    )
    longcalld_result = longcalld_result_single_cram or longcalld_result_multi_cram
    if longcalld_result:
        print(f"Longcalld VCF: {longcalld_result[ACCESSION]}")
    else:
        print("No longcalld result found. Filtering will proceed without it.")

    dnascopehybrid_result = get_variant_calling_output(
        donor_code, "dnascopehybrid", "sentieon_DNAscopeHybrid", "output_file_vcf_gz", smaht_key
    )
    print(f"\nDNAscopeHybrid germline calls for {donor_code}: {dnascopehybrid_result[ACCESSION]}")

    tissue_files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
    print("\nTissue specific Illumina files to be used:")
    for f in tissue_files_illumina:
        print(f" - File: {f[DISPLAY_TITLE]}")

    donor_files_illumina = get_released_illumina_wgs_files_for_donor(donor_code, smaht_key)
    print(f"\nDonor specific Illumina files to be used ({len(donor_files_illumina)}):")
    for f in donor_files_illumina:
        print(f" - File: {f[DISPLAY_TITLE]}")

    tissue_labels_short_read = []
    for f in donor_files_illumina:
        sample_sources = f.get("sample_sources", [])
        if len(sample_sources) != 1:
            raise Exception(f" - File: {f[DISPLAY_TITLE]} has {len(sample_sources)} sample sources, expected 1.")
        sample_source = sample_sources[0][DISPLAY_TITLE]
        tissue_labels_short_read.append(sample_source)

    donor_files_pacbio = get_released_pacbio_wgs_files_for_donor(donor_code, smaht_key)
    print(f"\nDonor specific PacBio files to be used ({len(donor_files_pacbio)}):")
    for f in donor_files_pacbio:
        print(f" - File: {f[DISPLAY_TITLE]}")

    donor_files_long_read = get_released_long_read_wgs_files_for_donor(donor_code, smaht_key)
    print(f"\nDonor specific long read files to be used ({len(donor_files_long_read)}):")
    for f in donor_files_long_read:
        print(f" - File: {f[DISPLAY_TITLE]}")

    # Matched tissue descriptors long read
    sequencer_to_label_mapping = {
        "ONT PromethION 24": "ONT",
        "PacBio Revio": "PB"
    }
    tissue_labels_long_read = []
    sequencer_labels_long_read = []
    for f in donor_files_long_read:
        sample_sources = f.get("sample_sources", [])
        if len(sample_sources) != 1:
            raise Exception(f" - File: {f[DISPLAY_TITLE]} has {len(sample_sources)} sample sources, expected 1.")
        sample_source = sample_sources[0][DISPLAY_TITLE]
        tissue_labels_long_read.append(sample_source)

        sequencers = f.get("data_generation_summary", {}).get("sequencing_platforms", [])
        if len(sequencers) != 1 or sequencers[0] not in sequencer_to_label_mapping.keys():
            raise Exception(f" - File: {f[DISPLAY_TITLE]} has unexpected sequencers, expected exactly one of PB or ONT.")
        sequencer = sequencers[0]
        sequencer_labels_long_read.append(sequencer_to_label_mapping[sequencer])

    # Compile input arguments for MWFR
    input_files_TNhaplotyper2_vcf_gz = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate([tnhaplotyper2_result])
    ]
    input_files_Strelka2_vcf_gz = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate([strelka2_result_snv])
    ]
    additional_files_vcf_gz = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate([strelka2_result_indel])
    ]
    input_files_RUFUS_vcf_gz = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate([rufus_result])
    ]
    germline_input_file_vcf_gz = [
        {"file": dnascopehybrid_result[UUID]}
    ]

    if longcalld_result:
        input_files_longcallD_vcf_gz = [
            {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate([longcalld_result])
        ]
    
    input_files_sr_cram_tissue_specific = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(tissue_files_illumina)
    ]
    input_files_sr_cram_donor_pooled = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(donor_files_illumina)
    ]
    input_files_pb_cram_donor_pooled = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(donor_files_pacbio)
    ]
    input_files_all_long_read_cram_donor_pooled = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(donor_files_long_read)
    ]

    mwfr_input = [
        get_mwfr_file_input_arg('input_files_TNhaplotyper2_vcf_gz', input_files_TNhaplotyper2_vcf_gz),
        get_mwfr_file_input_arg('input_files_Strelka2_vcf_gz', input_files_Strelka2_vcf_gz),
        get_mwfr_file_input_arg('input_files_RUFUS_vcf_gz', input_files_RUFUS_vcf_gz),
        get_mwfr_file_input_arg('additional_files_vcf_gz', additional_files_vcf_gz),
        get_mwfr_file_input_arg('germline_input_file_vcf_gz', germline_input_file_vcf_gz),
        get_mwfr_file_input_arg('input_files_sr_cram_tissue_specific', input_files_sr_cram_tissue_specific),
        get_mwfr_file_input_arg('input_files_sr_cram_donor_pooled', input_files_sr_cram_donor_pooled),
        get_mwfr_file_input_arg('input_files_pb_cram_donor_pooled', input_files_pb_cram_donor_pooled),
        get_mwfr_file_input_arg('input_files_all_long_read_cram_donor_pooled', input_files_all_long_read_cram_donor_pooled),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, tissue_accession),
        get_mwfr_parameter_input_arg('input_files_tissue_descriptors_sr', tissue_labels_short_read),
        get_mwfr_parameter_input_arg('input_files_tissue_descriptors_all_long_read', tissue_labels_long_read),
        get_mwfr_parameter_input_arg('input_files_types_all_long_read', sequencer_labels_long_read),
        get_mwfr_parameter_input_arg('sex', donor['sex'].lower()),
        get_mwfr_parameter_input_arg('current_tissue', tissue_code),
    ]
    if longcalld_result:
        mwfr_input.append(get_mwfr_file_input_arg('input_files_longcallD_vcf_gz', input_files_longcallD_vcf_gz))

    mwf_filtering_longcalld = get_latest_mwf(MWF_NAME_SNV_FILTERING_LONGCALLD, smaht_key)
    mwf_filtering = get_latest_mwf(MWF_NAME_SNV_FILTERING, smaht_key) 

    print("Validating Filtering MWFR.")
    mwfr = create_and_validate_analysis_mwfr(
        mwf_filtering_longcalld[UUID] if longcalld_result else mwf_filtering[UUID],
        analysis_run_accession,
        'input_files_TNhaplotyper2_vcf_gz',
        mwfr_input,
        f"{tissue_code}_snv_filtering",
        smaht_key,
    )

    post_analysis_mwfrs([mwfr], smaht_key)


def mwfrs_somatic_sv_callers(tissue_accession, analysis_run, smaht_key):

    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_uuid = tissue["donor"][UUID]

    # Get all released Pacbio WGS files for the tissue
    files_pacbio = get_released_pacbio_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Pacbio WGS files: {len(files_pacbio)}")

    # Get all released ONT WGS files for the tissue
    files_ont = get_released_ont_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of ONT WGS files: {len(files_ont)}")

    print("\nPacbio files to be used:")
    for f in files_pacbio:
        print(f"- Pacbio file: {f[DISPLAY_TITLE]}")

    print("\nONT files to be used:")
    for f in files_ont:
        print(f"- ONT file: {f[DISPLAY_TITLE]}")


    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"\nUsing provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            SOMATIC_SV_CALLING,
            f"Somatic SV Calling: {tissue_code}",
            [donor_uuid],
            [tissue_accession],
            smaht_key,
        )
        print(f"\nCreated AnalysisRun {analysis_run_accession}.")

    mwfrs_to_post = []

    file_long_read = files_pacbio + files_ont

    mwfr_severus = create_severus_mwfr(
        file_long_read,
        f"{tissue_code}_severus",
        analysis_run_accession,
        smaht_key,
    )
    mwfrs_to_post.append(mwfr_severus)

    mwfr_delly = create_delly_mwfr(
        file_long_read,
        f"{tissue_code}_delly",
        analysis_run_accession,
        smaht_key,
    )
    mwfrs_to_post.append(mwfr_delly)

    mwfr_sniffles = create_sniffles_mwfr(
        file_long_read,
        f"{tissue_code}_sniffles",
        analysis_run_accession,
        smaht_key,
    )
    mwfrs_to_post.append(mwfr_sniffles)

    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def mwfrs_somatic_sv_callers_by_core(tissue_accession, analysis_run, smaht_key):
    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_uuid = tissue["donor"][UUID]

    # Get all released short read WGS files for the tissue
    files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Illumina WGS files: {len(files_illumina)}")
    
    print("\nIllumina files associated with the tissue:")
    for f in files_illumina:
        print(f"- Illumina file: {f[DISPLAY_TITLE]}")


    print("\nThey will be processed in the following groups:")

    def get_core_from_display_title(file_item):
        display_title = file_item.get(DISPLAY_TITLE, "")
        parts = display_title.split("-")
        if len(parts) >= 3:
            return parts[2]
        return "unknown"

    grouped_files_illumina = {}
    for f in files_illumina:
        core = get_core_from_display_title(f)
        grouped_files_illumina.setdefault(core, [])
        grouped_files_illumina[core].append(f)


    for core in grouped_files_illumina:
        print(f"\nCore {core} - Illumina files:")
        for f in grouped_files_illumina[core]:
            print(f"- {f[DISPLAY_TITLE]}")
   
    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"\nUsing provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            SOMATIC_SV_CALLING,
            f"Somatic SV Calling (core specific): {tissue_code}",
            [donor_uuid],
            [tissue_accession],
            smaht_key,
        )
        print(f"\nCreated AnalysisRun {analysis_run_accession}.")

    mwfrs_to_post = []
    for core in grouped_files_illumina:
        core_specific_illumina = grouped_files_illumina[core]

        mwfr_delly = create_delly_sr_mwfr(
            core_specific_illumina,
            f"{tissue_code}_core_{core}_dellysr",
            analysis_run_accession,
            smaht_key,
        )
        mwfrs_to_post.append(mwfr_delly)

    # pprint.pprint(mwfrs_to_post)
    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def mwfrs_somatic_sv_callers_by_gcc(tissue_accession, analysis_run, smaht_key):
    tissue = get_item_es(tissue_accession, smaht_key, frame="embedded")
    tissue_code = tissue["external_id"]
    donor_uuid = tissue["donor"][UUID]

    # Get all released short read WGS files for the tissue
    files_illumina = get_released_illumina_wgs_files_for_tissue(tissue_code, smaht_key)
    print(f"Number of Illumina WGS files: {len(files_illumina)}")
    
    print("\nIllumina files associated with the tissue:")
    for f in files_illumina:
        print(f"- Illumina file: {f[DISPLAY_TITLE]}")


    print("\nThey will be processed in the following groups:")

    def get_gcc_from_display_title(file_item):
        display_title = file_item.get(DISPLAY_TITLE, "")
        parts = display_title.split("-")
        if len(parts) >= 6:
            return parts[5]
        return "unknown"

    grouped_files_illumina = {}
    for f in files_illumina:
        gcc = get_gcc_from_display_title(f)
        grouped_files_illumina.setdefault(gcc, [])
        grouped_files_illumina[gcc].append(f)


    for gcc in grouped_files_illumina:
        print(f"\nGCC {gcc} - Illumina files:")
        for f in grouped_files_illumina[gcc]:
            print(f"- {f[DISPLAY_TITLE]}")
   
    # Create the AnalysisRun Item that will contain all MWFRs
    if analysis_run:
        analysis_run_accession = analysis_run
        print(f"\nUsing provided AnalysisRun {analysis_run_accession}.")
    else:
        analysis_run_accession = post_analysis_run(
            SOMATIC_SV_CALLING,
            f"Somatic SV Calling (GCC specific): {tissue_code}",
            [donor_uuid],
            [tissue_accession],
            smaht_key,
        )
        print(f"\nCreated AnalysisRun {analysis_run_accession}.")

    mwfrs_to_post = []
    for gcc in grouped_files_illumina:
        gcc_specific_illumina = grouped_files_illumina[gcc]

        mwfr_delly = create_delly_sr_mwfr(
            gcc_specific_illumina,
            f"{tissue_code}_gcc_{gcc}_dellysr",
            analysis_run_accession,
            smaht_key,
        )
        mwfrs_to_post.append(mwfr_delly)

    # pprint.pprint(mwfrs_to_post)
    post_analysis_mwfrs(mwfrs_to_post, smaht_key)


def create_tnhaplotyper2_mwfr(
    files_illumina, tissue_accession, tag, analysis_run_accession, smaht_key
):

    # Create TNhaplotyper2 MWFR
    mwf_tnhaplotyper = get_latest_mwf(MWF_NAME_TNHAPLOTYPER, smaht_key)
    illumina_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_illumina)
    ]
    mwfr_tnhaplotyper_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, illumina_crams),
        get_mwfr_parameter_input_arg(SAMPLE_NAME, tissue_accession),
    ]
    print("\nValidating TNhaplotyper2 MWFR.")
    mwfr_tnhaplotyper = create_and_validate_analysis_mwfr(
        mwf_tnhaplotyper[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_tnhaplotyper_input,
        tag,
        smaht_key,
    )
    return mwfr_tnhaplotyper

def create_strelka2_mwfr(
    files_illumina, tag, analysis_run_accession, smaht_key
): 
    
    # Create Strelka2 MWFR
    mwf_strelka2 = get_latest_mwf(MWF_NAME_STRELKA2, smaht_key)
    illumina_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_illumina)
    ]
    mwfr_strelka2_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, illumina_crams),
    ]
    print("Validating Strelka2 MWFR.")
    mwfr_strelka2 = create_and_validate_analysis_mwfr(
        mwf_strelka2[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_strelka2_input,
        tag,
        smaht_key,
    )
    return mwfr_strelka2

def create_rufus_mwfr(
    files_illumina, tag, analysis_run_accession, smaht_key
): 
    # Create Rufus MWFR
    mwf_rufus = get_latest_mwf(MWF_NAME_RUFUS, smaht_key)
    illumina_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_illumina)
    ]
    mwfr_rufus_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, illumina_crams),
    ]
    print("Validating Rufus MWFR.")
    mwfr_rufus = create_and_validate_analysis_mwfr(
        mwf_rufus[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_rufus_input,
        tag,
        smaht_key,
    )
    return mwfr_rufus


def create_longcalld_mwfr(
    files, platform, tissue_accession, tag, analysis_run_accession, smaht_key
): 
    # Create longcalled MWFR
    mwf_longcalld = get_latest_mwf(MWF_NAME_LONGCALLD, smaht_key)
    mwf_longcalld_single_file = get_latest_mwf(MWF_NAME_LONGCALLD_SINGLE_FILE, smaht_key)
    if len(files) > 1:
        first_pacbio_cram = [
            {"file": files[0][UUID]}
        ]
        additional_pacbio_crams = [
            {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files[1:])
        ]
        mwfr_longcalled_input = [
            get_mwfr_file_input_arg(INPUT_FILE_CRAM, first_pacbio_cram),
            get_mwfr_file_input_arg(ADDITIONAL_INPUT_FILES_CRAM, additional_pacbio_crams),
            get_mwfr_parameter_input_arg(PLATFORM, platform),
            get_mwfr_parameter_input_arg(SAMPLE_NAME, tissue_accession),
        ]
        print("Validating Longcalld MWFR.")
        mwfr_longcalld = create_and_validate_analysis_mwfr(
            mwf_longcalld[UUID],
            analysis_run_accession,
            ADDITIONAL_INPUT_FILES_CRAM,
            mwfr_longcalled_input,
            tag,
            smaht_key,
        )
        return mwfr_longcalld
    elif len(files) == 1:
        first_pacbio_cram = [
            {"file": files[0][UUID]}
        ]
        mwfr_longcalled_input = [
            get_mwfr_file_input_arg(INPUT_FILE_CRAM, first_pacbio_cram),
            get_mwfr_parameter_input_arg(PLATFORM, platform),
            get_mwfr_parameter_input_arg(SAMPLE_NAME, tissue_accession),
        ]
        print("Validating Longcalld MWFR.")
        mwfr_longcalld = create_and_validate_analysis_mwfr(
            mwf_longcalld_single_file[UUID],
            analysis_run_accession,
            INPUT_FILE_CRAM,
            mwfr_longcalled_input,
            tag,
            smaht_key,
        )
        return mwfr_longcalld


def create_severus_mwfr(
    files_long_read, tag, analysis_run_accession, smaht_key
): 
    # Create Severus MWFR
    mwf_severus = get_latest_mwf(MWF_NAME_SEVERUS, smaht_key)
    long_read_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_long_read)
    ]
    mwfr_severus_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, long_read_crams),
    ]
    print("Validating Severus MWFR.")
    mwfr_severus = create_and_validate_analysis_mwfr(
        mwf_severus[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_severus_input,
        tag,
        smaht_key,
    )
    return mwfr_severus


def create_delly_mwfr(
    files_long_read, tag, analysis_run_accession, smaht_key
): 
    # Create Delly MWFR
    mwf_delly = get_latest_mwf(MWF_NAME_DELLY, smaht_key)
    long_read_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_long_read)
    ]
    mwfr_delly_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, long_read_crams),
    ]
    print("Validating Delly MWFR.")
    mwfr_delly = create_and_validate_analysis_mwfr(
        mwf_delly[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_delly_input,
        tag,
        smaht_key,
    )
    return mwfr_delly


def create_delly_sr_mwfr(
    files_illumina, tag, analysis_run_accession, smaht_key
): 
    # Create Delly SR MWFR
    mwf_delly_sr = get_latest_mwf(MWF_NAME_DELLY_SR, smaht_key)
    illumina_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_illumina)
    ]
    mwfr_delly_sr_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, illumina_crams),
    ]
    print("Validating Delly SR MWFR.")
    mwfr_delly_sr = create_and_validate_analysis_mwfr(
        mwf_delly_sr[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_delly_sr_input,
        tag,
        smaht_key,
    )
    return mwfr_delly_sr


def create_sniffles_mwfr(
    files_long_read, tag, analysis_run_accession, smaht_key
): 
    # Create Sniffles MWFR
    mwf_sniffles = get_latest_mwf(MWF_NAME_SNIFFLES, smaht_key)
    long_read_crams = [
        {"file": f[UUID], "dimension": f"{dim}"} for dim, f in enumerate(files_long_read)
    ]
    mwfr_sniffles_input = [
        get_mwfr_file_input_arg(INPUT_FILES_CRAM, long_read_crams),
    ]
    print("Validating Sniffles MWFR.")
    mwfr_sniffles = create_and_validate_analysis_mwfr(
        mwf_sniffles[UUID],
        analysis_run_accession,
        INPUT_FILES_CRAM,
        mwfr_sniffles_input,
        tag,
        smaht_key,
    )
    return mwfr_sniffles


################################################
#   Helper functions
################################################


def create_and_validate_analysis_mwfr(
    mwf_uuid, analysis_run, input_arg, mwfr_input, tag, smaht_key
):
    mwfr = mwfr_from_input(mwf_uuid, mwfr_input, input_arg, smaht_key)
    mwfr[ANALYSIS_RUNS] = [analysis_run]
    mwfr[TAGS] = [tag]
    mwfr[COMMON_FIELDS] = {SEQUENCING_CENTER: HMS_DAC_UUID}

    try:
        ff_utils.post_metadata(mwfr, META_WORKFLOW_RUN, smaht_key, add_on='?check_only=true')
    except Exception as e:
        raise RuntimeError(f"Validation failed: {e}") from e
    return mwfr


def post_analysis_mwfrs(mwfrs, smaht_key):

    for mwfr in mwfrs:
        #mwfr["final_status"] = "stopped"
        # pprint.pprint(mwfr)
        post_response = ff_utils.post_metadata(mwfr, META_WORKFLOW_RUN, smaht_key)
        mwfr_accession = post_response["@graph"][0]["accession"]
        print(f"Posted MetaWorkflowRun {mwfr_accession}.")
