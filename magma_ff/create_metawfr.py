import json
import copy
import uuid
from magma_ff.metawfl import MetaWorkflow
from magma_ff.metawflrun import MetaWorkflowRun
from dcicutils import ff_utils

from .utils import make_embed_request

################################################
#   Main functions
################################################
################################################
#   create_metawfr_from_cohort
################################################
def create_metawfr_from_cohort(metawf_uuid, sp_uuid, type, ff_key, post=False, patch_case=False, verbose=False):
# def create_metawfr_from_cohort(metawf_uuid, cohort_uuid, type, ff_key, post=False, patch_case=False, verbose=False):
    """
        create meta-workflow-run from cohort depending on type

        type
            'Joint-calling gvcf'
    """
    if patch_case:
        post = True

    # !!! temporarily we don't have a cohort object so we start from sample_processing !!!
    # cohort_meta = ff_utils.get_metadata(cohort_uuid, add_on='frame=raw&datastore=database', key=ff_key)
    # sp_uuid = cohort_meta['sample_processing']
    sp_meta = ff_utils.get_metadata(sp_uuid, add_on='frame=raw&datastore=database', key=ff_key)
    sample_uuids = sp_meta['samples']

    if type in ['Joint-calling gvcf']:
        input = create_metawfr_input_from_samples_gvcf(sample_uuids, ff_key)
    else:
        raise ValueError('type argument is not recognized')

    # check if input
    #   else exit function
    if not input:
        return

    # metawfr = create_metawfr_from_input(input, metawf_uuid, cohort_meta, ff_key)
    metawfr = create_metawfr_from_input_TMP(input, metawf_uuid, sp_meta, ff_key)


    # post meta-wfr
    if post:
        print("posting metawfr...")
        res_post = ff_utils.post_metadata(metawfr, 'MetaWorkflowRun', key=ff_key)
        if verbose:
            print(res_post)

    if patch_case:
        pass
        # print("patching case with metawfr...")
        # if type in ['Joint-calling gvcf']:
        #     res_patch = ff_utils.patch_metadata({'': metawfr['uuid']}, , key=ff_key)
        # if verbose:
        #     print(res_patch)

    return metawfr

################################################
#   create_metawfr_from_case
################################################
def create_metawfr_from_case(metawf_uuid, case_uuid, type, ff_key, post=False, patch_case=False, verbose=False):
    """
        create meta-workflow-run from case depending on type

        type
            'WGS trio', 'WGS family', 'WGS proband', 'WGS cram proband',
            'WES trio', 'WES family', 'WES proband', 'WES cram proband',
            'SV trio', 'SV proband'
    """
    if patch_case:
        post = True

    case_meta = ff_utils.get_metadata(case_uuid, add_on='frame=raw&datastore=database', key=ff_key)
    sp_uuid = case_meta['sample_processing']
    sp_meta = ff_utils.get_metadata(sp_uuid, add_on='frame=object&datastore=database', key=ff_key)
    pedigree = sp_meta['samples_pedigree']
    pedigree = remove_parents_without_sample(pedigree)  # remove no-sample individuals
    pedigree = sort_pedigree(pedigree)

    if type in ['WGS cram proband', 'WES cram proband']:
        pedigree = pedigree[0:1]
        input = create_metawfr_input_from_pedigree_cram_proband_only(pedigree, ff_key)
    elif type in ['WGS proband', 'WES proband']:
        pedigree = pedigree[0:1]
        input = create_metawfr_input_from_pedigree_proband_only(pedigree, ff_key)
    elif type in ['WGS trio', 'WES trio']: # parents are required
        input = create_metawfr_input_from_pedigree_trio(pedigree, ff_key)
    elif type in ['WGS family', 'WES family']: # trio like but parents may be missing
        input = create_metawfr_input_from_pedigree_family(pedigree, ff_key)
    elif type in ['SV proband']:
        input = create_metawfr_input_from_pedigree_SV_proband_only(pedigree, ff_key)
    elif type in ['SV trio']:
        input = create_metawfr_input_from_pedigree_SV_trio(pedigree, ff_key)
    else:
        raise ValueError('type argument is not recognized')

    # check if input
    #   else exit function
    if not input:
        return

    metawfr = create_metawfr_from_input(input, metawf_uuid, case_meta, ff_key)

    # post meta-wfr
    if post:
        print("posting metawfr...")
        res_post = ff_utils.post_metadata(metawfr, 'MetaWorkflowRun', key=ff_key)
        if verbose:
            print(res_post)

    if patch_case:
        print("patching case with metawfr...")
        if type in ['WGS cram proband', 'WGS proband', 'WGS trio', 'WGS family'
                    'WES cram proband', 'WES proband', 'WES trio', 'WES family']:
            res_patch = ff_utils.patch_metadata({'meta_workflow_run': metawfr['uuid']}, case_uuid, key=ff_key)
        elif type in ['SV proband', 'SV trio']:
            res_patch = ff_utils.patch_metadata({'meta_workflow_run_sv': metawfr['uuid']}, case_uuid, key=ff_key)
        if verbose:
            print(res_patch)

    return metawfr

################################################
#   Helper functions
################################################
################################################
#   create_metawfr_from_input_TMP
#       just a very temporary solution until
#       we have a cohort in place
################################################
def create_metawfr_from_input_TMP(metawfr_input, metawf_uuid, case_meta, ff_key):
    metawf_meta = ff_utils.get_metadata(metawf_uuid, add_on='frame=raw&datastore=database', key=ff_key)
    metawfr = {'meta_workflow': metawf_uuid,
               'input': metawfr_input,
               'title': 'MetaWorkflowRun %s on sample_processing %s' % (metawf_meta['title'], case_meta['uuid']),
               'project': case_meta['project'],
               'institution': case_meta['institution'],
               'common_fields': {'project': case_meta['project'],
                                 'institution': case_meta['institution']},
               'final_status': 'pending',
               'workflow_runs' : [],
               'uuid': str(uuid.uuid4())}

    mwf = MetaWorkflow(metawf_meta)

    # get the input structure in magma format
    metawfr_mgm = MetaWorkflowRun(metawfr).to_json()
    input_structure = metawfr_mgm['input'][0]['files']
    # this is expecting the argument for input files first in the input list

    # create workflow_runs
    mwfr = mwf.write_run(input_structure)
    metawfr['workflow_runs'] = mwfr['workflow_runs']

    return metawfr

################################################
#   create_metawfr_from_input
################################################
def create_metawfr_from_input(metawfr_input, metawf_uuid, case_meta, ff_key):
    metawf_meta = ff_utils.get_metadata(metawf_uuid, add_on='frame=raw&datastore=database', key=ff_key)
    metawfr = {'meta_workflow': metawf_uuid,
               'input': metawfr_input,
               'title': 'MetaWorkflowRun %s on case %s' % (metawf_meta['title'], case_meta['accession']),
               'project': case_meta['project'],
               'institution': case_meta['institution'],
               'common_fields': {'project': case_meta['project'],
                                 'institution': case_meta['institution'],
                                 'case_accession': case_meta['accession']},
               'final_status': 'pending',
               'workflow_runs' : [],
               'uuid': str(uuid.uuid4())}

    mwf = MetaWorkflow(metawf_meta)

    # get the input structure in magma format
    metawfr_mgm = MetaWorkflowRun(metawfr).to_json()
    input_structure = metawfr_mgm['input'][0]['files']
    # this is expecting the argument for input files first in the input list

    # create workflow_runs
    mwfr = mwf.write_run(input_structure)
    metawfr['workflow_runs'] = mwfr['workflow_runs']

    return metawfr

################################################
#   create_metawfr_input_from_pedigree_SV_proband_only
################################################
def create_metawfr_input_from_pedigree_SV_proband_only(pedigree, ff_key):
    # sample names
    sample = pedigree[0]
    sample_names = [sample['sample_name']]
    sample_names_str = json.dumps(sample_names)
    # qc pedigree parameter
    qc_pedigree_str = json.dumps(pedigree_to_qc_pedigree(pedigree))

    input_bams = {
        'argument_name': 'input_bams',
        'argument_type': 'file', 'files':[]}

    dimension = -1
    for a_member in pedigree:
        dimension += 1
        if 'bam_location' in a_member:
            x = a_member['bam_location'].split("/")[0]
            input_bams['files'].append({'file': x, 'dimension': str(dimension)})
        else: return

    input = []
    input.append(input_bams)
    input.append({'argument_name': 'sample_names_proband_first_if_trio', 'argument_type': 'parameter', 'value': sample_names_str, 'value_type': 'json'})
    input.append({'argument_name': 'pedigree', 'argument_type': 'parameter', 'value': qc_pedigree_str, 'value_type': 'string'})

    return input

################################################
#   create_metawfr_input_from_pedigree_SV_trio
################################################
def create_metawfr_input_from_pedigree_SV_trio(pedigree, ff_key):
    # sample names
    sample_names = [s['sample_name'] for s in pedigree]
    sample_names_str = json.dumps(sample_names)

    # qc pedigree parameter
    qc_pedigree_str = json.dumps(pedigree_to_qc_pedigree(pedigree))
    input_bams = {
        'argument_name': 'input_bams',
        'argument_type': 'file', 'files':[]}

    dimension = -1
    for a_member in pedigree:
        dimension += 1
        if 'bam_location' in a_member:
            x = a_member['bam_location'].split("/")[0]
            input_bams['files'].append({'file': x, 'dimension': str(dimension)})
        else: return

    input = []
    input.append(input_bams)
    input.append({'argument_name': 'sample_names_proband_first_if_trio', 'argument_type': 'parameter', 'value': sample_names_str, 'value_type': 'json'})
    input.append({'argument_name': 'pedigree', 'argument_type': 'parameter', 'value': qc_pedigree_str, 'value_type': 'string'})

    return input

################################################
#   create_metawfr_input_from_pedigree_cram_proband_only
################################################
def create_metawfr_input_from_pedigree_cram_proband_only(pedigree, ff_key):
    sample = pedigree[0]
    sample_acc = sample['sample_accession']
    sample_meta = ff_utils.get_metadata(sample_acc, add_on='frame=raw&datastore=database', key=ff_key)
    cram_uuids = sample_meta['cram_files']
    cram_files = [{'file': cf, 'dimension': str(i)} for i, cf in enumerate(cram_uuids)]

    # sample names
    sample_names = [sample['sample_name']]
    sample_names_str = json.dumps(sample_names)

    # qc pedigree parameter
    qc_pedigree_str = json.dumps(pedigree_to_qc_pedigree(pedigree))

    # bamsnap titles
    bamsnap_titles_str = json.dumps([sample_names[0] + ' (proband)'])

    # create metawfr input
    input = [{'argument_name': 'crams', 'argument_type': 'file', 'files': cram_files},
             {'argument_name': 'bamsnap_titles', 'argument_type': 'parameter', 'value': bamsnap_titles_str, 'value_type': 'json'},
             {'argument_name': 'sample_names', 'argument_type': 'parameter', 'value': sample_names_str, 'value_type': 'json'},
             {'argument_name': 'pedigree', 'argument_type': 'parameter', 'value': qc_pedigree_str, 'value_type': 'string'}]

    return input

################################################
#   create_metawfr_input_from_pedigree_proband_only
################################################
def create_metawfr_input_from_pedigree_proband_only(pedigree, ff_key):
    sample = pedigree[0]
    sample_acc = sample['sample_accession']
    sample_meta = ff_utils.get_metadata(sample_acc, add_on='frame=raw&datastore=database', key=ff_key)
    fastq_uuids = sample_meta['files']
    r1_uuids =[]
    r2_uuids = []
    j = 0  # second dimension of files
    for fastq_uuid in fastq_uuids:
        fastq_meta = ff_utils.get_metadata(fastq_uuid, add_on='frame=raw&datastore=database', key=ff_key)
        if fastq_meta['paired_end'] == '1':
           dimension = str(j)  # dimension string for files
           r1_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})
           r2_uuids.append({'file': fastq_meta['related_files'][0]['file'], 'dimension': dimension})
           j += 1
        # # below is temporary code for a case with no paired_end / related_files fields in fastq metadata.
        # # only works for a single pair !! NOT SURE WHAT THIS WAS FOR, LEGACY FROM SOO
        # dimension = str(j)
        # if fastq_meta['description'].endswith('paired end:1'):
        #     r1_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})
        # else:
        #     r2_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})

    # sample names
    sample_names = [sample['sample_name']]
    sample_names_str = json.dumps(sample_names)

    # qc pedigree parameter
    qc_pedigree_str = json.dumps(pedigree_to_qc_pedigree(pedigree))

    # bamsnap titles
    bamsnap_titles_str = json.dumps([sample_names[0] + ' (proband)'])

    # create metawfr
    input = [{'argument_name': 'fastqs_R1', 'argument_type': 'file', 'files': r1_uuids},
             {'argument_name': 'fastqs_R2', 'argument_type': 'file', 'files': r2_uuids},
             {'argument_name': 'sample_names', 'argument_type': 'parameter', 'value': sample_names_str, 'value_type': 'json'},
             {'argument_name': 'bamsnap_titles', 'argument_type': 'parameter', 'value': bamsnap_titles_str, 'value_type': 'json'},
             {'argument_name': 'pedigree', 'argument_type': 'parameter', 'value': qc_pedigree_str, 'value_type': 'string'}]

    return input

################################################
#   create_metawfr_input_from_pedigree_trio
################################################
def create_metawfr_input_from_pedigree_trio(pedigree, ff_key):
    # prepare fastq input
    r1_uuids_fam = []
    r2_uuids_fam = []
    i = 0  # first dimension of files
    for sample in pedigree:
        sample_acc = sample['sample_accession']
        sample_meta = ff_utils.get_metadata(sample_acc, add_on='frame=raw&datastore=database', key=ff_key)
        fastq_uuids = sample_meta['files']
        r1_uuids =[]
        r2_uuids = []
        j = 0  # second dimension of files
        for fastq_uuid in fastq_uuids:
            fastq_meta = ff_utils.get_metadata(fastq_uuid, add_on='frame=raw&datastore=database', key=ff_key)
            if fastq_meta['paired_end'] == '1':
                dimension = '%d,%d' % (i, j)  # dimension string for files
                r1_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})
                r2_uuids.append({'file': fastq_meta['related_files'][0]['file'], 'dimension': dimension})
                j += 1
        r1_uuids_fam.extend(r1_uuids)
        r2_uuids_fam.extend(r2_uuids)
        i += 1

    # sample names
    sample_names = [s['sample_name'] for s in pedigree]
    sample_names_str = json.dumps(sample_names)

    # qc pedigree parameter
    qc_pedigree_str = json.dumps(pedigree_to_qc_pedigree(pedigree))

    # family size
    family_size = len(pedigree)

    # rcktar_content_file_names
    rcktar_content_file_names = [s + '.rck.gz' for s in sample_names]
    rcktar_content_file_names_str = json.dumps(rcktar_content_file_names)

    # bamsnap titles
    bamsnap_titles_str = json.dumps(['%s (%s)' % (s['sample_name'], s['relationship']) for s in pedigree])

    # create metawfr
    input = [{'argument_name': 'fastqs_proband_first_R1', 'argument_type': 'file', 'files': r1_uuids_fam},
             {'argument_name': 'fastqs_proband_first_R2', 'argument_type': 'file', 'files': r2_uuids_fam},
             {'argument_name': 'sample_names_proband_first', 'argument_type': 'parameter', 'value': sample_names_str, 'value_type': 'json'},
             {'argument_name': 'pedigree', 'argument_type': 'parameter', 'value': qc_pedigree_str, 'value_type': 'string'},
             {'argument_name': 'bamsnap_titles', 'argument_type': 'parameter', 'value': bamsnap_titles_str, 'value_type': 'json'},
             {'argument_name': 'family_size', 'argument_type': 'parameter', 'value': str(family_size), 'value_type': 'integer'},
             {'argument_name': 'rcktar_content_file_names', 'argument_type': 'parameter', 'value': rcktar_content_file_names_str, 'value_type': 'json'}]

    return input

################################################
#   create_metawfr_input_from_pedigree_family
################################################
def create_metawfr_input_from_pedigree_family(pedigree, ff_key):
    # create trio input
    input_ = create_metawfr_input_from_pedigree_trio(pedigree, ff_key)

    # modify it for family
    input = []
    for i in input_:
        if i['argument_name'] == 'rcktar_content_file_names':
            pass
        else:
            input.append(i)
            if i['argument_name'] == 'sample_names_proband_first':
                sample_name_proband = json.dumps([json.loads(i['value'])[0]])
                input.append({'argument_name': 'sample_name_proband',
                              'argument_type': 'parameter',
                              'value': sample_name_proband,
                              'value_type': 'json'})

    return input

################################################
#   create_metawfr_input_from_samples_gvcf
################################################
def create_metawfr_input_from_samples_gvcf(sample_uuids, ff_key):
    """
        create meta-workflow-run input for joint calling
        for samples specified in sample_uuids
    """
    # initialize argument
    input_gvcfs = {
        'argument_name': 'input_gvcfs',
        'argument_type': 'file', 'files':[]}

    # get gvcfs and add to argument
    for i, sample_uuid in enumerate(sample_uuids):
        sample_meta = ff_utils.get_metadata(sample_uuid, add_on='frame=raw&datastore=database', key=ff_key)
        uuid_ = processed_file_from_sample_by_type(sample_meta, 'gVCF', ff_key)
        input_gvcfs['files'].append({'file': uuid_, 'dimension': str(i)})

    # create input
    input = []
    input.append(input_gvcfs)

    return input

################################################
#   sort_pedigree
################################################
def sort_pedigree(pedigree):
    sorted_pedigree = sorted(pedigree, key=lambda x: x['relationship'] != 'proband') # make it proband-first
    sorted_pedigree[1:] = sorted(sorted_pedigree[1:], key=lambda x: x['relationship'] not in ['mother','father']) # parents next

    return sorted_pedigree

################################################
#   pedigree_to_qc_pedigree
################################################
def pedigree_to_qc_pedigree(samples_pedigree):
    """
        extract pedigree for qc for every family member
            - input samples accession list
            - qc pedigree
    """
    qc_pedigree = []
    # get samples
    for sample in samples_pedigree:
        member_qc_pedigree = {
            'gender': sample.get('sex', ''),
            'individual': sample.get('individual', ''),
            'parents': sample.get('parents', []),
            'sample_name': sample.get('sample_name', '')
            }
        qc_pedigree.append(member_qc_pedigree)

    return qc_pedigree

################################################
#   remove_parents_without_sample
################################################
def remove_parents_without_sample(samples_pedigree):
    individuals = [i['individual'] for i in samples_pedigree]
    for a_member in samples_pedigree:
        parents = a_member['parents']
        new_parents = [i for i in parents if i in individuals]
        a_member['parents'] = new_parents

    return samples_pedigree

################################################
#   processed_file_from_sample_by_type
################################################
def processed_file_from_sample_by_type(sample_meta, type, ff_key):
    """
        given sample metadata extract processed file with specified file_type
    """
    for file_uuid in sample_meta['processed_files']:
        file_meta = ff_utils.get_metadata(file_uuid, add_on='frame=raw&datastore=database', key=ff_key)
        if file_meta['file_type'] == type:
            return file_meta['uuid']




class MetaWorkflowRunCreationError(Exception):
    """"""
    pass


class MetaWorkflowRunInput:

    # Schema constants
    TITLE = "title"
    INPUT = "input"
    ARGUMENT_NAME = "argument_name"
    ARGUMENT_TYPE = "argument_type"
    VALUE = "value"
    VALUE_TYPE = "value_type"
    FILE = "file"
    FILES = "files"
    PARAMETER = "parameter"
    PROJECT = "project"
    INSTITUTION = "institution"
    UUID = "uuid"
    COMMON_FIELDS = "common_fields"
    META_WORKFLOW = "meta_workflow"
    FINAL_STATUS = "final_status"
    PENDING = "pending"
    WORKFLOW_RUNS = "workflow_runs"
    DIMENSION = "dimension"
    DIMENSIONALITY = "dimensionality"
    INPUT_SAMPLES = "input_samples"
    ASSOCIATED_SAMPLE_PROCESSING = "associated_sample_processing"

    # Class constants
    META_WORKFLOW_RUN_ENDPOINT = "/meta-workflow-runs"
    STATUS = "status"
    SUCCESS_STATUS = "success"
    CHECK_ONLY_ADD_ON = "check_only=true"

    def __init__(self, meta_workflow, input_properties):
        """"""
        self.meta_workflow = meta_workflow
        self.input_properties = input_properties

    def create_input(self):
        """"""
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
        """"""
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

    def format_file_input_value(self, file_parameter, file_value, input_dimensions):
        """"""
        result = []
        sorted_key_indices_by_sample = sorted(file_value.keys())
        for sample_idx in sorted_key_indices_by_sample:
            sample_file_uuids = file_value[sample_idx]
            if input_dimensions == 1:
                if len(sample_file_uuids) > 1:
                    raise MetaWorkflowRunCreationError(
                        "Found multiple input files when only 1 was expected for"
                        " parameter %s: %s"
                        % (file_parameter, sample_file_uuids)
                    )
                for file_uuid in sample_file_uuids:
                    dimension = str(sample_idx)
                    formatted_file_result = {
                        self.FILE: file_uuid,
                        self.DIMENSION: dimension,
                    }
                    result.append(formatted_file_result)
            elif input_dimensions == 2:
                for file_uuid_idx, file_uuid in enumerate(sample_file_uuids):
                    dimension = "%s,%s" % (sample_idx, file_uuid_idx)
                    formatted_file_result = {
                        self.FILE: file_uuid,
                        self.DIMENSION: dimension,
                    }
                    result.append(formatted_file_result)
            else:
                raise MetaWorkflowRunCreationError(
                    "Received an unexpected dimension number for parameter %s: %s"
                    % (file_parameter, input_dimensions)
                )
        return result

    def fetch_parameters(self, parameters_to_fetch):
        """"""
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
        """"""
        if isinstance(parameter_value, list) or isinstance(parameter_value, dict):
            result = json.dumps(parameter_value)
        else:
            result = str(parameter_value)
        return result


class MetaWorkflowRunFromSampleProcessing():
    """"""
    # Embedding API fields
    FIELDS_TO_GET = [
        "project",
        "institution",
        "uuid",
        "meta_workflow_runs.uuid",
        "samples_pedigree",
        "samples.bam_sample_id",
        "samples.uuid",
        "samples.files.uuid",
        "samples.files.related_files",
        "samples.files.paired_end",
        "samples.files.file_format.file_format",
        "samples.cram_files.uuid",
        "samples.processed_files.uuid",
        "samples.processed_files.file_format.file_format",
    ]

    # Schema constants
    SAMPLE_PROCESSING = "sample_processing"
    META_WORKFLOW_RUNS = "meta_workflow_runs"
    ASSOCIATED_META_WORKFLOW_RUN = "associated_meta_workflow_run"
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
    INPUT_SAMPLES = "input_samples"
    ASSOCIATED_SAMPLE_PROCESSING = "associated_sample_processing"
    FILES = "files"

    # Class constants
    META_WORKFLOW_RUN_ENDPOINT = "/meta-workflow-runs"
    STATUS = "status"
    SUCCESS_STATUS = "success"
    CHECK_ONLY_ADD_ON = "check_only=true"

    def __init__(self, sample_processing_identifier, meta_workflow_identifier, auth_key):
        """"""
        self.auth_key = auth_key
        sample_processing = make_embed_request(
            sample_processing_identifier, self.FIELDS_TO_GET, self.auth_key
        )
        if not sample_processing:
            raise MetaWorkflowRunCreationError(
                "No SampleProcessing found for given identifier: %s"
                % sample_processing_identifier
            )
        self.meta_workflow = self.get_item_properties(meta_workflow_identifier)
        if not self.meta_workflow:
            raise MetaWorkflowRunCreationError(
                "No MetaWorkflow found for given identifier: %s"
                % meta_workflow_identifier
            )
        self.project = sample_processing.get(self.PROJECT)
        self.institution = sample_processing.get(self.INSTITUTION)
        self.sample_processing_uuid = sample_processing.get(self.UUID)
        self.existing_meta_workflow_runs = sample_processing.get(
            self.META_WORKFLOW_RUNS, []
        )
        self.input_properties = InputPropertiesFromSampleProcessing(sample_processing)
        self.meta_workflow_run_input = MetaWorkflowRunInput(
            self.meta_workflow, self.input_properties
        ).create_input()
        self.meta_workflow_run_uuid = str(uuid.uuid4())
        self.meta_workflow_run = self.create_meta_workflow_run()

    def create_meta_workflow_run(self):
        """"""
        meta_workflow_title = self.meta_workflow.get(self.TITLE)
        title = (
            "MetaWorkflowRun %s on SampleProcessing %s"
            % (meta_workflow_title, self.sample_processing_uuid)
        )
        meta_workflow_run = {
            self.META_WORKFLOW: self.meta_workflow.get(self.UUID),
            self.INPUT: self.meta_workflow_run_input,
            self.TITLE: title,
            self.PROJECT: self.project,
            self.INSTITUTION: self.institution,
            self.INPUT_SAMPLES: self.input_properties.input_sample_uuids,
            self.ASSOCIATED_SAMPLE_PROCESSING: self.sample_processing_uuid,
            self.COMMON_FIELDS: {
                self.PROJECT: self.project,
                self.INSTITUTION: self.institution,
                self.ASSOCIATED_META_WORKFLOW_RUN: self.meta_workflow_run_uuid,
            },
            self.FINAL_STATUS: self.PENDING,
            self.WORKFLOW_RUNS: [],
            self.UUID: self.meta_workflow_run_uuid,
        }
        self.create_workflow_runs(meta_workflow_run)
        return meta_workflow_run

    def create_workflow_runs(self, meta_workflow_run):
        """"""
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

    def get_item_properties(self, item_uuid):
        """"""
        try:
            result = ff_utils.get_metadata(
                item_uuid, key=self.auth_key, add_on="frame=raw"
            )
        except Exception:
            result = None
        return result

    def post_and_patch(self):
        """"""
        self.post_meta_workflow_run()
        meta_workflow_run_uuids = [
            item.get(self.UUID) for item in self.existing_meta_workflow_runs
        ]
        meta_workflow_run_uuids.append(self.meta_workflow_run_uuid)
        patch_body = {self.META_WORKFLOW_RUNS: meta_workflow_run_uuids}
        self.validate_patch_item(self.sample_processing_uuid, patch_body)
        self.patch_item(self.sample_processing_uuid, patch_body)

    def post_meta_workflow_run(self):
        """"""
        self.validate_post_item(
            self.meta_workflow_run, self.META_WORKFLOW_RUN_ENDPOINT,
        )
        self.post_item(self.meta_workflow_run, self.META_WORKFLOW_RUN_ENDPOINT)

    def validate_post_item(self, item_to_post, item_endpoint):
        """"""
        validation = ff_utils.post_metadata(
            item_to_post,
            item_endpoint,
            key=self.auth_key,
            add_on=self.CHECK_ONLY_ADD_ON,
        )
        validation_status = validation.get(self.STATUS)
        if validation_status != self.SUCCESS_STATUS:
            raise MetaWorkflowRunCreationError(
                "POST of item to %s did not validate. Error message: %s"
                % (item_endpoint, validation)
            )

    def post_item(self, item_to_post, item_endpoint):
        """"""
        ff_utils.post_metadata(item_to_post, item_endpoint, key=self.auth_key)

    def validate_patch_item(self, item_to_patch, patch_body):
        """"""
        validation = ff_utils.patch_metadata(
            patch_body,
            obj_id=item_to_patch,
            key=self.auth_key,
            add_on=self.CHECK_ONLY_ADD_ON,
        )
        validation_status = validation.get(self.STATUS)
        if validation_status != self.SUCCESS_STATUS:
            raise MetaWorkflowRunCreationError(
                "PATCH of item %s did not validate. Error message: %s"
                % (item_to_patch, validation)
            )

    def patch_item(self, item_to_patch, patch_body):
        """"""
        ff_utils.patch_metadata(patch_body, obj_id=item_to_patch, key=self.auth_key)



class InputPropertiesFromSampleProcessing:
    """"""
    # Schema constants
    UUID = "uuid"
    ACCESSION = "accession"
    SAMPLES_PEDIGREE = "samples_pedigree"
    SAMPLES = "samples"
    BAM_SAMPLE_ID = "bam_sample_id"
    PROCESSED_FILES = "processed_files"
    FILES = "files"
    RELATED_FILES = "related_files"
    FILE_FORMAT = "file_format"
    PAIRED_END = "paired_end"
    PAIRED_END_1 = "1"
    PAIRED_END_2 = "2"

    # File formats
    FASTQ_FORMAT = "fastq"
    CRAM_FORMAT = "cram"
    BAM_FORMAT = "bam"
    GVCF_FORMAT = "gvcf_gz"

    # Pedigree constants
    RELATIONSHIP = "relationship"
    PROBAND = "proband"
    MOTHER = "mother"
    FATHER = "father"
    INDIVIDUAL = "individual"
    PARENTS = "parents"
    SAMPLE_NAME = "sample_name"
    GENDER = "gender"
    SEX = "sex"

    # MetaWorkflow constants
    RCKTAR_FILE_ENDING = ".rck.gz"

    def __init__(self, sample_processing):
        """"""
        self.sample_processing = sample_processing
        self.sorted_samples, self.sorted_samples_pedigree = self.clean_and_sort_samples_and_pedigree()

    def clean_and_sort_samples_and_pedigree(self):
        """"""
        proband_name = None
        mother_name = None
        father_name = None
        samples_pedigree = self.sample_processing.get(self.SAMPLES_PEDIGREE, [])
        if not samples_pedigree:
            raise MetaWorkflowRunCreationError(
                "No samples_pedigree found on SampleProcessing: %s"
                % self.sample_processing
            )
        samples = self.sample_processing.get(self.SAMPLES, [])
        if not samples:
            raise MetaWorkflowRunCreationError(
                "No Samples found on SampleProcessing: %s" % self.sample_processing
            )
        if len(samples) != len(samples_pedigree):
            raise MetaWorkflowRunCreationError(
                "Number of Samples did not match number of entries in samples_pedigree"
                " on SampleProcessing: %s"
                % self.sample_processing
            )
        all_individuals = [
            sample.get(self.INDIVIDUAL) for sample in samples_pedigree
            if sample.get(self.INDIVIDUAL)
        ]
        bam_sample_ids = [
            sample.get(self.BAM_SAMPLE_ID) for sample in samples
            if sample.get(self.BAM_SAMPLE_ID)
        ]
        for pedigree_sample in samples_pedigree:
            parents = pedigree_sample.get(self.PARENTS, [])
            if parents:  # Remove parents that aren't in samples_pedigree
                missing_parents = [parent for parent in parents if parent not in
                        all_individuals]
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
                "No proband found within the pedigree: %s" % samples_pedigree
            )
        sorted_samples_pedigree = self.sort_by_sample_name(
            samples_pedigree, self.SAMPLE_NAME, proband_name, mother=mother_name, father=father_name
        )
        sorted_samples = self.sort_by_sample_name(
            samples, self.BAM_SAMPLE_ID, proband_name, mother=mother_name, father=father_name
        )
        return sorted_samples, sorted_samples_pedigree

    def sort_by_sample_name(self, items_to_sort, sample_name_key, proband, mother=None, father=None):
        """"""
        result = []
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
            result.append(items_to_sort[proband_idx])
        if mother_idx is not None:
            result.append(items_to_sort[mother_idx])
        if father_idx is not None:
            result.append(items_to_sort[father_idx])
        for idx in other_idx:
            result.append(items_to_sort[idx])
        return result

    def get_samples_processed_file_for_format(self, file_format):
        """"""
        result = {}
        for idx, sample in enumerate(self.sorted_samples):
            matching_files = self.get_processed_file_for_format(sample, file_format)
            result[idx] = matching_files
        return result

    def get_processed_file_for_format(self, sample, file_format, key=None, value=None):
        """"""
        result = []
        processed_files = sample.get(self.PROCESSED_FILES, [])
        for processed_file in processed_files:
            if key is not None and value is not None:
                key_value = processed_file.get(key)
                if key_value != value:
                    continue
            processed_file_format = processed_file.get(self.FILE_FORMAT,
                    {}).get(self.FILE_FORMAT)
            if processed_file_format == file_format:
                file_uuid = processed_file.get(self.UUID)
                result.append(file_uuid)
        if not result:
            raise MetaWorkflowRunCreationError(
                "No file with format %s found on Sample: %s"
                % (file_format, sample)
            )
        return result

    def get_fastqs_for_paired_end(self, paired_end):
        """"""
        result = {}
        for idx, sample in enumerate(self.sorted_samples):
            paired_end_fastqs = []
            fastq_files = sample.get(self.FILES, [])
            for fastq_file in fastq_files:  # Expecting only FASTQs, but check
                file_format = fastq_file.get(self.FILE_FORMAT, {}).get(self.FILE_FORMAT)
                if file_format != self.FASTQ_FORMAT:
                    continue
                related_files = fastq_file.get(self.RELATED_FILES)
                if related_files is None:
                    raise MetaWorkflowRunCreationError(
                        "Sample contains a FASTQ file without a related file: %s"
                        % sample
                    )
                file_paired_end = fastq_file.get(self.PAIRED_END)
                if file_paired_end == paired_end:
                    file_uuid = fastq_file.get(self.UUID)
                    paired_end_fastqs.append(file_uuid)
            if not paired_end_fastqs:  # May have come from CRAM conversion
                paired_end_fastqs = self.get_processed_file_for_format(
                    sample, self.FASTQ_FORMAT, key=self.PAIRED_END, value=paired_end
                )
            result[idx] = paired_end_fastqs
        return result

    @property
    def sample_names(self):
        """"""
        return [
            pedigree_sample[self.SAMPLE_NAME]
            for pedigree_sample in self.sorted_samples_pedigree
        ]

    @property
    def input_sample_uuids(self):
        """"""
        return [sample[self.UUID] for sample in self.sorted_samples]

    @property
    def pedigree(self):
        """"""
        result = []
        for pedigree_sample in self.sorted_samples_pedigree:
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
    def input_crams(self):
        """"""
        result = {}
        for idx, sample in self.sorted_samples:
            cram_uuids = []
            cram_files = sample.get(self.CRAM_FILES)
            if cram_files is None:
                raise MetaWorkflowRunCreationError(
                    "Tried to grab CRAM files from a Sample lacking them: %s"
                    % sample
                )
            for cram_file in cram_files:
                cram_uuid = cram_file.get(self.UUID)
                cram_uuids.append(cram_uuid)
            result[idx] = cram_uuids
        return result

    @property
    def input_gvcfs(self):
        """"""
        return self.get_samples_processed_file_for_format(self.GVCF_FORMAT)

    @property
    def fastqs_r1(self):
        """"""
        return self.get_fastqs_for_paired_end(self.PAIRED_END_1)

    @property
    def fastqs_r2(self):
        """"""
        return self.get_fastqs_for_paired_end(self.PAIRED_END_2)

    @property
    def input_bams(self):
        """"""
        return self.get_samples_processed_file_for_format(self.BAM_FORMAT)

    @property
    def rcktar_file_names(self):
        """"""
        return [
            sample_name + self.RCKTAR_FILE_ENDING for sample_name in self.sample_names
        ]

    @property
    def sample_name_proband(self):
        """"""
        return self.sample_names[0]  # Already sorted to proband-first

    @property
    def bamsnap_titles(self):
        """"""
        result = []
        for sample_pedigree in self.sorted_samples_pedigree:
            sample_name = sample_pedigree.get(self.SAMPLE_NAME)
            sample_relationship = sample_pedigree.get(self.RELATIONSHIP, "")
            result.append("%s (%s)" % (sample_name, sample_relationship))
        return result

    @property
    def family_size(self):
        """"""
        return len(self.sample_names)
