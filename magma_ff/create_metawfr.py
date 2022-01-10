import json
import copy
import uuid
from magma_ff.metawfl import MetaWorkflow
from magma_ff.metawflrun import MetaWorkflowRun
from dcicutils import ff_utils

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
                                 'institution': case_meta['institution']
                                 'associated_case': case_meta['accession']},
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
