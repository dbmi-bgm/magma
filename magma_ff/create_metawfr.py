import json
import copy
import uuid
from magma_ff.metawfl import MetaWorkflow
from magma_ff.metawflrun import MetaWorkflowRun
from dcicutils import ff_utils


def create_metawfr_from_case(metawf_uuid, case_uuid, type, ff_key, post=False, patch_case=False, verbose=False):
    """This is the main API - the rest are internal functions.
    type should be 'WGS trio', 'WGS proband', 'WGS cram proband'
    """
    case_meta = ff_utils.get_metadata(case_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    sp_uuid = case_meta['sample_processing']
    sp_meta = ff_utils.get_metadata(sp_uuid, add_on='?frame=object&datastore=database', key=ff_key)
    pedigree = sp_meta['samples_pedigree']
    pedigree = remove_parents_without_sample(pedigree)  # remove no-sample individuals
    pedigree = sort_pedigree(pedigree)

    if type == 'WGS cram proband':
        pedigree = pedigree[0:1]
        input = create_metawfr_input_from_pedigree_proband_only(pedigree, ff_key)
    elif type == 'WGS proband':
        pedigree = pedigree[0:1]
        input = create_metawfr_input_from_pedigree_proband_only(pedigree, ff_key)
    elif type == 'WGS trio':
        input = create_metawfr_input_from_pedigree_trio(pedigree, ff_key)

    metawfr = create_metawfr_from_input(input, metawf_uuid, case_meta, ff_key)

    # post meta-wfr
    if post:
        print("posting metawfr...")
        res_post = ff_utils.post_metadata(metawfr, 'MetaWorkflowRun', key=ff_key)
        if verbose:
            print(res_post)

    if patch_case:
        print("patching case with metawfr...")
        res_patch = ff_utils.patch_metadata({'meta_workflow_run': metawfr['uuid']}, case_uuid, key=ff_key)
        if verbose:
            print(res_patch)

    return metawfr


def create_metawfr_from_input(metawfr_input, metawf_uuid, case_meta, ff_key):
    metawf_meta = ff_utils.get_metadata(metawf_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
    metawfr = {'meta_workflow': metawf_uuid,
               'input': metawfr_input,
               'title': 'MetaWorkflowRun %s on case %s' % (metawf_meta['title'], case_meta['accession']),
               #'name': 'metawf_%s_on_case_%s' % (metawf_meta['accession'], case_meta['accession']),
               #'name': 'MetaWorkflowRun %s for case %s' % (metawf_meta['title'], case_meta['accession']),
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

    # create workflow_runs
    mwfr = mwf.write_run(input_structure)
    metawfr['workflow_runs'] = mwfr['workflow_runs']

    return metawfr


def create_metawfr_input_from_pedigree_cram_proband_only(pedigree, ff_key):
    sample = pedigree[0]
    sample_acc = sample['sample_accession']
    sample_meta = ff_utils.get_metadata(sample_acc, add_on='?frame=raw&datastore=database', key=ff_key)
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


def create_metawfr_input_from_pedigree_proband_only(pedigree, ff_key):
    sample = pedigree[0]
    sample_acc = sample['sample_accession']
    sample_meta = ff_utils.get_metadata(sample_acc, add_on='?frame=raw&datastore=database', key=ff_key)
    fastq_uuids = sample_meta['files']
    r1_uuids =[]
    r2_uuids = []
    j = 0  # second dimension of files
    for fastq_uuid in fastq_uuids:
        fastq_meta = ff_utils.get_metadata(fastq_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
        #if fastq_meta['paired_end'] == '1':
        #    dimension = str(j)  # dimension string for files
        #    r1_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})
        #    r2_uuids.append({'file': fastq_meta['related_files'][0]['file'], 'dimension': dimension})
        #    j += 1
        # below is temporary code for a case with no paired_end / related_files fields in fastq metadata.
        # only works for a single pair
        dimention = str(j)
        if fastq_meta['description'].endswith('paired end:1'):
            r1_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})
        else:
            r2_uuids.append({'file': fastq_meta['uuid'], 'dimension': dimension})

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


def create_metawfr_input_from_pedigree_trio(pedigree, ff_key):
    # prepare fastq input
    r1_uuids_fam = []
    r2_uuids_fam = []
    i = 0  # first dimension of files
    for sample in pedigree:
        sample_acc = sample['sample_accession']
        sample_meta = ff_utils.get_metadata(sample_acc, add_on='?frame=raw&datastore=database', key=ff_key)
        fastq_uuids = sample_meta['files']
        r1_uuids =[]
        r2_uuids = []
        j = 0  # second dimension of files
        for fastq_uuid in fastq_uuids:
            fastq_meta = ff_utils.get_metadata(fastq_uuid, add_on='?frame=raw&datastore=database', key=ff_key)
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


def sort_pedigree(pedigree):
    sorted_pedigree = sorted(pedigree, key=lambda x: x['relationship'] != 'proband') # make it proband-first
    sorted_pedigree[1:] = sorted(sorted_pedigree[1:], key=lambda x: x['relationship'] not in ['mother','father']) # parents next

    return sorted_pedigree


def pedigree_to_qc_pedigree(samples_pedigree):
    """extract pedigree for qc for every family member
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


def remove_parents_without_sample(samples_pedigree):
    individuals = [i['individual'] for i in samples_pedigree]
    for a_member in samples_pedigree:
        parents = a_member['parents']
        new_parents = [i for i in parents if i in individuals]
        a_member['parents'] = new_parents

    return samples_pedigree
