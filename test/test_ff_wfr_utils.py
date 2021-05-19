from magma.ff_wfr_utils import FFWfrUtils
import pytest


@pytest.fixture
def fake_wfr_output():
    return [{'a':'b', 'c':'d',
            'type': 'Output processed file',
            'value': {'uuid': 'someuuid'},
            'workflow_argument_name': 'somearg'},
            {'a2':'b2', 'c2':'d2',
            'type': 'Output processed file',
             'value': {'uuid': 'someuuid2'},
             'workflow_argument_name': 'somearg2'},
            {'a3':'b3', 'c3':'d3',
             'type': 'Output QC file',
             'workflow_argument_name': 'somearg3'}]

def test_FFWfrUtils():
    ff = FFWfrUtils('fourfront-cgapwolf')
    assert ff.env == 'fourfront-cgapwolf'

def test_ff_key():
    """This test requires connection"""
    ff = FFWfrUtils('fourfront-cgapwolf')
    assert ff.ff_key.get('server') == 'http://fourfront-cgapwolf.9wzadzju3p.us-east-1.elasticbeanstalk.com'

def test_wfr_metadata():
    ff = FFWfrUtils('fourfront-cgapwolf')
    ff._metadata['jobid'] = {'a': 'b'}
    assert ff.wfr_metadata('jobid') == {'a': 'b'}

def test_wfr_output():
    ff = FFWfrUtils('fourfront-cgapwolf')
    ff._metadata['jobid'] = {'output_files': [{'b': 'c'}]}
    assert ff.wfr_output('jobid') == [{'b': 'c'}]

def test_wfr_run_status():
    ff = FFWfrUtils('fourfront-cgapwolf')
    ff._metadata['jobid'] = {'run_status': 'complete'}
    assert ff.wfr_run_status('jobid') == 'complete'

def test_filter_wfr_output_minimal_processed(fake_wfr_output):
    filtered_output = FFWfrUtils.filter_wfr_output_minimal_processed(fake_wfr_output)
    assert filtered_output == [{'uuid': 'someuuid', 'workflow_argument_name': 'somearg'},
                               {'uuid': 'someuuid2', 'workflow_argument_name': 'somearg2'}]

def test_get_minimal_processed_output(fake_wfr_output):
    ff = FFWfrUtils('fourfront-cgapwolf')
    ff._metadata['jobid'] = {'output_files': fake_wfr_output}
    final_out = ff.get_minimal_processed_output('jobid')
    assert final_out == [{'uuid': 'someuuid', 'workflow_argument_name': 'somearg'},
                         {'uuid': 'someuuid2', 'workflow_argument_name': 'somearg2'}]
