from dcicutils.ff_utils import post_metadata

from magma_smaht.create_metawfr import mwfr_from_input
from magma_smaht.utils import get_auth_key


def test_drr():
    input_ = [
    ]
    key = get_auth_key("staging")
    mwf_uuid = "df0466e1-356d-45bc-bd55-950704596a1b"
    input_arg = "input_files_r1_fastq_gz"
    mwfr = mwfr_from_input(mwf_uuid, input_, input_arg, key)
    post_response = post_metadata(mwfr, "MetaWorkflowRun", key)
    print(post_response)
