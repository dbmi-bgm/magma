import json
from pathlib import Path

from magma_ff.create_metawfr import (
    MetaWorkflowRunFromSample,
    MetaWorkflowRunFromSampleProcessing,
)
from magma_ff.status_metawfr import status_metawfr


def get_keys():
    with Path.home().joinpath(".cgap-keys.json").open() as f:
         keys = json.load(f)
    return keys


AUTH = get_keys()["mgb"]


def test_check_status():
    uuid = "d829c0e7-6e53-4e5f-a3a7-0290d35a79db"
    status_metawfr(uuid, AUTH)



# def test_make_sample_processing_meta_workflow_runs():
#     sample_processings = [
#         "f23dc610-6093-4476-a4ce-6ba2b081e949"
#     ]
#     meta_workflow = "GAPMW8YO637M"
#     meta_workflow_runs = []
#     for sample_processing in sample_processings:
#         mwfr = MetaWorkflowRunFromSampleProcessing(
#             sample_processing, meta_workflow, AUTH
#         )
#         meta_workflow_runs.append(mwfr)
#     import pdb; pdb.set_trace()
#     for meta_workflow_run in meta_workflow_runs:
# #        meta_workflow_run.post_meta_workflow_run()
#         meta_workflow_run.post_and_patch()


# def test_make_sample_meta_workflow_run():
#     samples = [
#         "/samples/GAPSA3ZPRL16/",
#         "/samples/GAPSA2P2HFMU/",
#         "/samples/GAPSAWAVMKSO/",
#         "/samples/GAPSAQPLQ3J1/",
#         "/samples/GAPSAOC4TJ3D/",
#         "/samples/GAPSA7KGEBUD/",
#         "/samples/GAPSAEHPQ5SP/",
#         "/samples/GAPSA7DWCFJA/",
#         "/samples/GAPSAG51QC4K/",
#         "/samples/GAPSAYTFL4QJ/",
#         "/samples/GAPSAWI8Z5TY/",
#         "/samples/GAPSAJAB3ZAJ/",
#         "/samples/GAPSAA8IV39D/",
#         "/samples/GAPSA6VJ3F7O/",
#         "/samples/GAPSAA8FBUH7/",
#         "/samples/GAPSAORCCGPB/",
#         "/samples/GAPSA9CXTHHR/",
#         "/samples/GAPSA6GEKIPA/",
#         "/samples/GAPSA52RCJWX/",
#         "/samples/GAPSA7XALEWW/",
#         "/samples/GAPSALSGRM97/",
#         "/samples/GAPSANMEOSOX/",
#         "/samples/GAPSA3IRW4LZ/",
#         "/samples/GAPSASG9Q5HX/",
#         "/samples/GAPSA195G8EK/",
#         "/samples/GAPSAZMQ2OSD/",
#         "/samples/GAPSA4TIKG1M/",
#         "/samples/GAPSAJ8J44DA/",
#         "/samples/GAPSAI3MVC6B/",
#         "/samples/GAPSAE1V1YKM/",
#         "/samples/GAPSAH92UECP/",
#         "/samples/GAPSA6ZOQTEL/",
#         "/samples/GAPSAG3DPYKM/",
#         "/samples/GAPSA8YET76O/",
#         "/samples/GAPSA64X1VKP/",
#         "/samples/GAPSAPSWI766/",
#         "/samples/GAPSAP6R3E36/",
#         "/samples/GAPSA5AG594U/",
#         "/samples/GAPSAXPZFNEX/",
#         "/samples/GAPSA3P8YLKA/",
#         "/samples/GAPSAY2MRAMI/",
#         "/samples/GAPSAYGCST7P/",
#         "/samples/GAPSAEJPM7RQ/",
#         "/samples/GAPSA81RY9V7/",
#         "/samples/GAPSAGTOZJA7/",
#         "/samples/GAPSA3EWV2GS/",
#         "/samples/GAPSA1DFR97U/",
#         "/samples/GAPSA5B2JCML/",
#         "/samples/GAPSA1UNACKD/",
#         "/samples/GAPSA6VMLF11/",
#         "/samples/GAPSA7R5UCJO/",
#         "/samples/GAPSATBJ4RHC/",
#         "/samples/GAPSAY7MHISO/",
#         "/samples/GAPSAIRSA95W/",
#         "/samples/GAPSA83FV7Z5/",
#         "/samples/GAPSA29OYOI3/",
#         "/samples/GAPSAWK8JLQ2/",
#         "/samples/GAPSA5DVAX9S/"
# 	]
#     meta_workflow = "GAPMWTIM8PR2"
#     for sample in samples:
#         mwfr = MetaWorkflowRunFromSample(
#             sample, meta_workflow, AUTH
#         )
#         mwfr.post_and_patch()
