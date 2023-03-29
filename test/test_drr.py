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


AUTH = get_keys()["msa"]


# def test_check_status():
#     uuid = "d829c0e7-6e53-4e5f-a3a7-0290d35a79db"
#     status_metawfr(uuid, AUTH)



# def test_make_sample_processing_meta_workflow_runs():
#     sample_processings = [
# 		"37406a8b-6b5f-4b69-a721-9256b0b6f7b1",
# 		"681499e2-53bb-4fc4-ae2c-2cf66341d727",
# 		"879545b3-3f6e-4d18-a1f9-cb87ca4bf41c",
# 		"ed65b890-ea3f-444d-a077-4daec6ca26a9",
# 		"cc99d260-dd7d-4b5e-ae27-a69ccb81d912",
# 		"5be2b5d1-49fc-4330-835f-22039cdffe13",
# 		"aa1dfa0b-a608-4773-be5e-d2163b0df118",
# 		"935e9c58-f71b-48ab-abb5-fe99eee28da1",
# 		"cb395cfd-86aa-4fbc-816f-ce6bb2568f96",
# 		"5d47d069-4c20-41ad-9f0b-bbc9d6edc027",
# 		"f5e9cb3c-3a86-4049-9cf9-055d34b15097",
# 		"907f3d1e-ba0d-4be8-bbea-f27e5a887e8b",
# 		"3e08a9e6-922a-4f99-931f-564d90c60bf0",
# 		"094bf382-c035-4952-9572-39a619b69a86",
# 		"2202cf30-a716-4859-b5df-dc42cd861bed",
# 		"b8e6f1f1-1609-4565-8a07-ae43cf9f4de9",
# 		"0c564a53-5e36-4c39-b20f-9cf66a5ef723",
# 		"4873a6d4-8a7f-42f6-841a-7f3f32bcb74f",
# 		"e17df274-d9fe-431e-9a61-51637f6012c5",
# 		"bb0956e2-3c04-4c2d-8adc-0e225909aba7",
# 		"fe72cbf8-5fbe-4ac0-9fae-3eda30505774",
# 		"2218ea50-4c5e-4554-a7c7-da3510829316",
# 		"efa85d80-30dd-4437-b01d-320aa655eec6",
# 		"ff06bce2-5c99-44cb-86e6-525e946e9ad0",
# 		"aa5e169d-edbb-428e-bbf2-4fb8db30ad67",
# 		"ff9fd1dc-0c77-47ee-82b4-1b26fd64192d",
# 		"601a143c-69fe-4958-9a91-32cd02a384ba",
# 		"e609dfc9-01b4-41d2-b869-fe60dd445471",
# 		"f214f27c-d428-46fb-8e5a-90ecd27fdfa4",
# 		"20b73848-ca51-4f89-9ca6-b8a62c476935",
# 		"a543b56d-10ce-4cd1-bedd-07be4e5035b5",
# 		"c9affab9-02fb-4f48-b283-abb05a961597",
# 		"8468bf74-c777-4646-8d2e-9bb1a6f8639d",
# 		"5c9c9b9a-d43a-4658-8f04-a651660a649a",
# 		"bd8810e7-2445-429e-a292-6d9d1856d720",
# 		"78ddbf15-42a7-46da-8890-dd22fae95951",
# 		"7970bb67-205a-4a21-ae13-e74dcbe43c42",
# 		"4638a852-1ee8-4fce-97a2-438b0aa8c224",
# 		"b52b5b9f-9e7d-4038-9900-5990e64554a4",
# 		"8b0cd45a-b71e-4ddb-8fd3-dad779b5f94a",
# 		"425c82ce-e3ae-46d8-b8b5-904d65e14b43",
# 		"1d584120-b0b0-4640-8190-806b121803f8",
# 		"3153f9c0-75a1-4306-93ee-218dffd81e9b",
# 		"8a867da6-e343-4c66-997b-3122e5c74628",
# 		"d497f35d-068b-4d5d-9d6e-559561143973",
# 		"15d65e47-fd37-4642-9e26-57dfabe1fff7",
# 		"0d145d80-142e-484c-b5b3-0df3346d82f2",
# 		"8973794c-d2cf-46bd-915d-da74ff921966",
# 		"2001fdbe-e961-4c5d-8bb3-b20935c33d7f",
# 		"3c1ad1cd-6666-4377-952f-c9dbf3d11ea8",
# 		"d8f2e84e-f5a7-41bc-821f-7225fac9a1e2",
# 		"36e94357-9dbe-4c52-8cde-915b80b30290",
# 		"c6fda993-2706-4c0b-93a2-2df4d6b50f42",
# 		"dcd740df-7b7c-433e-9037-e42128c06456",
# 		"42104fef-8bc1-4a82-8e30-814aedc9bbfc",
# 		"4fce2025-1c83-4467-9315-b13aeabb81da",
# 		"e3fc0893-8401-4e40-8245-5cee39f34d6b",
# 		"fe8dc610-05b0-4905-875d-d474737363ed",
# 		"f5fa6bda-cc32-4290-8b5e-a38596690027",
# 		"4c947de2-b366-410f-9f9d-f44e9a96fedc",
# 		"018d3cf1-ef41-450c-9ab4-bd3bc101b67b",
# 		"a9a3e01f-2038-4e5a-8953-4e2f38a9a3e5",
# 		"cdce3b2b-2e8c-49ff-b003-166704896763",
# 		"b5232753-d1c0-4096-a715-7ef9b2efcec9",
# 		"136c9cc7-d94b-4dfc-be11-a2a47a35efcf",
# 		"38bfcc9f-d0bd-4ed4-8bd2-cce4f6709eb1",
# 		"d2ce52fc-2bba-4ee9-b036-8168228c6e97",
# 		"32ccdb34-045e-406b-b025-af1fbe05ac63",
# 		"3bf94091-6da8-41ca-8d64-b5fce912d99d",
# 		"3fd57cb9-4ff8-4e37-bdac-2883c3eba281",
# 		"4810371c-b2e0-4b57-b25f-8159716c9880",
# 		"7b0a939a-c32c-424c-a779-d4c7da2214e7",
# 		"97c36448-a246-4d17-9db5-fa3f4ce7df8b",
# 		"4ca7e161-be7c-4a1c-9f18-ae91e68ef5f1",
# 		"7f646775-3c2e-4bcf-9fe6-a903e259f200",
# 		"6327666f-a5a0-407e-b73d-1c69d84c5fd4",
# 		"445e9ba7-d2a4-463a-91ac-6994bd5e3b59",
# 		"83bdf383-0362-428e-844b-3ca05f821c29",
# 		"11c4dd30-5f14-4468-bcea-fdb47215ffb0"
#     ]
#     meta_workflow = "GAPMW4A2ABQ9"
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
#         "GAPSAGVKW4N3",
#         "GAPSAY3SBOJO",
#         "GAPSA3I9B8KB",
#         "GAPSAG8NW4FV",
#         "GAPSAS85536P",
#         "GAPSAUQVRYHE",
#         "GAPSABXNDCOX",
#         "GAPSA9VKBOYM",
#         "GAPSAAGBDV1N",
#         "GAPSAEQN7YUT",
#         "GAPSAT3IOOS8",
#         "GAPSAG2GB842",
#         "GAPSABGJMKR4",
#         "GAPSAYOYT6PW",
#         "GAPSAJKB29JX",
#         "GAPSAG5SA5XF",
#         "GAPSA4IR8UR5",
#         "GAPSAITXS98P",
#         "GAPSANN46EBE",
#         "GAPSAEH1W1SL",
#         "GAPSAWZEVAHE",
#         "GAPSAF9JBJ4V",
#         "GAPSA6BBPV8M",
#         "GAPSAU45Y3XP",
#         "GAPSAS323L1Q",
#         "GAPSAMO8HNQR",
#         "GAPSAFTFSQDE",
#         "GAPSAPYRTDNN",
#         "GAPSAW1AUPT1",
#         "GAPSAYEC4PV9",
#         "GAPSAFB189I5",
#         "GAPSATJKCGBD",
#         "GAPSAUJCO1KX",
#         "GAPSA4QCMK2R",
#         "GAPSAZB5CGNA",
#         "GAPSACH23D8D",
#         "GAPSATZ19YOY",
#         "GAPSAY85VZMS",
#         "GAPSA8V5PA5Q",
#         "GAPSAW7CQMV3",
#         "GAPSAKVKMJDQ",
#         "GAPSA5Z99ES1",
#         "GAPSAD5Y2TAV",
#         "GAPSAQMTB61Z",
#         "GAPSAX63QE4W",
#         "GAPSA9Z1PX2A",
#         "GAPSAU944QBU",
#         "GAPSA1U48LFL",
#         "GAPSAY6UUQ5N",
#         "GAPSAJY5434F",
#         "GAPSAWO8WJ5Y",
#         "GAPSAVI9MRSP",
#         "GAPSAXMC63JQ",
#         "GAPSAHIXAYSQ",
#         "GAPSAC5NWBU1",
#         "GAPSABP5C6P4",
#         "GAPSAI6WS3A7",
#         "GAPSAVAWUKP6",
#         "GAPSAVWPO6TV",
#         "GAPSAVW7RKXG",
#         "GAPSAPGQSJXP",
#         "GAPSAMNX63XO",
#         "GAPSAPTG8QZ7",
#         "GAPSAOK2JSSU",
#         "GAPSAFURG7LF",
#         "GAPSAFQHXNTU",
#         "GAPSA5R5GFFI",
#         "GAPSAA9FRG78",
#         "GAPSAE4CIYRQ",
#         "GAPSASL7NZTN",
#         "GAPSASUVE2W3",
#         "GAPSAHC1USY8",
#         "GAPSADEH5ELD",
#         "GAPSAIMZQ1YP",
#         "GAPSAMIHZOEZ",
#         "GAPSAVF8DCLG",
#         "GAPSAIG2RYFH",
#         "GAPSAH2TOIHN",
#         "GAPSAJBE8Q7U"
# 	]
#     meta_workflow = "GAPMWTIM8PR2"
#     meta_workflow_runs = []
#     for sample in samples:
#         mwfr = MetaWorkflowRunFromSample(
#             sample, meta_workflow, AUTH
#         )
#         meta_workflow_runs.append(mwfr)
#     import pdb; pdb.set_trace()
#     for meta_workflow_run in meta_workflow_runs:
#         meta_workflow_run.post_and_patch()
