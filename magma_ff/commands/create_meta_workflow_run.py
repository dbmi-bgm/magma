import argparse

from ..create_metawfr import create_meta_workflow_run
from ..utils import get_auth_key


def main() -> None:
    parser = argparse.ArgumentParser(description="Create MetaWorkflowRun")
    parser.add_argument(
        "input_item_identifier", help="Identifier for item for input to MWFR"
    )
    parser.add_argument(
        "meta_workflow_identifier", help="MetaWorkflow identifier for input to MWFR"
    )
    parser.add_argument(
        "-e", "--auth-env", default="default", help="Env name in ~/.cgap-keys.json"
    )
    parser.add_argument(
        "--no-post", action="store_true", help="Do not POST the MWFR created"
    )
    parser.add_argument(
        "--no-patch",
        action="store_true",
        help="Do not PATCH the input item with the MWFR created",
    )
    args = parser.parse_args()
    auth_key = get_auth_key(args.auth_env)
    post = not args.no_post
    patch = not args.no_patch
    create_meta_workflow_run(
        args.input_item_identifier,
        args.meta_workflow_identifier,
        auth_key,
        post=post,
        patch=patch,
    )


if __name__ == "__main__":
    main()
