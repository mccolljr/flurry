import os
import sys
import semver
import pathlib
import argparse
import subprocess
from github import Github


def parse_args():
    token = os.environ.get("GH_ACCESS_TOKEN", "")
    tag = os.environ.get("GITHUB_REF_NAME", "")
    assets = sys.argv[1:]
    return token, tag, assets


def main():
    token, tag, assets = parse_args()
    version = semver.VersionInfo.parse(tag)
    api = Github(token)
    repo = api.get_repo("mccolljr/flurry")
    release = repo.create_git_release(
        tag=str(version),
        name=str(version),
        draft=False,
        message=f"Release of version {version}",
        prerelease=version.prerelease is not None,
    )
    try:
        for asset in assets:
            path = pathlib.Path(asset)
            label = path.name
            release.upload_asset(str(path), label)
    except:
        release.delete_release()
        raise
    print(f"released {version}")


if __name__ == "__main__":
    main()
