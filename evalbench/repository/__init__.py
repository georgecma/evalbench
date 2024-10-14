from .nl2sqlRepo import NL2SQLRepo


def get_repository(repo_config):
    return NL2SQLRepo(repo_config)
