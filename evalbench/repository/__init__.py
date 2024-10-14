from .nldRepo import NLDRepo


def get_repository(repo_config):
    return NLDRepo(repo_config)
