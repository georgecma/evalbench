import os
import shutil
import logging
from git import Repo
from .base import Repository


class NL2SQLRepo(Repository):
    def __init__(self, repo_config):
        self.config = repo_config

    def clone(self):
        if 'repo_dir' not in self.config or 'repo_url' not in self.config:
            return
        repo_dir = self.config['repo_dir']
        repo_url = self.config['repo_url']
        if os.path.exists(repo_dir):
            logging.info(f"NL2SQL: Repository directory '{repo_dir}' exists. Deleting it...")
            shutil.rmtree(repo_dir)
        logging.info(f"NL2SQL: Cloning '{repo_url}' to '{repo_dir}'...")
        Repo.clone_from(repo_url, repo_dir)
