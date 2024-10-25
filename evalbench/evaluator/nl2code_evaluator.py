import json
import uuid
import datetime
import queue
import logging
import databases
import setup_teardown
from databases.util import is_bat_dataset
from dataset.evaloutput import EvalOutput
import subprocess
import json
import logging
import subprocess
import os

from eval_nl2code_request_pb2 import EvalInputRequest

class Nl2CodeEvaluator:

    def __init__(self, datasets_repo_path, app_repo_path):
        self.datasets_repo_path = datasets_repo_path
        self.app_repo_path = app_repo_path

    def apply_git_patch(self, patch_file):
        """Applies a git patch file from the datasets repo to the app repo."""
        try:
            patch_file_path = os.path.join(self.datasets_repo_path, patch_file)
            subprocess.run(["git", "-C", self.app_repo_path,"apply",  "--ignore-space-change", "--ignore-whitespace", patch_file_path], check=True)
            logging.info(f"Successfully applied git patch: {patch_file_path}")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error applying git patch: {e}")
            raise


    def insert_code(self, file_path, generated_code):
        """Replaces the entire content of the specified file with the generated code."""
        try:
            absolute_file_path = os.path.join(self.app_repo_path, file_path)

            with open(absolute_file_path, "w") as f:
                f.write(generated_code)

            logging.info(f"Successfully replaced code into: {absolute_file_path}.")

        except (FileNotFoundError, IOError) as e:
            logging.error(f"Error inserting code: {e}")
            raise


    def verify_code(self, verification_command):
        """
        Verifies the code using the provided command and returns the return code.
        """
        try:
            command_str = f"{verification_command} -f {self.app_repo_path}"
            result = subprocess.run(command_str, shell=True, text=True)
            logging.info("Code verification successful.")
            return result.returncode  # Capture and return the return code
        except subprocess.CalledProcessError as e:
            logging.error(f"Code verification failed: {e}")
            return e.returncode  # Capture and return the return code in case of error

    def reset_code(self):
        """Resets the code in the app repo to HEAD."""
        try:
            subprocess.run(["git", "-C", self.app_repo_path, "reset", "--hard", "HEAD"], check=True)
            logging.info("Successfully reset code to HEAD.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error resetting code: {e}")
            raise

    def apply_and_verify_code(self, eval_input_request):
        """
        Applies a git patch, inserts generated code, and verifies the result.

        Args:
            eval_input_request (EvalInputRequest): An instance of the EvalInputRequest proto.
        """
        try:
            file_path = eval_input_request.user_action.file_path
            generated_code = eval_input_request.generated_code
            self.insert_code(file_path, generated_code)
            return_code = self.verify_code(eval_input_request.verification_command) 
            logging.info(f"Successfully processed: {eval_input_request.id}")
        except Exception as e:
            logging.error(f"Error processing {eval_input_request.id}: {e}")
        finally:
            self.reset_code()
        return return_code
   
    def evaluate(self, dataset):
        run_time = datetime.datetime.now()
        passed = 0
        for eval_input in dataset:
            return_code = self.apply_and_verify_code(eval_input)
            if return_code==0:
                passed = passed + 1
        return run_time, passed, len(dataset)
    
    def return_golden_code(self, file_path):
        try:
            logging.info("Reading Golden code")
            absolute_file_path = os.path.join(self.app_repo_path, file_path)
            with open(absolute_file_path, 'r') as f:
                file_content = f.read()
            return file_content
        except FileNotFoundError:
            return None  

    def return_current_file(self, file_path, patch):
        """Reads the current open file..

        Args:
        app_repo_path: The path to the application repository within the container.
        file_path: The path to the file within the application repository.

        Returns:
         A string containing the file content.
        """
        try:
            self.apply_git_patch(patch)
            absolute_file_path = os.path.join(self.app_repo_path, file_path)
            with open(absolute_file_path, 'r') as f:
                file_content = f.read()
            return file_content
        except FileNotFoundError:
            return None  
        
