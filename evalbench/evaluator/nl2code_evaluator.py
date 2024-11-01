import json
import datetime
import logging
from dataset.nl2code_evaloutput import EvalOutput
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
            subprocess.run(["git", "-C", self.app_repo_path, "apply",
                           "--ignore-space-change", "--ignore-whitespace", patch_file_path], check=True)
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

            logging.info(
                f"Successfully replaced code into: {absolute_file_path}.")

        except (FileNotFoundError, IOError) as e:
            logging.error(f"Error inserting code: {e}")
            raise


    def verify_code(self, verification_command):
        """
        Verifies the code using the provided command.
        Returns a dictionary containing the return code, STDOUT, and STDERR.
        """
        result_dict = {}
        try:
            command_str = f"{verification_command} -f {self.app_repo_path}"
            result = subprocess.run(command_str, shell=True, text=True, capture_output=True)
            result_dict['return_code'] = result.returncode
            result_dict['stdout'] = result.stdout
            result_dict['stderr'] = result.stderr
            logging.info("Code verification successful.")
            logging.info(f"Result: {result_dict}") 
            return result_dict

        except subprocess.CalledProcessError as e:
            result_dict['return_code'] = e.returncode
            result_dict['stdout'] = e.stdout
            result_dict['stderr'] = e.stderr
            logging.error(f"Code verification failed: {e}")
            logging.error(f"Result: {result_dict}")
            return result_dict


    def is_compilable(self, build_command):
        """
        Compiles the code using the provided command and returns the return 
        boolean to represent whether code can be compiled or not.
        """
        try:
            command_str = f"{build_command} -f {self.app_repo_path}"
            subprocess.run(command_str, shell=True, text=True)
            logging.info("Code compilation successful.")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Code verification failed: {e}")
            return False  # Capture and return the return code in case of error

    def reset_code(self):
        """Resets the code in the app repo to HEAD."""
        try:
            subprocess.run(["git", "-C", self.app_repo_path,
                           "reset", "--hard", "HEAD"], check=True)
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
            result = self.verify_code(
                eval_input_request.verification_command)
            logging.info(f"Successfully processed: {eval_input_request.id}")
        except Exception as e:
            logging.error(f"Error processing {eval_input_request.id}: {e}")
        finally:
            self.reset_code()
        return result

    def evaluate(self, dataset):
        eval_outputs = []
        scoring_results = []

        run_time = datetime.datetime.now()
        passed = 0
        for eval_input in dataset:
            score = {
                "syntactic_correctness": False,
                "semantic_correctness": False,
                "job_id": None,
                "run_time": None,
                "latency": None
            }

            verification_result = self.apply_and_verify_code(eval_input)
            if verification_result['return_code'] == 0:
                passed = passed + 1
                score["syntactic_correctness"] = True
                score["semantic_correctness"] = True
            else:
                score["semantic_correctness"] = False
                score["syntactic_correctness"] = self.is_compilable(
                    eval_input.build_command)

            score["return_code"] = verification_result['return_code']
            job_id = eval_input.job_id
            eval_output = EvalOutput(eval_input)
            eval_output["job_id"] = job_id
            eval_output["run_time"] = run_time
            eval_output["stdout"] = verification_result['stdout']
            eval_output["stderr"] = verification_result['stderr']
            eval_outputs.append(eval_output)
            score["job_id"] = job_id
            score["run_time"] = run_time
            score["latency"] = eval_input.dbcodegen_time
            score["id"] = eval_input.id
            scoring_results.append(score)

        with open(f"/tmp/eval_output_{job_id}.json", "w") as f:
            json.dump(eval_outputs, f, sort_keys=True, indent=4, default=str)

        with open(f"/tmp/score_result_{job_id}.json", "w") as f:
            json.dump(scoring_results, f, sort_keys=True,
                      indent=4, default=str)

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
