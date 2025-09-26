from typing import Any, List
import datetime
from work import promptgenwork
from work import sqlgeninteractwork
from work import interactsqlexecwork
from work import scorework
from work import vuserwork
from mp import mprunner
import concurrent.futures
from dataset.evalinteractinput import EvalInteractInputRequest, InteractionType
from dataset.evalinteractoutput import EvalInteractOutput
from evaluator import virtualuser
from evaluator.progress_reporter import (
    record_successful_prompt_gen,
    record_successful_sql_gen,
    record_successful_sql_exec,
    record_successful_scoring,
)
from queue import Queue
from databases import DB
from util.interactutil import check_response, print_interact, write_item, read_item
from util import truncateExecutionOutputs
import logging


class InteractEvaluator:
    def __init__(
        self,
        config,
    ):
        self.config = config
        runner_config = self.config.get("runners", {})
        self.promptgen_runners = runner_config.get("promptgen_runners", 10)
        self.sqlgen_runners = runner_config.get("sqlgen_runners", 10)
        self.vuser_runners = runner_config.get("vuser_runners", 10)
        self.sqlexec_runners = runner_config.get("sqlexec_runners", 10)
        self.scoring_runners = runner_config.get("scoring_runners", 10)

    def evaluate(
        self,
        dataset: List[EvalInteractInputRequest],
        db_queue: Queue[DB],
        prompt_generator,
        model_generator,
        job_id: str,
        run_time: datetime.datetime,
        progress_reporting,
        global_models,
        core_db,
    ):
        eval_outputs: List[Any] = []
        scoring_results: List[Any] = []

        self.vuser = virtualuser.VUser(self.config, global_models, core_db)
        self.promptrunner = mprunner.MPRunner(self.promptgen_runners)
        self.genrunner = mprunner.MPRunner(self.sqlgen_runners)
        self.vuser_runner = mprunner.MPRunner(self.vuser_runners)
        self.sqlrunner = mprunner.MPRunner(self.sqlexec_runners)
        self.scoringrunner = mprunner.MPRunner(self.scoring_runners)
        prompt_generator.setup()
        self.promptrunner.futures.clear()
        self.genrunner.futures.clear()
        self.vuser_runner.futures.clear()
        self.sqlrunner.futures.clear()
        self.scoringrunner.futures.clear()

        for eval_input in dataset:
            eval_output = EvalInteractOutput(eval_input)
            eval_output["job_id"] = job_id
            eval_output["run_time"] = run_time
            self.interact_loop(
                eval_output,
                prompt_generator,
                model_generator,
                progress_reporting,
                global_models,
                core_db,
                db_queue,
                eval_outputs,
                scoring_results,
            )
        if db_queue:
            while not db_queue.empty():
                db = db_queue.get()
                db.close_connections()
        return eval_outputs, scoring_results

    def interact_loop(
        self,
        eval_output,
        prompt_generator,
        model_generator,
        progress_reporting,
        global_models,
        core_db,
        db_queue,
        eval_outputs,
        scoring_results,
    ):
        terminate_flag = False
        max_turn = eval_output["payload"]["max_turn"]
        eval_output["payload"]["step_type"] = InteractionType.LLM_QUESTION

        while eval_output["payload"]["turn"] < max_turn and not terminate_flag:
            eval_output["payload"]["turn"] = eval_output["payload"]["turn"] + 1
            logging.info(
                "**************** Instance: "
                + str(eval_output["payload"]["instance_id"])
                + ":Turn:"
                + str(eval_output["payload"]["turn"])
                + " ****************"
            )

            # Make us an LLM side prompt
            work = promptgenwork.SQLPromptGenWork(prompt_generator, eval_output)
            self.promptrunner.execute_work(work)

            # Generate SQL or question
            for future in concurrent.futures.as_completed(self.promptrunner.futures):
                self.promptrunner.futures.remove(future)
                eval_output = future.result()
                record_successful_prompt_gen(progress_reporting)
                work = sqlgeninteractwork.SQLGenInteractWork(
                    model_generator, eval_output
                )
                self.genrunner.execute_work(work)

            # Disambiguate question
            for future in concurrent.futures.as_completed(self.genrunner.futures):
                self.genrunner.futures.remove(future)
                eval_output = future.result()
                record_successful_sql_gen(progress_reporting)
                # Check if we got SQL
                extracted_response, terminate_flag = check_response(
                    eval_output["payload"]
                )
                if terminate_flag:
                    work = interactsqlexecwork.InteractSQLExecWork(
                        core_db, self.config, eval_output, db_queue
                    )
                    self.sqlrunner.execute_work(work)
                else:
                    work = vuserwork.VUserWork(self.vuser, eval_output)
                    self.vuser_runner.execute_work(work)

            # Execute SQL
            for future in concurrent.futures.as_completed(self.vuser_runner.futures):
                self.vuser_runner.futures.remove(future)
                eval_output = future.result()
                record_successful_sql_exec(progress_reporting)
                eval_output["payload"]["step_type"] = InteractionType.LLM_ANSWER

            for future in concurrent.futures.as_completed(self.sqlrunner.futures):
                self.sqlrunner.futures.remove(future)
                eval_output = future.result()
                record_successful_sql_exec(progress_reporting)
                work = scorework.ScorerWork(
                    self.config, eval_output, scoring_results, global_models
                )
                self.scoringrunner.execute_work(work)

            for future in concurrent.futures.as_completed(self.scoringrunner.futures):
                eval_output = future.result()
                logging.info(f"Scoring {eval_output['payload']['instance_id']}")

                record_successful_scoring(progress_reporting)
                truncateExecutionOutputs(
                    eval_output,
                    self.config,
                )
                eval_outputs.append(eval_output)
