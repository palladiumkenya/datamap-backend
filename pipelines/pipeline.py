import shutil
import asyncio
import luigi
import os
from datetime import datetime, date
from routes.data_extraction_api import extract_data_pipeline
from routes.usl_data_transmission_api import send_data_pipeline




OUTPUT_DIR = "./pipelines/task_output"


def clear_folder(folder_path):
    if os.path.exists(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # Delete file or link
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Delete subfolder
            except Exception as e:
                print(f'❌ Failed to delete {file_path}. Reason: {e}')
        print(f"✅ Cleared all contents of: {folder_path}")
    else:
        print(f"📂 Folder does not exist: {folder_path}")




class LoadLabs(luigi.Task):
    run_date = luigi.DateParameter(default=date.today())

    def output(self):
        return luigi.LocalTarget(f"{OUTPUT_DIR}/LoadLabs_job.txt")

    def run(self):
        # run extraction pipeline
        asyncio.run(extract_data_pipeline("lab"))
        with open(self.output().path, 'w', encoding='utf-8') as f:
            f.write(f"✅ LoadLabs job completed at {datetime.now()}\n")
        print("✔️ LoadLabs job complete")


class LoadEnrolments(luigi.Task):
    run_date = luigi.DateParameter(default=date.today())

    def requires(self):
        return LoadLabs(run_date=self.run_date)

    def output(self):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        return luigi.LocalTarget(f"{OUTPUT_DIR}/LoadEnrolments_job.txt")

    def run(self):
        # run extraction pipeline
        asyncio.run(extract_data_pipeline("enrolments"))
        with open(self.output().path, 'w', encoding='utf-8') as f:
            f.write(f"✅ LoadEnrolments completed at {datetime.now()}\n")

        print("✔️ SendLabs complete")


class StartLoad(luigi.Task):
    run_date = luigi.DateParameter(default=date.today())

    def requires(self):
        return LoadEnrolments(run_date=self.run_date)

    def output(self):
        clear_folder(OUTPUT_DIR)  # Clear the whole output folder (optional)

        return luigi.LocalTarget(f"{OUTPUT_DIR}/StartLoad.txt")

    def run(self):
        with open(self.output().path, 'w', encoding='utf-8') as f:
            f.write(f"✅ StartLoad completed at {datetime.now()}\n")
        print("✔️ StartLoad complete")
# python -m luigi --module pipelines.pipeline TaskC --scheduler-host localhost
# routes.data_extraction_api.extract_data_pipeline
