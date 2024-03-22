import logging

import pandas as pd

from src.datamodel.CodeFormat import CodeFormat
from src.datamodel.DataColumns import CommonColumns as cc
from src.datamodel.Event import EventConstant, EventCategory
from src.datamodel.ExperimentConfig import ExperimentConfig
from src.repository.CodeDescriptionRepository import CodeDescriptionRepository
from src.util.FileProvider import FileProvider


class BuildEventsMetadata:

    def __init__(self, cd_repo: CodeDescriptionRepository):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cd_repo = cd_repo
        self.fp = FileProvider()

    def execute(self, experiment_config: ExperimentConfig):
        self.logger.debug(f'execute')
        codes_df = self.build_codes_metadata(experiment_config.levels)
        file_dir, file_name = self.fp.events_metadata_file_location(experiment_config.outcome_dir)
        self.fp.save_dataframe_file(df=codes_df, file_dir=file_dir, filename=file_name)

    def build_codes_metadata(self, levels: list):
        self.logger.debug('build_codes_metadata')
        events = [e for level in levels for e in level.events]
        codes = [(c, EventCategory.from_string(e.category).value.code_systems)
                 for e in events for c in e.codes]

        df = self.__get_code_metadata_job(codes)
        if df is None or df.empty:
            return None
        res_dfs = list()
        # join negative codes according to experiment config
        for level in levels:
            for event in level.events:
                sub_df = df[df[cc.code].isin(event.codes)]
                if event.negation:
                    sub_dict = {
                        cc.code: [
                            CodeFormat.simple_to_negative(sub_df[cc.code].unique().tolist())],
                        cc.description: [f"{CodeFormat.negation_word} "
                                         f"[{'| '.join(sub_df[cc.code_description].unique().tolist())}]"]
                    }
                    sub_df = pd.DataFrame(sub_dict)

                sub_df[cc.category] = event.category.lower()
                sub_df[cc.event_id] = event.id
                sub_df[cc.event_name] = event.name
                sub_df[cc.level] = level.name
                res_dfs.append(sub_df)

        df = pd.concat(res_dfs)

        df = df[[cc.code, cc.category, cc.code_description, cc.event_name, cc.event_id, cc.level]]. \
            drop_duplicates()

        return df

    def __get_code_metadata_job(self, curr_codes: list):
        """curr_codes is a list of tuples (code, list of code systems)"""
        self.logger.debug(f'get code metadata for codes {curr_codes}')
        dfs = list()
        # process DEATH code
        if [c for c in curr_codes if c[0] == EventConstant.DEATH]:
            dfs.append(
                pd.DataFrame({cc.code: [EventConstant.DEATH], cc.code_description: [EventConstant.DEATH]})
            )
            curr_codes = [c for c in curr_codes if c[0] != EventConstant.DEATH]
        # process all passed codes
        dfs.append(self.cd_repo.get_code_description(curr_codes))

        return pd.concat(dfs)
