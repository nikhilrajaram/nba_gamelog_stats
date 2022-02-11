import datetime
import glob
import json
import re
from collections import Counter

import numpy as np
import pandas as pd


def get_game_id(row: pd.core.series.Series):
    home, away = (row['team_id'], row['opp_id']) if row['game_location'] != "@" else (row['opp_id'], row['team_id'])
    return f'{row["date_game"]}-{home}-{away}'


def chain_queries(*args):
    non_null_args = list(filter(lambda x: x is not None, args))
    if len(non_null_args) == 0:
        raise Exception('at least one query must be non null')

    return ' & '.join(map(lambda x: f'({x})', non_null_args))


class GamelogDataFrame(pd.DataFrame):
    INFORMATIONLESS_LOGS = {'Did Not Play', 'Inactive', 'Did Not Dress', 'Not With Team', 'Player Suspended'}
    STATS = ['fg', 'fga', 'fg_pct', 'fg3', 'fg3a', 'fg3_pct', 'ft', 'fta', 'ft_pct', 'orb', 'drb', 'trb', 'ast', 'stl',
             'blk', 'tov', 'pf', 'pts', 'game_score', 'plus_minus']

    # normal properties
    _metadata = ["INFORMATIONLESS_LOGS", "STATS"]

    @property
    def _constructor(self):
        return GamelogDataFrame

    def __init__(self, df):
        super().__init__(df)

    @staticmethod
    def read_from_glob(index_file: str, glob_pattern: str):
        index = GamelogDataFrame._get_index(index_file)
        files = glob.glob(glob_pattern)
        key_match = re.compile(glob_pattern.replace('*', '([0-9]+)'))
        dfs = []
        for file in files:
            key = key_match.fullmatch(file).group(1)
            _df = pd.read_csv(file)
            _df['player_name'] = index[key]
            _df = _df[~_df['pts'].isin(GamelogDataFrame.INFORMATIONLESS_LOGS)]
            dfs.append(_df)
        df = GamelogDataFrame(pd.concat(dfs))
        df['game_id'] = df.apply(get_game_id, axis=1)
        return GamelogDataFrame._infer_dtypes(df)

    @staticmethod
    def _get_index(file):
        with open(file, 'r') as f:
            return json.load(f)

    @staticmethod
    def _infer_dtypes(_df):
        for col in _df.columns:
            try:
                _df[col] = _df[col].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
            except:
                pass
            try:
                _df[col] = _df[col].astype(np.float64)
            except:
                pass
        return _df

    def _apply_query(self, query=None):
        return self if query is None else self.query(query)

    def get_record(self, query=None):
        df = self._apply_query(query)
        c = Counter(
            df.groupby('date_game')
                .apply(lambda x: x['game_result'].str.contains('W').all())
                .values
        )
        return c[True], c[False]

    def with_player(self, player, query=None):
        df = self._apply_query(query)
        return df.groupby('date_game') \
            .filter(lambda x: x['player_name'].str.contains(player).any())

    def without_player(self, player, query=None):
        df = self._apply_query(query)
        return df.groupby('date_game') \
            .filter(lambda x: not x['player_name'].str.contains(player).any())

    def game_totals(self, team, team_stat, query=None):
        query = chain_queries(query, f'team_id == "{team}" | opp_id == "{team}"')
        return self.query(query)\
                .groupby('game_id')[team_stat]\
                .sum()

    def over_unders(self, home, away, team_stat, query=None):
        df = self._apply_query(query)
        home_totals = df.game_totals(home, team_stat)
        away_totals = df.game_totals(away, team_stat)

        return pd.concat((home_totals, away_totals))
