import json
import os
import cotools
import shutil
import pandas as pd

DATASET_PATH = 'data/'
METADATA_PATH = 'data/metadata.csv'
MINI_DATASET_JSON_PATH = 'data/minidataset/'
MINI_DATASET = 'data/covid_20k_df.csv'
SIZE = 20000

def load_create_dataset()->pd.DataFrame:
    if os.path.isfile(MINI_DATASET):
        df = pd.read_csv(MINI_DATASET)
        print(f'loaded {MINI_DATASET}')
        return df
    else:
        metadata = load_metadata()
        print('loaded metadata')

        move_files(metadata)
        print('created `minidataset` folder')

        papers = cotools.Paperset(MINI_DATASET_JSON_PATH)
        df = load_papers_into_df(papers, size=SIZE)
        df.to_csv(MINI_DATASET, index=False)
        print(f'saved the final df to {MINI_DATASET}')
        return df

def load_metadata(metadata_path: str = METADATA_PATH, num_of_papers: int = 30000) -> pd.DataFrame:
    """
    Load Initial Amount of metadata papers.
    :param metadata_path: Path to metadata.
    :param num_of_papers: Initial # of metadata.
    :return: df contains num_of_papers metadata.
    """
    metadata = pd.read_csv(metadata_path)
    metadata.sort_values('publish_time', ascending=False, inplace=True)
    metadata.dropna(subset=['sha'], inplace=True)
    metadata = metadata[metadata['publish_time'] < '2022-04-15']
    metadata = metadata[metadata['sha'].map(len) == 40]
    metadata = metadata[metadata['pdf_json_files'].map(len) == 70]
    df = metadata[0:num_of_papers]
    df.set_index('sha', inplace=True)
    return df


def move_files(df: pd.DataFrame,
               dataset_path: str = DATASET_PATH,
               mini_dataset_json_path: str = MINI_DATASET_JSON_PATH) -> None:
    """
    Helper function to move initial amount of papers into new folder.
    :param df:  metadata papers
    :param dataset_path:  path of old path
    :param mini_dataset_json_path: new dataset path
    :return: None
    """
    if not os.path.isdir(mini_dataset_json_path):
        os.mkdir(mini_dataset_json_path)

    for path in df['pdf_json_files'].values:
        old_path = dataset_path + path
        new_path = mini_dataset_json_path + old_path.split('/')[-1]
        try:
            shutil.copyfile(old_path, new_path)

        except:
            continue




def load_papers_into_df(data: cotools.Paperset, size: int = 20000) -> pd.DataFrame:
    """
    Function that load papers and make some pre-processing-
        *Deleting papers with none data(Titles, paper_ids, body_text, abstract)
    Saves JSON file.
    :param size: DataFrame Size.
    :param data: 'cotools.Paperset' object that holds relevant papers
    :return: DataFrame of relevant papers
    """
    covid_df = pd.DataFrame(columns=["title", "paper_id", "abstract", "body_text"])

    for i, paper in enumerate(data):
        txt = []
        abstract = []
        if paper['metadata']['title'] is None or paper['metadata']['title'] == '':
            continue

        if paper['paper_id'] is None or paper['paper_id'] == '':
            continue

        for text in paper['body_text']:
            txt.append(text['text'])

        if not txt:
            continue

        for abstr in paper['abstract']:
            abstract.append(abstr['text'])

        if not abstract:
            continue

        # title
        covid_df.loc[i, 'title'] = paper['metadata']['title']

        # paper_id
        covid_df.loc[i, 'paper_id'] = paper['paper_id']

        # Abstract
        covid_df.loc[i]['abstract'] = ''.join(abstract)

        # Body text
        covid_df.loc[i]['body_text'] = ''.join(txt)

    covid_df[:size].to_json('data/document_parses/covid19_{}.json'.format(size))

    return covid_df[:size]
