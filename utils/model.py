import pandas as pd
import gzip

# papers = 'data/document_parses/covid19_10k.json'


def compute_ncd(x: str, y: str) -> float:
    """
    Function to calculate the Normalized Compression Distance.
    param x: First Paper Text to compress.
    param y: Second Paper Text to compress.
    return: The distance between the encoded papers.
    """
    x = str(x).encode()
    y = str(y).encode()
    xy = x + y
    l_x = len(gzip.compress(x))
    l_y = len(gzip.compress(y))
    l_x_y = len(gzip.compress(xy))
    ncd = (l_x_y - min(l_x, l_y)) / max(l_x, l_y)
    return ncd


def k_similar_papers(paper_id: str, k: int, df: pd.DataFrame, txt_to_encode: str = 'body_text') -> pd.DataFrame:
    """
    Function that calculates k similar papers using NCD on given paper.
    :param paper_id: paper id.
    :param k: # of similar papers
    :param df: df holds features.
    :param txt_to_encode: which text data to encode
    :return: df with k similar papers.
    """
    similarity_df = pd.DataFrame(columns=["title", "paper_id", "abstract", "body_text", "ncd_distance"])
    similarity_df['title'] = df['title']
    similarity_df['paper_id'] = df['paper_id']
    similarity_df['abstract'] = df['abstract']
    similarity_df['body_text'] = df['body_text']

    x = df[df['paper_id'] == paper_id][txt_to_encode].to_numpy()
    similarity_df['ncd_distance'] = df[txt_to_encode].apply(lambda y: compute_ncd(x, y))

    similarity_df.sort_values(by='ncd_distance', inplace=True, ascending=True)

    return similarity_df[:k + 1]
