import pandas as pd
import math


def common_users_percentage(vec1, vec2):
    if (vec1.sum() == 0):
        return 0
    else:
        return ((vec1 * vec2).sum()) / (vec1.sum())


def cosine(vec1, vec2):
    r = ((vec1 * vec2).sum()) / (math.sqrt(vec1.sum()) * (math.sqrt(vec2.sum())))
    return r

def get_common_users_matrix(vectors):
    matrix = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    matrix = matrix.pivot(index='access_receiver', columns='signer_account_id', values='has_transactions')

    num_top_projects = len(vectors["access_receiver"].drop_duplicates())
    top_projects = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    top_projects = top_projects[vectors.has_transactions == 1].drop_duplicates().access_receiver.value_counts()
    top_projects = pd.DataFrame(top_projects.reset_index()["index"].head(num_top_projects))
    top_projects["key"] = 0
    top_projects = top_projects.set_index("key")
    top_projects.columns = ["project"]

    print("projects len : " + str(len(top_projects)))

    common_users_matrix = top_projects.join(top_projects, how="outer", lsuffix="1", rsuffix="2")
    common_users_matrix["value"] = 0
    common_users_matrix = common_users_matrix.pivot(index="project1", columns="project2", values="value")

    print("calculating common users")
    i = 0
    for p1 in top_projects.project:
        print(i)
        for p2 in top_projects.project:
            common_users_matrix.loc[p1, p2] = common_users_percentage(matrix.loc[p1, :].to_numpy(), matrix.loc[p2, :].to_numpy())
        i = i + 1

    return common_users_matrix

def get_cosine_matrix(vectors):
    matrix = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    matrix = matrix.pivot(index='access_receiver', columns='signer_account_id', values='has_transactions')

    num_top_projects = len(vectors["access_receiver"].drop_duplicates())
    top_projects = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    top_projects = top_projects[vectors.has_transactions == 1].drop_duplicates().access_receiver.value_counts()
    top_projects = pd.DataFrame(top_projects.reset_index()["index"].head(num_top_projects))
    top_projects["key"] = 0
    top_projects = top_projects.set_index("key")
    top_projects.columns = ["project"]

    print("projects len : " + str(len(top_projects)))

    cosine_matrix = top_projects.join(top_projects, how="outer", lsuffix="1", rsuffix="2")
    cosine_matrix["value"] = 0
    cosine_matrix = cosine_matrix.pivot(index="project1", columns="project2", values="value")

    print("calculating cosines")
    i = 0
    for p1 in top_projects.project:
        print(i)
        for p2 in top_projects.project:
            cosine_matrix.loc[p1, p2] = cosine(matrix.loc[p1, :].to_numpy(), matrix.loc[p2, :].to_numpy())
        i = i + 1

    return cosine_matrix

