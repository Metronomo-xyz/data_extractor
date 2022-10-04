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


def get_cosine_matrix(vectors):
    matrix = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    matrix = matrix.pivot(index='access_receiver', columns='signer_account_id', values='has_transactions')

    print("calculating signers")
    num_top_users = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    num_top_users = len(num_top_users[vectors.has_transactions == 1].drop_duplicates().signer_account_id.value_counts())
    signers = vectors[["signer_account_id", "access_receiver", "has_transactions"]]
    signers = signers[vectors.has_transactions == 1].drop_duplicates().signer_account_id.value_counts()
    signers = pd.DataFrame(signers.reset_index()["index"].head(num_top_users))
    signers["key"] = 0
    signers = signers.set_index("key")
    signers.columns = ["signer_account_id"]

    print("calculating users cosines")

    cosine_users_matrix = signers.join(signers, how="outer", lsuffix="1", rsuffix="2")
    cosine_users_matrix["value"] = 0
    cosine_users_matrix = cosine_users_matrix.pivot(index="signer_account_id1", columns="signer_account_id2",values="value")

    i = 0
    for s1 in signers.signer_account_id:
        print(i)
        for s2 in signers.signer_account_id:
            cosine_users_matrix.loc[s1, s2] = cosine(matrix.loc[:, s1].to_numpy(), matrix.loc[:, s2].to_numpy())
        i = i + 1

    return cosine_users_matrix

