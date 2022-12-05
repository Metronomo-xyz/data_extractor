import pandas as pd
import math
import datetime



def getPowerUsers(data, nft_contract_id=None):
    if (nft_contract_id):
        offers = data[((data["kind"] == "make_offer") & (data["nft_contract_id"] == nft_contract_id))]
        #TODO: create normalizers classes

        print("offers : ")
        monetary = offers[["nft_contract_id", "tx_sender", "price"]].groupby(["nft_contract_id", "tx_sender"]).sum()
        parameters_df = monetary

        def getRecency(ts):
            return (datetime.date.today() - datetime.datetime.strptime(ts.split(".")[0],
                                                                            "%Y-%m-%dT%H:%M:%S").date()).days
        offers["recency"] = offers["timestamp"].apply(getRecency)
        recency = offers[["nft_contract_id", "tx_sender", "recency"]].groupby(["nft_contract_id", "tx_sender"]).min()
        parameters_df = parameters_df.join(recency)

        frequency = offers[["nft_contract_id", "tx_sender", "receipt_id"]].groupby(["nft_contract_id", "tx_sender"]).count()
        parameters_df = parameters_df.join(frequency)

        parameters_df.columns = ["monetary", "recency", "frequency"]

        def normalize(x):
            #    print(x)
            c = x["nft_contract_id"]
            sender = x["tx_sender"]
            m = x.monetary
            f = x.frequency
            r = x.recency
            df = parameters_df.loc[c]

            if len(df > 1):
                m_mean = df.monetary.mean()
                m_std = df.monetary.std()
                norm_m = (m - m_mean) / m_std

                f_mean = df.frequency.mean()
                f_std = df.frequency.std()
                norm_f = (f - f_mean) / f_std

                r_mean = df.recency.mean()
                r_std = df.recency.std()
                norm_r = (r - r_mean) / r_std

                return pd.Series([c, sender, r, norm_r, f, norm_f, m, norm_m])

            else:
                return pd.Series([c, sender, r, 0, f, 0, m, 0])

            normalized_params = parameters_df.reset_index().apply(lambda x: normalize(x), axis=1).fillna(0)
            normalized_params.columns = ["nft_contract_id", "tx_sender", "recency", "recency_norm", "frequency",
                                         "frequency_norm", "monetary", "monetary_norm"]
            normalized_params = normalized_params.apply(lambda x: pd.Series(
                [x["nft_contract_id"], x["tx_sender"], x["recency_norm"] * x["frequency_norm"] * x["monetary_norm"]]),
                axis=1)
            normalized_params.columns = ["nft_contract_id", "tx_sender", "rfm"]

            quantiles = normalized_params[["nft_contract_id", "rfm"]]
            quantiles = quantiles.groupby("nft_contract_id").quantile(0.95, interpolation="higher")

            result = normalized_params.set_index("nft_contract_id").join(quantiles, rsuffix="_qunatile").reset_index()
            result = result[result["nft_contract_id"] == nft_contract_id]
            result = result[(result["rfm"] >= result["rfm_qunatile"])]


    else:
        print("no smart contract provided")


