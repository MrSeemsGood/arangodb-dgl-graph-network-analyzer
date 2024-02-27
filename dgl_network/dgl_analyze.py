'''
Импорт коллекций ArangoDB в DGLGraph и анализ с помощью графовой нейронной сети.
'''


import torch
import json

from model import train_model

from arango import ArangoClient
from arango_upload import arango_connect

from adbdgl_adapter import ADBDGL_Adapter
from adbdgl_adapter.encoders import IdentityEncoder, CategoricalEncoder


def upload_labels_json(labels):
    labels_list = torch.flatten(
        labels
    ).tolist()

    labels_dict_list = [{"tx_id" : i, "is_alerted" : label} for i, label in enumerate(labels_list)]

    with open('dgl_network/output/trained_labels.json', 'w+') as file:
        json.dump(labels_dict_list, file)


adb_client = ArangoClient()
adb = arango_connect()
adbdgl_adapter = ADBDGL_Adapter(adb)

# Описание метаграфа - структуры графа для корректного импорта коллекций
tx_metagraph = {
    "vertexCollections" : {
        "account" : {
            "features" : {
                "account_type" : CategoricalEncoder(mapping={
                    "DEBIT" : 0,
                    "CREDIT" : 1
                }),
                "account_creation_dttm" : IdentityEncoder(dtype=torch.int)
            }
        }
    },
    "edgeCollections" : {
        "transaction" : {
            "features" : {
                "tx_type" : CategoricalEncoder(),
                "tx_amount" : IdentityEncoder(dtype=torch.int),
                "timestamp" : IdentityEncoder(dtype=torch.int)
            },
            "label" : "is_alerted"
        }
    }
}

dgl_tx = adbdgl_adapter.arangodb_to_dgl(
    "transactions_graph",
    metagraph=tx_metagraph
)


# Добавить маску (train / test)
dgl_tx.edata['train_mask'] = torch.zeros(
    len(dgl_tx.edata['label']), dtype=torch.bool
).bernoulli(0.7)

dgl_tx.edata['label'] = torch.unsqueeze(dgl_tx.edata['label'], 1)


new_labels = train_model(dgl_tx)

upload_labels_json(new_labels)
