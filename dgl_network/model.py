'''
Модель графовой нейронной сети для анализа транзакций.

Источник: https://docs.dgl.ai/en/1.1.x/guide/training-edge.html
(с модификациями)
'''


import dgl
import dgl.nn as dglnn
import dgl.function as fn

import torch
import torch.nn as nn


NUM_HIDDEN_FEATURES = 20    # Размер скрытого слоя
LOSS_TOLERANCE = 0.0001     # Минимальная разница в потере (если потери изменяются слабее, обучение прекращается)
MAX_NUM_STEMPS = 500        # Максимальное число итераций


class SAGE(nn.Module):
    def __init__(self, in_feats, hid_feats, out_feats):
        super().__init__()
        self.conv1 = dglnn.SAGEConv(
            in_feats=in_feats, out_feats=hid_feats, aggregator_type='mean')
        self.conv2 = dglnn.SAGEConv(
            in_feats=hid_feats, out_feats=out_feats, aggregator_type='mean')

    def forward(self, graph, inputs):
        h = self.conv1(graph, inputs)
        h = self.conv2(graph, h)
        return h
    

class DotProductPredictor(nn.Module):
    def forward(self, graph, nf):
        with graph.local_scope():
            graph.ndata['features'] = nf
            graph.apply_edges(fn.u_dot_e('features', 'features', 'label'))

            max_val = torch.max(graph.edata['label'])
            min_val = torch.min(graph.edata['label'])

            graph.edata['label'] = (graph.edata['label'] - min_val) / (max_val - min_val)
            return graph.edata['label']
        

class Model(nn.Module):
    def __init__(self, in_features, hidden_features, out_features):
        super().__init__()
        self.sage = SAGE(in_features, hidden_features, out_features)
        self.pred = DotProductPredictor()
    def forward(self, g, x):
        h = self.sage(g, x)
        return self.pred(g, h)
    

def train_model(graph:dgl.DGLGraph):
    node_features = graph.ndata['features']
    edge_features = graph.edata['features']
    edge_label = graph.edata['label']
    train_mask = graph.edata['train_mask']

    in_feats = len(node_features[0])
    out_feats = len(edge_features[0])

    model = Model(
        in_features=in_feats,
        hidden_features=NUM_HIDDEN_FEATURES,
        out_features=out_feats
    )

    opt = torch.optim.Adam(model.parameters())

    steps = 0
    old_loss = 1

    while True:
        steps += 1

        pred = model(graph, node_features)
        loss = ((pred[train_mask] - edge_label[train_mask]) ** 2).mean()
        opt.zero_grad()
        loss.backward()
        opt.step()
        print('Шаг {}, потери = {:.4f}'.format(
            steps,
            loss.item()
        ))

        if steps > MAX_NUM_STEMPS or abs(loss.item() - old_loss) < LOSS_TOLERANCE:
            print('Я обучился достаточно! Возврат итоговых атрибутов для рёбер.')
            return graph.edata['label']
