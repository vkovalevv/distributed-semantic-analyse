import json
import numpy as np
from sentence_transformers import SentenceTransformer, util

vectors = np.load("vectors.npy")
meta = json.load(open("meta.json", encoding="utf-8"))
model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")


def search(query: str, k: int = 5):
    q = model.encode([query])
    sims = util.cos_sim(q, vectors)[0]
    top = sims.argsort(descending=True)[:k]
    return [(float(sims[i]), meta[i]["text"]) for i in top]

for score, text in search('football match results'):
    print(round(score,3), text[:100])
