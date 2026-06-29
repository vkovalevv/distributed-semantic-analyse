import time 
from sentence_transformers import SentenceTransformer
from datasets import load_dataset

model = SentenceTransformer('all-MiniLM-L6-v2', device='cuda')
texts = load_dataset("fancyzhx/ag_news", split="train")['text'][:5000]

for B in [8,16,32,64,128,256]:
    model.encode(texts[:B], batch_size=B)
    t0 = time.perf_counter()
    model.encode(texts, batch_size=B)
    t1 = time.perf_counter()
    print(f'batch={B}: {len(texts)/(t1-t0):.1f} docs/s')
