import sys
import subprocess
import random
import time

N_VALUES = [1, 2, 3, 4, 5, 6, 8, 10, 12]
# рандомный порядок, дрейф не коррелируют с N
random.shuffle(N_VALUES)
COOLDOWN = 10

for N in N_VALUES:
    workers = [subprocess.Popen(
        [sys.executable, 'worker.py', str(i)]) for i in range(N)]
    try:
        # блокирует; warm_up ждёт воркеров
        subprocess.run([sys.executable, 'driver.py', str(N)])
    finally:
        for w in workers:
            w.terminate()
        for w in workers:
            w.wait()                  # дождаться смерти ДО следующего N
    time.sleep(COOLDOWN)
