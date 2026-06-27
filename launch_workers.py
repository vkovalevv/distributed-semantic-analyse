import sys, subprocess

N = int(sys.argv[1])                      # python launch_workers.py 3
procs = [subprocess.Popen([sys.executable, 'worker.py']) for _ in range(N)]
try:
    for p in procs:
        p.wait()
except KeyboardInterrupt:
    pass
finally:
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait()          # дождаться, пока реально умрут