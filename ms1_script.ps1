# start_milestone1_grid.ps1
# Requires Windows Terminal (wt.exe)

wt -d . powershell -NoExit -Command "python src/server_replica.py --replica-id S1 --host 0.0.0.0 --port 5000" `
   ';' split-pane --horizontal -d . powershell -NoExit -Command "python src/client.py --client-id C1 --server-host localhost --server-port 5000 --loop --delay 3.0" `
   ';' focus-pane -t 0 `
   ';' split-pane --vertical -d . powershell -NoExit -Command "python src/local_fault_detector.py --lfd-id LFD1 --server-host localhost --server-port 5000 --heartbeat-freq-ms 1000" `
   ';' focus-pane -t 1 `
   ';' split-pane --vertical -d . powershell -NoExit -Command "python src/client.py --client-id C2 --server-host localhost --server-port 5000 --loop --delay 3.0" `
   ';' split-pane --vertical -d . powershell -NoExit -Command "python src/client.py --client-id C3 --server-host localhost --server-port 5000"