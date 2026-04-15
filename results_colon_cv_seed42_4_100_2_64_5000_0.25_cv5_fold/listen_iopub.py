import json
from jupyter_client import BlockingKernelClient

CONN = "/root/.local/share/jupyter/runtime/kernel-e7a00f7c-6217-4da2-a1b7-ff89d79e474c.json"

kc = BlockingKernelClient(connection_file=CONN)
kc.load_connection_file()
kc.start_channels()

print("Listening IOPub...", flush=True)

try:
    while True:
        try:
            msg = kc.get_iopub_msg(timeout=1)
        except Exception:
            continue

        t = msg["header"]["msg_type"]
        c = msg["content"]

        if t == "stream":
            print(c.get("text", ""), end="", flush=True)

        elif t == "error":
            print("\n".join(c.get("traceback", [])), flush=True)

except KeyboardInterrupt:
    pass
finally:
    kc.stop_channels()
