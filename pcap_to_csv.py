import os
import csv
import pyshark
from collections import defaultdict
from datetime import datetime
import asyncio

# Prevent "event loop is already running" error
try:
    asyncio.set_event_loop(asyncio.new_event_loop())
except Exception:
    pass

##############################################
# --------- Packet Parsing Helpers ----------
##############################################

def load_pcap(file_path):
    """Load packets using PyShark's FileCapture (lazy loading)."""
    cap =  pyshark.FileCapture(
        file_path,
        keep_packets=False,        # frees memory after use
        use_json=True,             # faster
        include_raw=False
    )
    print("Total packets read:", len(list(cap)))
    return cap


def get_timestamp(pkt):
    try:
        return float(pkt.sniff_timestamp)
    except:
        return None

def make_flow_key(pkt):
    try:
        src = (pkt.ip.src, pkt.tcp.srcport)
        dst = (pkt.ip.dst, pkt.tcp.dstport)

        # Normalize so direction doesn't matter
        return tuple(sorted([src, dst]))
    except AttributeError:
        return None

def packet_size(pkt):
    try:
        return int(pkt.length)
    except:
        return 0


##############################################
# --------- Flow Construction ---------------
##############################################

def construct_5tuple_flows(packets):
    flows = defaultdict(list)
    for pkt in packets:
        ts = get_timestamp(pkt)
        if ts is None:
            continue

        flow_key = make_flow_key(pkt)
        if flow_key is None:
            continue

        flows[flow_key].append(pkt)

    # Ensure flows are time-ordered
    for k in flows:
        flows[k].sort(key=lambda x: float(x.sniff_timestamp))

    return flows


def flow_duration(flow):
    if not flow:
        return 0
    return (get_timestamp(flow[-1]) - get_timestamp(flow[0])) / 60.0  # minutes


def split_into_windows(flow, window_minutes=30):
    if not flow:
        return []

    windows = []
    start_ts = get_timestamp(flow[0])
    current = []

    for pkt in flow:
        if get_timestamp(pkt) - start_ts <= window_minutes * 60:
            current.append(pkt)
        else:
            windows.append(current)
            current = [pkt]
            start_ts = get_timestamp(pkt)

    if current:
        windows.append(current)
    return windows


##############################################
# --------- Flow Filtering Logic ------------
##############################################

def is_encrypted_flow(flow):
    """Detect encryption by presence of TLS layer."""
    for pkt in flow:
        if 'TLS' in pkt:
            return True
    return False


def handshake_completed(flow):
    """Detect TCP SYN → SYN/ACK → ACK handshake."""
    syn = None
    syn_ack = None

    for pkt in flow:
        if "TCP" not in pkt:
            continue

        flags = pkt.tcp.flags

        # SYN
        if flags == "0x0002":
            syn = pkt
        # SYN-ACK
        elif flags == "0x0012":
            syn_ack = pkt
        # Final ACK completes handshake
        elif flags == "0x0010" and syn and syn_ack:
            return True

    return False


def is_encrypted_connection(flow):
    """Check if TLS appears after handshake."""
    return is_encrypted_flow(flow)


##############################################
# -------- Feature + Transition Matrix -------
##############################################

def extract_flow_features(flow):
    sizes = [packet_size(pkt) for pkt in flow]
    timestamps = [get_timestamp(pkt) for pkt in flow]

    duration = timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0

    return {
        "packet_count": len(flow),
        "min_size": min(sizes) if sizes else 0,
        "max_size": max(sizes) if sizes else 0,
        "avg_size": sum(sizes)/len(sizes) if sizes else 0,
        "duration_sec": duration,
        "pps": len(flow)/duration if duration > 0 else 0,
    }


def packet_state(pkt, inter_arrival):
    """State assignment using size thresholds (150 bytes/ms)."""
    size = packet_size(pkt)
    if size < 150:
        return 0
    elif size < 300:
        return 1
    return 2


def compute_transition_matrix(flow):
    M = [[0, 0, 0] for _ in range(3)]

    prev_state = None
    prev_ts = None

    for pkt in flow:
        ts = get_timestamp(pkt)
        if prev_ts is None:
            prev_ts = ts
            prev_state = 1
            continue

        inter = ts - prev_ts
        state = packet_state(pkt, inter)

        M[prev_state][state] += 1

        prev_state = state
        prev_ts = ts

    # Normalize rows
    for i in range(3):
        rsum = sum(M[i])
        if rsum > 0:
            M[i] = [x / rsum for x in M[i]]

    return M


##############################################
# -------- Main PCAP Processing -------------
##############################################

def process_pcap_file(path):
    cap = load_pcap(path)
    rows = []

    try:
        flows = construct_5tuple_flows(cap)

        for fid, flow in flows.items():
            dur = flow_duration(flow)

            if dur < 30:
                windows = split_into_windows(flow, 30)
                windows = [w for w in windows if not is_encrypted_flow(w)]
            else:
                if is_encrypted_flow(flow):
                    continue
                windows = [flow]

            for F in windows:
                if not handshake_completed(F):
                    continue

                if is_encrypted_connection(F):
                    continue

                feat = extract_flow_features(F)
                M = compute_transition_matrix(F)

                for i in range(3):
                    for j in range(3):
                        feat[f"M_{i}{j}"] = M[i][j]

                feat["flow_id"] = str(fid)
                feat["pcap"] = os.path.basename(path)
                rows.append(feat)

    finally:
        # Explicit close to stop PyShark from calling close_async inside __del__
        try:
            cap.close()
        except Exception as e:
            print(f"Warning: pyshark close() issue: {e}")

    return rows


##############################################
# -------- Dataset Runner (CSV) -------------
##############################################

def process_dataset(input_dir, output_csv="ctu13_pyshark.csv"):
    header_written = False

    with open(output_csv, "w", newline="") as f:
        writer = None

        for file in os.listdir(input_dir):
            if not file.lower().endswith(".pcap"):
                continue

            path = os.path.join(input_dir, file)
            print(f"Processing: {path}")

            rows = process_pcap_file(path)

            if rows:
                if not header_written:
                    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    header_written = True

                for r in rows:
                    writer.writerow(r)

    print(f"\nDone. CSV saved → {output_csv}")

process_dataset("data/ctu13", "data/ctu13/ctu13_processed_pyshark.csv")