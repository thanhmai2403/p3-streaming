"""python -m tests.test_detector  -> so detector.py với bản tham chiếu độc lập."""
from common import config
from flink_job.detector import Params, new_state, step, is_probe
from tests.make_synth import make
from tests import ref_independent

def run_detector(events, p):
    states, alerts, probes, dropped = {}, [], [], []
    for e in events:  # đúng thứ tự đến (seq)
        st = states.setdefault((e["station_id"], e["param"]), new_state())
        d, al = step(st, e["value"], e["event_time_ms"], p)
        if d: dropped.append(e["seq"]); continue
        alerts += [(e["seq"], det, sc) for det, sc in al]
        if is_probe(e["seq"], p): probes.append(e["seq"])
    return alerts, sorted(probes), sorted(dropped)

def compare(a, b):
    da = {(s, d): sc for s, d, sc in a}; db = {(s, d): sc for s, d, sc in b}
    assert set(da) == set(db), (set(da) ^ set(db))
    return max((abs(da[k] - db[k]) for k in da), default=0.0)

if __name__ == "__main__":
    p = Params.from_config(config.load())
    ev = make()
    a, pr, dr = run_detector(ev, p)
    b, pr2, dr2 = ref_independent.run(ev)
    diff = compare(a, b)
    assert pr == pr2 and dr == dr2
    zs = sum(1 for x in a if x[1] == "zscore"); ew = len(a) - zs
    print(f"events={len(ev)} alerts={len(a)} (zscore={zs}, ewma={ew}) probes={len(pr)} "
          f"late_dropped={len(dr)} max|score diff|={diff:.2e}")
    assert diff < 1e-6 and len(dr) > 0 and zs > 0 and ew > 0
    print("DETECTOR_OK")
