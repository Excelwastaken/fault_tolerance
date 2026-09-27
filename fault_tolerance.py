import copy
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

TRANSACTIONS = [
    ("T001", 12000, "None"),
    ("T002", 25000, "Network"),
    ("T003", 8000, "None"),
    ("T004", 45000, "Timeout"),
    ("T005", 13000, "None"),
    ("T006", 70000, "Database"),
    ("T007", 9000, "None"),
    ("T008", 31000, "Network"),
    ("T009", 15000, "None"),
    ("T010", 50000, "Timeout"),
    ("T011", 6000, "None"),
    ("T012", 80000, "Database"),
    ("T013", 11000, "None"),
    ("T014", 22000, "None"),
    ("T015", 40000, "Network"),
]

FAULT_RULES = {
    "None":     ["Success"],
    "Network":  ["Fail", "Success"],
    "Timeout":  ["Fail", "Fail", "Success"],
    "Database": ["Fail", "Fail", "Fail"],  # -> Rollback
}

EXCEPTION_FOR_FAILURE = {
    "Network": "ConnectionError",
    "Timeout": "TimeoutError",
    "Database": "DatabaseError",
}


class DatabaseError(Exception):
    pass


@dataclass
class Transaction:
    tx_id: str
    amount: int
    failure_type: str


def load_transactions():
    return [Transaction(t, a, f) for t, a, f in TRANSACTIONS]


class FakeClock:
    def __init__(self):
        self.t = datetime(2026, 9, 27, 10, 0, 0)

    def now(self):
        return self.t

    def tick(self, seconds=1):
        self.t += timedelta(seconds=seconds)
        return self.t

def run_attempt(tx: Transaction, attempt_index: int):
    """Simulate one Validate->Process->Record attempt for a transaction.
    Raises the appropriate exception if this attempt is a scripted failure.
    """
    outcomes = FAULT_RULES[tx.failure_type]
    outcome = outcomes[attempt_index] if attempt_index < len(outcomes) else outcomes[-1]
    if outcome == "Fail":
        exc_name = EXCEPTION_FOR_FAILURE[tx.failure_type]
        if exc_name == "ConnectionError":
            raise ConnectionError(f"{tx.tx_id}: network error contacting payment gateway")
        elif exc_name == "TimeoutError":
            raise TimeoutError(f"{tx.tx_id}: payment gateway timed out")
        elif exc_name == "DatabaseError":
            raise DatabaseError(f"{tx.tx_id}: database write failed")
    return True  # Validate->Process->Record all succeeded this attempt


def part_a_baseline(transactions):
    attempted = []
    successful = []
    processed_amount = 0
    stopped_at = None

    for tx in transactions:
        attempted.append(tx.tx_id)
        # baseline only ever makes ONE (the initial) attempt, no try/except
        outcome = FAULT_RULES[tx.failure_type][0]
        if outcome == "Fail":
            stopped_at = tx.tx_id
            break  # simulates uncaught exception terminating the application
        successful.append(tx.tx_id)
        processed_amount += tx.amount

    lost = [tx.tx_id for tx in transactions if tx.tx_id not in successful]
    lost_amount = sum(tx.amount for tx in transactions if tx.tx_id in lost)

    return {
        "attempted": attempted,
        "successful": successful,
        "lost": lost,
        "processed_amount": processed_amount,
        "lost_amount": lost_amount,
        "stopped_at": stopped_at,
    }


def part_b_exception_handling(transactions, logger):
    results = []
    for tx in transactions:
        try:
            run_attempt(tx, attempt_index=0)
        except (ConnectionError, TimeoutError, DatabaseError) as exc:
            exc_type = type(exc).__name__
            recovery_action = "Logged failure, transaction skipped, processing continued"
            logger.warning("Transaction %s failed: %s: %s", tx.tx_id, exc_type, exc)
            results.append({
                "tx_id": tx.tx_id,
                "failure_type": tx.failure_type,
                "exception": exc_type,
                "recovery_action": recovery_action,
                "final_status": "Failed",
            })
        else:
            logger.info("Transaction %s processed successfully", tx.tx_id)
            results.append({
                "tx_id": tx.tx_id,
                "failure_type": tx.failure_type,
                "exception": None,
                "recovery_action": "None needed",
                "final_status": "Success",
            })
    return results


def part_c_retry(transactions, logger, clock):
    attempt_log = []  # rows for the required attempt log table
    final_results = {}

    for tx in transactions:
        outcomes = FAULT_RULES[tx.failure_type]
        final_status = None
        for attempt_index, outcome in enumerate(outcomes):
            ts = clock.tick(2)
            step_label = "Initial attempt" if attempt_index == 0 else f"Retry {attempt_index}"
            if outcome == "Success":
                action = "Commit"
                result = "Success"
                logger.info("%s | %s | attempt=%s | %s -> %s",
                            tx.tx_id, tx.failure_type, step_label, action, result)
                final_status = "Success"
            else:
                exc_name = EXCEPTION_FOR_FAILURE.get(tx.failure_type, "Error")
                is_last_attempt = attempt_index == len(outcomes) - 1
                action = "Rollback" if is_last_attempt else "Retry scheduled"
                result = "Fail"
                logger.warning("%s | %s | attempt=%s | exception=%s -> %s",
                               tx.tx_id, tx.failure_type, step_label, exc_name, action)
                if is_last_attempt:
                    final_status = "Rollback"

            attempt_log.append({
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "tx_id": tx.tx_id,
                "attempt": attempt_index + 1,
                "failure_type": tx.failure_type if outcome == "Fail" else "-",
                "action": action,
                "result": result,
            })
        final_results[tx.tx_id] = final_status

    return attempt_log, final_results


@dataclass
class EngineState:
    committed_ids: list = field(default_factory=list)
    total_amount: int = 0


def part_d_checkpoint_rollback(transactions, logger, clock):
    state = EngineState()
    checkpoints = []  # list of (label, deep-copied state)
    rollback_events = []
    retry_events = 0
    per_tx_final = {}

    def make_checkpoint(label):
        snapshot = copy.deepcopy(state)
        checkpoints.append((label, snapshot))
        logger.info("CHECKPOINT %s saved: %d successful tx, total=%s KZT",
                    label, len(snapshot.committed_ids), snapshot.total_amount)

    for tx in transactions:
        outcomes = FAULT_RULES[tx.failure_type]
        outcome_final = None
        for attempt_index, outcome in enumerate(outcomes):
            clock.tick(2)
            if outcome == "Fail":
                is_last_attempt = attempt_index == len(outcomes) - 1
                if is_last_attempt:
                    latest_label, latest_state = checkpoints[-1] if checkpoints else ("CP0(initial)", EngineState())
                    logger.error("%s: all attempts failed -> ROLLBACK to %s (state restored: %d tx, %s KZT)",
                                 tx.tx_id, latest_label, len(latest_state.committed_ids), latest_state.total_amount)
                    state.committed_ids = list(latest_state.committed_ids)
                    state.total_amount = latest_state.total_amount
                    rollback_events.append({
                        "tx_id": tx.tx_id,
                        "restored_to": latest_label,
                        "restored_count": len(latest_state.committed_ids),
                        "restored_amount": latest_state.total_amount,
                    })
                    outcome_final = "Rollback"
                else:
                    retry_events += 1
                    logger.warning("%s: attempt %d failed, retrying...", tx.tx_id, attempt_index + 1)
            else:
                state.committed_ids.append(tx.tx_id)
                state.total_amount += tx.amount
                outcome_final = "Success"
                logger.info("%s: committed (total now %d tx, %s KZT)",
                            tx.tx_id, len(state.committed_ids), state.total_amount)
                if len(state.committed_ids) % 5 == 0:
                    make_checkpoint(f"CP{len(checkpoints) + 1}")

        per_tx_final[tx.tx_id] = outcome_final

 
    if not checkpoints or checkpoints[-1][1].committed_ids != state.committed_ids:
        make_checkpoint(f"CP{len(checkpoints) + 1} (final)")

    return {
        "final_state": state,
        "checkpoints": checkpoints,
        "rollback_events": rollback_events,
        "retry_events": retry_events,
        "per_tx_final": per_tx_final,
    }



def make_logger(name, filepath):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fh = logging.FileHandler(filepath, mode="w", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(fh)
    logger.propagate = False
    return logger


def main():
    transactions = load_transactions()

    a_result = part_a_baseline(transactions)

    b_logger = make_logger("partB", "partB_exceptions.log")
    b_result = part_b_exception_handling(transactions, b_logger)

    c_logger = make_logger("partC", "partC_retry.log")
    c_clock = FakeClock()
    attempt_log, c_final = part_c_retry(transactions, c_logger, c_clock)

    d_logger = make_logger("partD", "partD_checkpoint_rollback.log")
    d_clock = FakeClock()
    d_result = part_d_checkpoint_rollback(transactions, d_logger, d_clock)

    ft_successful = [tid for tid, st in d_result["per_tx_final"].items() if st == "Success"]
    ft_failed = [tid for tid, st in d_result["per_tx_final"].items() if st == "Rollback"]
    ft_processed_amount = sum(t.amount for t in transactions if t.tx_id in ft_successful)
    ft_lost_amount = sum(t.amount for t in transactions if t.tx_id in ft_failed)

    total_amount = sum(t.amount for t in transactions)

    summary = {
        "part_a": a_result,
        "part_b": b_result,
        "part_c_final": c_final,
        "part_d": {
            "successful": ft_successful,
            "failed_rollback": ft_failed,
            "processed_amount": ft_processed_amount,
            "lost_amount": ft_lost_amount,
            "retry_events": d_result["retry_events"],
            "rollback_events": d_result["rollback_events"],
            "checkpoints": [
                {"label": lbl, "count": len(st.committed_ids), "amount": st.total_amount,
                 "state_ids": list(st.committed_ids)}
                for lbl, st in d_result["checkpoints"]
            ],
        },
        "total_amount": total_amount,
    }

    with open("attempt_log.json", "w", encoding="utf-8") as f:
        json.dump(attempt_log, f, indent=2)

    with open("summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("=" * 70)
    print("PART A - BASELINE (no fault tolerance)")
    print("=" * 70)
    print(f"Attempted:          {a_result['attempted']}")
    print(f"Successful:         {a_result['successful']}")
    print(f"Lost:               {a_result['lost']}")
    print(f"Processed amount:   {a_result['processed_amount']:,} KZT")
    print(f"Lost amount:        {a_result['lost_amount']:,} KZT")
    print(f"Stopped at:         {a_result['stopped_at']} (unhandled exception)")

    print()
    print("=" * 70)
    print("PART B - EXCEPTION HANDLING")
    print("=" * 70)
    for r in b_result:
        if r["exception"]:
            print(f"{r['tx_id']:5} | {r['failure_type']:8} | {r['exception']:16} | {r['recovery_action']:45} | {r['final_status']}")

    print()
    print("=" * 70)
    print("PART C - RETRY MECHANISM (attempt log)")
    print("=" * 70)
    for row in attempt_log:
        print(f"{row['timestamp']} | {row['tx_id']:5} | attempt {row['attempt']} | "
              f"{row['failure_type']:8} | {row['action']:16} | {row['result']}")
    print()
    print("Final results per transaction:", c_final)

    print()
    print("=" * 70)
    print("PART D - CHECKPOINT AND ROLLBACK")
    print("=" * 70)
    for lbl, st in d_result["checkpoints"]:
        print(f"{lbl}: {len(st.committed_ids)} successful tx, total={st.total_amount:,} KZT, "
              f"ids={st.committed_ids}")
    print("Rollback events:")
    for ev in d_result["rollback_events"]:
        print(f"  {ev['tx_id']} -> restored to {ev['restored_to']} "
              f"({ev['restored_count']} tx, {ev['restored_amount']:,} KZT)")
    print(f"Total retry events: {d_result['retry_events']}")

    print()
    print("=" * 70)
    print("PART E - BEFORE / AFTER")
    print("=" * 70)
    baseline_completion = len(a_result["successful"]) / len(transactions) * 100
    ft_completion = len(ft_successful) / len(transactions) * 100
    print(f"{'Metric':<28}{'Baseline':<15}{'Fault-tolerant':<18}")
    print(f"{'Successful transactions':<28}{len(a_result['successful']):<15}{len(ft_successful):<18}")
    print(f"{'Failed/lost transactions':<28}{len(a_result['lost']):<15}{len(ft_failed):<18}")
    print(f"{'Lost amount (KZT)':<28}{a_result['lost_amount']:<15,}{ft_lost_amount:<18,}")
    print(f"{'Retries':<28}{0:<15}{d_result['retry_events']:<18}")
    print(f"{'Rollbacks':<28}{0:<15}{len(d_result['rollback_events']):<18}")
    print(f"{'Completion rate':<28}{baseline_completion:<15.1f}{ft_completion:<18.1f}")


if __name__ == "__main__":
    main()
