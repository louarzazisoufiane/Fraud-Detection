import argparse 
import json 
import math 
import random
import signal
import sys
import time
import uuid


from confluent_kafka import Producer

from Fraud_Detect import config as C

CITIES = {
    "Casablanca":(33.57,-7.59), "London":(51.51,-0.13),
    "New York":(40.71,-74.00), "Tokyo":(35.68,139.69),
    "Sydney":(-33.87,151.21), "Lagos":(6.52,3.38),
    "Sao Paulo":(-23.55,-46.63), "Dubai":(25.20,55.27),
}

MERCHANTS = ["Grocer","cafe","fuel","pharmacy","electronics","airline","restaurant","bookshop","transit","streaming"]


class Account:
    def __init__(self, i , rng):
        self.id = f"acct-{i:04d}"
        self.home = rng.choice(list(CITIES))
        self.baseline = rng.uniform(15, 120)

def make_txn(acct, rng , ts , amount=None , city=None):
    lat , lon = CITIES[city or acct.home]
    if amount is None :
        amount = max(1.0 , rng.gauss(acct.baseline , acct.baseline * 0.25))
    return {
        "schema_version": 1,
        "txn_id": str(uuid.uuid4()),
        "account_id": acct.id,
        "amount": round(amount,2),
        "currency":"USD",
        "merchant": rng.choice(MERCHANTS),
        "lat": lat + rng.gauss(0, 0.05),
        "lon": lon + rng.gauss(0, 0.05),
        "ts":ts,

    }



def card_testing_burst(acct, rng , now):
    return [make_txn(acct, rng , now + i * 0.4, amount=rng.uniform(1,5)) for i in range(8)]


def impossible_travel(acct, rng, now):
    far = rng.choice([c for c in CITIES if c!=acct.home])
    return [make_txn(acct, rng,now), make_txn(acct, rng , now + 2 , city=far)]

def big_ticket(acct, rng, now):
    return [make_txn(acct, rng , now , amount= acct.baseline * rng.uniform(15,40))]

SCENARIOS    = [card_testing_burst, impossible_travel, big_ticket]


class Sender :
    def __init__(self):
        self.producer = Producer(C.base_conf(**{
            "enable.idempotence":True,
            "linger.ms":20 ,
            "batch.size":64 * 1024,
            "compression.type":"lz4",
        }))
        self.sent = 0 
        self.failed = 0

    def _on_delivery(self, err , msg):
        if err : 
            self.failed += 1
            print(f"Delivery failed ,{err}")

    def send(self, key , value):

        self.producer.produce(C.T_RAW, key = key , value = value, on_delivery= self._on_delivery)
        self.producer.poll(0)
        self.sent += 1

    def send_txn(self, txn):
        self.send(txn["account_id"], json.dumps(txn))



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float , default=10)
    ap.add_argument("--accounts", type = int , default=500)
    ap.add_argument("--fraud-rate", type = float , default=0.01)
    ap.add_argument("--poison-rate", type=float, default=0.002)
    ap.add_argument("--no-warmup", action="store_true")
    ap.add_argument("--seed", type= int )
    ap.add_argument("--duration", type=float , help="stop after N seconds")
    ap.add_argument("--max-txns", type=int , help="stop after N produced messages")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    accounts = [Account(i, rng) for i in range(args.accounts)]
    out = Sender()

    if not args.no_warmup:
        now = time.time()
        for i in range(12):
            for a in accounts:
                out.send_txn(make_txn(a,rng, now - (12 - i)*600 + rng.random()))
        out.producer.flush()
        print(f"warmup done {out.sent}")

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    start = time.time()
    try :
        while True:
            if args.duration is not None and time.time() - start >= args.duration:
                break
            if args.max_txns is not None and out.sent >= args.max_txns:
                break

            acct = rng.choice(accounts)
            roll = rng.random()
            if roll < args.poison_rate :
                out.send(acct.id, rng.choice([b"{not json ", b'{"amount":-5}']))

            elif roll < args.poison_rate + args.fraud_rate:
                for t in rng.choice(SCENARIOS)(acct, rng, time.time()):
                    out.send_txn(t)

            else:
                out.send_txn(make_txn(acct, rng , time.time()))

            if out.sent % 100 == 0 :
                print(f"sent {out.sent}  failed {out.failed}")

            time.sleep(rng.expovariate(args.rate))

    except KeyboardInterrupt:
        pass
    finally:
        out.producer.flush(10)
        print(f"done : sent {out.sent}  failed {out.failed}")

if __name__ == "__main__":
    main()