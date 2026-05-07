import pandas as pd
import json
import asyncio
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from dotenv import load_dotenv

load_dotenv()

# ========================= CONFIG =========================
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "raw-logs")
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "banking-events")


def clean_customer_id(cid):
    """Clean customer_id - no more UNKNOWN fallback if unified file has good data"""
    if pd.isna(cid) or str(cid).strip().lower() in ["none", "null", "", "nan"]:
        return f"CUST_GEN_{pd.Timestamp.now().strftime('%H%M%S')}"
    return str(cid).strip()


def read_unified_events(container):
    print("📥 Reading unified_events.csv from Blob...")
    try:
        blob_client = container.get_blob_client("unified_events.csv")
        download_stream = blob_client.download_blob()
        df = pd.read_csv(StringIO(download_stream.readall().decode('utf-8')))
        print(f"✅ Loaded {len(df)} unified events")
        return df
    except Exception as e:
        print(f"❌ Error reading unified_events.csv: {e}")
        return pd.DataFrame()


def create_events_from_unified():
    events = []
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container = blob_service.get_container_client(BLOB_CONTAINER)

    df = read_unified_events(container)

    if df.empty:
        print("Falling back to transactions.csv...")
        # Optional fallback
        df = pd.read_csv(StringIO(container.get_blob_client("transactions.csv").download_blob().readall().decode('utf-8')))

    for _, row in df.head(1500).iterrows():          # Adjust limit as needed
        event = {
            "event_id": str(row.get("event_id", f"evt_{int(pd.Timestamp.now().timestamp())}")),
            "event_type": str(row.get("event_type", "TRANSACTION")),
            "customer_id": clean_customer_id(row.get("customer_id")),
            "timestamp": str(row.get("timestamp")),
            "amount": float(row.get("amount", 0)),
            "transaction_type": str(row.get("transaction_type", "")),
            "channel": str(row.get("channel", "")),
            "device_id": str(row.get("device_id", "")),
            "ip_address": str(row.get("ip_address", "")),
            "status": str(row.get("status", "SUCCESS")),
            "merchant_category": str(row.get("merchant_category", "")),
            "risk_score": float(row.get("risk_score", 0.0)),
            "is_fraud": int(row.get("is_fraud", 0)),
            "source": "UNIFIED_EVENTS"
        }
        events.append(event)

    print(f"✅ Total events prepared from unified_events.csv: {len(events)}")
    return events


async def send_to_eventhub(events, batch_size=150):
    if not EVENT_HUB_CONNECTION_STRING:
        print("📝 Preview mode (first 2 events):")
        for e in events[:2]:
            print(json.dumps(e, indent=2))
        return

    async with EventHubProducerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME
    ) as producer:
        
        total_sent = 0
        for i in range(0, len(events), batch_size):
            batch_events = events[i:i + batch_size]
            try:
                batch = await producer.create_batch()
                for event in batch_events:
                    batch.add(EventData(json.dumps(event)))
                await producer.send_batch(batch)
                total_sent += len(batch_events)
                print(f"✅ Sent batch → {total_sent}/{len(events)} events")
            except Exception as e:
                print(f"⚠️ Batch error: {e}. Sending one by one...")
                for single in batch_events:
                    try:
                        b = await producer.create_batch()
                        b.add(EventData(json.dumps(single)))
                        await producer.send_batch(b)
                        total_sent += 1
                    except:
                        pass

    print(f"🎉 Successfully sent {total_sent} events to Event Hub!")


if __name__ == "__main__":
    print("🚀 Unified Events Producer Started")
    events = create_events_from_unified()
    asyncio.run(send_to_eventhub(events, batch_size=150))