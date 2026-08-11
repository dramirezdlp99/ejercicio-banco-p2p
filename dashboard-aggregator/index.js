const { Kafka } = require('kafkajs');
const { WebSocketServer } = require('ws');

const KAFKA_BROKERS = [process.env.KAFKA_BROKER];
const RABBITMQ_HTTP_API = process.env.RABBITMQ_HTTP_API;
const RABBITMQ_USER = process.env.RABBITMQ_USER;
const RABBITMQ_PASS = process.env.RABBITMQ_PASS;

const kafka = new Kafka({ brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: 'dashboard-aggregator-group' });

const wss = new WebSocketServer({ port: 8081 });
const transacciones = new Map();

function broadcast(type, data) {
  const msg = JSON.stringify({ type, data, timestamp: new Date().toISOString() });
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(msg);
  });
}

async function fetchQueueStats() {
  if (!RABBITMQ_HTTP_API || !RABBITMQ_USER || !RABBITMQ_PASS) return;

  const headers = {
    Authorization: `Basic ${Buffer.from(`${RABBITMQ_USER}:${RABBITMQ_PASS}`).toString('base64')}`,
  };

  const endpoints = [
    `${RABBITMQ_HTTP_API}/api/queues/%2F/transfer_commands`,
    `${RABBITMQ_HTTP_API}/api/queues/%2F/email_queue`,
  ];

  try {
    const [transferRes, emailRes] = await Promise.all(
      endpoints.map((url) => fetch(url, { headers }))
    );

    if (!transferRes.ok || !emailRes.ok) return;

    const transferData = await transferRes.json();
    const emailData = await emailRes.json();

    broadcast('QUEUE_STATS', {
      transfer_commands: {
        messages: transferData.messages ?? 0,
        ready: transferData.messages_ready ?? 0,
        unacked: transferData.messages_unacknowledged ?? 0,
      },
      email_queue: {
        messages: emailData.messages ?? 0,
        ready: emailData.messages_ready ?? 0,
        unacked: emailData.messages_unacknowledged ?? 0,
      },
    });
  } catch (err) {
    console.warn('Queue stats error:', err.message);
  }
}

async function main() {
  let retries = 10;
  while (retries > 0) {
    try {
      await consumer.connect();
      console.log('✅ Dashboard conectado a Kafka');
      break;
    } catch (err) {
      console.log(`⏳ Esperando Kafka... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  await consumer.subscribe({ topics: ['transactions_log', 'fraud_alerts'], fromBeginning: false });

  consumer.run({
    eachMessage: async ({ topic, message }) => {
      const payload = JSON.parse(message.value.toString());

      if (topic === 'transactions_log') {
        const txState = {
          tx_id: payload.tx_id,
          from_user: payload.from_user,
          to_user: payload.to_user,
          amount: payload.amount,
          status: payload.status,
          processed_at: payload.processed_at,
          is_suspicious: false,
          fraud_reason: null
        };

        transacciones.set(payload.tx_id, txState);
        broadcast('TX_UPDATE', txState);
        console.log(`📊 Dashboard actualizado: ${payload.tx_id}`);
      }

      if (topic === 'fraud_alerts') {
        const tx = transacciones.get(payload.tx_id);
        if (tx) {
          tx.is_suspicious = true;
          tx.fraud_reason = payload.reason;
          broadcast('FRAUD_ALERT', tx);
          console.log(`🚨 Transaccion marcada como fraude: ${payload.tx_id}`);
        }
      }
    }
  });

  wss.on('connection', (ws) => {
    console.log('👤 Cliente conectado al dashboard');
    const todas = Array.from(transacciones.values());
    ws.send(JSON.stringify({ type: 'SNAPSHOT', data: todas }));
  });

  console.log('🖥️  Dashboard WebSocket corriendo en puerto 8081');

  if (RABBITMQ_HTTP_API && RABBITMQ_USER && RABBITMQ_PASS) {
    setInterval(fetchQueueStats, 5000);
  }
}

main();
