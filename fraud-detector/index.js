const { Kafka } = require('kafkajs');

const KAFKA_BROKERS = [process.env.KAFKA_BROKER];
const HIGH_VALUE_THRESHOLD = 10000;
const VELOCITY_WINDOW_MS = 60000;
const VELOCITY_MAX_TXS = 3;

const userTransactionHistory = new Map();

const kafka = new Kafka({ brokers: KAFKA_BROKERS });

const consumer = kafka.consumer({ groupId: 'fraud-detector-group' });
const producer = kafka.producer();

function checkVelocityFraud(fromUser) {
  const now = Date.now();
  const history = userTransactionHistory.get(fromUser) || [];
  const recentTxs = history.filter(ts => now - ts < VELOCITY_WINDOW_MS);
  recentTxs.push(now);
  userTransactionHistory.set(fromUser, recentTxs);
  return recentTxs.length > VELOCITY_MAX_TXS;
}

async function publishFraudAlert(evento, reason) {
  const alerta = {
    tx_id: evento.tx_id,
    reason,
    amount: evento.amount,
    from_user: evento.from_user,
    flagged_at: new Date().toISOString()
  };

  await producer.send({
    topic: 'fraud_alerts',
    messages: [{
      key: evento.tx_id,
      value: JSON.stringify(alerta)
    }]
  });

  console.log(`🚨 ALERTA DE FRAUDE publicada: ${evento.tx_id} por $${evento.amount}`);
}

async function main() {
  let retries = 10;
  while (retries > 0) {
    try {
      await consumer.connect();
      await producer.connect();
      console.log('✅ Fraud detector conectado a Kafka');
      break;
    } catch (err) {
      console.log(`⏳ Esperando Kafka... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  await consumer.subscribe({ topic: 'transactions_log', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      console.log(`🔍 Analizando transaccion: ${evento.tx_id} por $${evento.amount}`);

      if (evento.status !== 'COMPLETED') return;

      if (evento.amount > HIGH_VALUE_THRESHOLD) {
        await publishFraudAlert(evento, 'HIGH_VALUE_TRANSACTION');
        return;
      }

      if (checkVelocityFraud(evento.from_user)) {
        await publishFraudAlert(evento, 'VELOCITY_FRAUD');
      }
    }
  });
}

main();
