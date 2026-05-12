const { Kafka } = require('kafkajs');
const { WebSocketServer } = require('ws');

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER] });
const consumerTx = kafka.consumer({ groupId: 'dashboard-tx-group' });
const consumerFraud = kafka.consumer({ groupId: 'dashboard-fraud-group' });

const wss = new WebSocketServer({ port: 8081 });
const transacciones = new Map();

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(msg);
  });
}

async function main() {
  let retries = 10;
  while (retries > 0) {
    try {
      await consumerTx.connect();
      await consumerFraud.connect();
      console.log('✅ Dashboard conectado a Kafka');
      break;
    } catch (err) {
      console.log(`⏳ Esperando Kafka... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  await consumerTx.subscribe({ topic: 'transactions_log', fromBeginning: true });
  await consumerFraud.subscribe({ topic: 'fraud_alerts', fromBeginning: true });

  consumerTx.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      transacciones.set(evento.tx_id, { ...evento, fraud: false });
      console.log(`📊 Dashboard actualizado: ${evento.tx_id}`);
      broadcast({ type: 'NEW_TRANSACTION', data: evento });
    }
  });

  consumerFraud.run({
    eachMessage: async ({ message }) => {
      const alerta = JSON.parse(message.value.toString());
      if (transacciones.has(alerta.tx_id)) {
        const tx = transacciones.get(alerta.tx_id);
        tx.fraud = true;
        transacciones.set(alerta.tx_id, tx);
        console.log(`🚨 Transacción marcada como fraude: ${alerta.tx_id}`);
        broadcast({ type: 'FRAUD_ALERT', data: alerta });
      }
    }
  });

  wss.on('connection', (ws) => {
    console.log('👤 Cliente conectado al dashboard');
    const todas = Array.from(transacciones.values());
    ws.send(JSON.stringify({ type: 'INIT', data: todas }));
  });

  console.log('🖥️  Dashboard WebSocket corriendo en puerto 8081');
}

main();
