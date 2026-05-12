const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER] });

const consumer = kafka.consumer({ groupId: 'fraud-detector-group' });
const producer = kafka.producer();

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

  await consumer.subscribe({ topic: 'transactions_log', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());
      console.log(`🔍 Analizando transacción: ${evento.tx_id} por $${evento.amount}`);

      // Lógica stateful: si amount > 10000 es sospechoso
      if (evento.amount > 10000) {
        const alerta = {
          tx_id: evento.tx_id,
          reason: 'HIGH_VALUE_TRANSACTION',
          amount: evento.amount,
          from_user: evento.from_user,
          timestamp: new Date().toISOString()
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
    }
  });
}

main();
