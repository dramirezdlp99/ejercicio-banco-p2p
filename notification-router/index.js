const amqp = require('amqplib');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: 'notification-router-group' });

let channel;

async function connectRabbitMQ() {
  let retries = 10;
  while (retries > 0) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue('email_queue', { durable: true });
      console.log('✅ Notification router conectado a RabbitMQ');
      return;
    } catch (err) {
      console.log(`⏳ Esperando RabbitMQ... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

async function connectKafka() {
  let retries = 10;
  while (retries > 0) {
    try {
      await consumer.connect();
      console.log('✅ Notification router conectado a Kafka');
      return;
    } catch (err) {
      console.log(`⏳ Esperando Kafka... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

async function main() {
  await connectRabbitMQ();
  await connectKafka();

  await consumer.subscribe({ topic: 'transactions_log', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const evento = JSON.parse(message.value.toString());

      if (evento.status === 'COMPLETED') {
        const emailTask = {
          to: `${evento.from_user}@banco.com`,
          body: `Tu transferencia de $${evento.amount} a ${evento.to_user} fue exitosa. ID: ${evento.tx_id}`
        };

        channel.sendToQueue(
          'email_queue',
          Buffer.from(JSON.stringify(emailTask)),
          { persistent: true }
        );

        console.log(`📧 Tarea de email enviada para: ${evento.tx_id}`);
      }
    }
  });
}

main();
