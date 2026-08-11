const amqp = require('amqplib');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER] });
const producer = kafka.producer();

async function connectRabbitMQ() {
  let retries = 10;
  while (retries > 0) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL);
      const channel = await conn.createChannel();
      await channel.assertQueue('transfer_commands', {
        durable: true,
        arguments: { 'x-dead-letter-exchange': 'dlx.transfer_commands' },
      });
      channel.prefetch(1);
      console.log('✅ Conectado a RabbitMQ, esperando comandos...');
      return channel;
    } catch (err) {
      console.log(`⏳ Esperando RabbitMQ... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw new Error('No se pudo conectar a RabbitMQ');
}

async function connectKafka() {
  let retries = 10;
  while (retries > 0) {
    try {
      await producer.connect();
      console.log('✅ Conectado a Kafka');
      return;
    } catch (err) {
      console.log(`⏳ Esperando Kafka... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw new Error('No se pudo conectar a Kafka');
}

async function main() {
  await connectKafka();
  const channel = await connectRabbitMQ();

  channel.consume('transfer_commands', async (msg) => {
    if (!msg) return;

    try {
      const comando = JSON.parse(msg.content.toString());
      console.log(`⚙️  Procesando transaccion: ${comando.tx_id}`);

      // Simular validacion: 70% exito, 30% fallo
      const status = Math.random() < 0.7 ? 'COMPLETED' : 'FAILED';

      const evento = {
        tx_id: comando.tx_id,
        from_user: comando.from_user,
        to_user: comando.to_user,
        amount: comando.amount,
        status,
        processed_at: new Date().toISOString()
      };

      await producer.send({
        topic: 'transactions_log',
        messages: [{
          key: comando.tx_id,
          value: JSON.stringify(evento)
        }]
      });

      console.log(`📒 Evento publicado en Kafka: ${comando.tx_id} -> ${status}`);
      channel.ack(msg);
    } catch (err) {
      console.error('❌ Error procesando comando:', err.message);
      channel.nack(msg, false, false);
    }
  });
}

main();
