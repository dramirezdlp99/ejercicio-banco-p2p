const express = require('express');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

let channel;

// Conectarse a RabbitMQ con reintentos
async function connectRabbitMQ() {
  let retries = 10;
  while (retries > 0) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue('transfer_commands', { durable: true });
      console.log('✅ Conectado a RabbitMQ');
      return;
    } catch (err) {
      console.log(`⏳ Esperando RabbitMQ... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
  throw new Error('No se pudo conectar a RabbitMQ');
}

// Ruta principal para recibir transferencias
app.post('/transfer', async (req, res) => {
  const { from_user, to_user, amount } = req.body;

  if (!from_user || !to_user || !amount) {
    return res.status(400).json({ error: 'Faltan campos: from_user, to_user, amount' });
  }
  if (typeof amount !== 'number' || amount <= 0) {
    return res.status(400).json({ error: 'amount debe ser un número positivo' });
  }
  if (!channel) {
    return res.status(503).json({ error: 'Servicio no disponible, intentar de nuevo' });
  }

  const tx_id = uuidv4();
  const comando = { tx_id, from_user, to_user, amount, timestamp: new Date().toISOString() };

  channel.sendToQueue(
    'transfer_commands',
    Buffer.from(JSON.stringify(comando)),
    { persistent: true }
  );

  console.log(`📤 Comando enviado a RabbitMQ: ${tx_id}`);
  res.json({ message: 'Transferencia recibida', tx_id });
});

app.get('/health', (req, res) => res.json({ status: 'ok' }));

async function main() {
  await connectRabbitMQ();
  app.listen(3000, () => console.log('🚀 transaction-api corriendo en puerto 3000'));
}

main();
