const express = require('express');
const amqp = require('amqplib');
const rateLimit = require('express-rate-limit');
const cors = require('cors');
const { randomUUID } = require('crypto');

const app = express();
app.use(express.json());
app.use(cors());

app.use(rateLimit({ windowMs: 1000, max: 10, message: 'Demasiadas solicitudes' }));

let channel;

// Conectarse a RabbitMQ con reintentos
async function connectRabbitMQ() {
  let retries = 10;
  while (retries > 0) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue('transfer_commands', {
        durable: true,
        arguments: { 'x-dead-letter-exchange': 'dlx.transfer_commands' },
      });
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

  if (!from_user || !to_user || !amount || amount <= 0) {
    return res.status(400).json({ error: 'Campos requeridos: from_user, to_user, amount (> 0)' });
  }

  const tx_id = randomUUID();
  const comando = { tx_id, from_user, to_user, amount, issued_at: new Date().toISOString() };

  channel.sendToQueue('transfer_commands', Buffer.from(JSON.stringify(comando)), {
    persistent: true,
    contentType: 'application/json',
  });

  console.log(`📤 Comando enviado a RabbitMQ: ${tx_id}`);
  res.status(202).json({ tx_id, message: 'Transaccion en proceso', status: 'PENDING' });
});

app.get('/health', (req, res) => res.json({ status: 'ok', service: 'transaction-api' }));

async function main() {
  await connectRabbitMQ();
  app.listen(3000, () => console.log('🚀 transaction-api corriendo en puerto 3000'));
}

main();
