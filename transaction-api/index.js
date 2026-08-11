const express = require('express');
const amqp = require('amqplib');
const rateLimit = require('express-rate-limit');
const cors = require('cors');
const { randomUUID } = require('crypto');

const app = express();
app.use(express.json());
app.use(cors());

// Rate limiter configurable vía variables de entorno:
// - DISABLE_RATE_LIMIT=1 -> desactiva (útil en desarrollo)
// - RATE_LIMIT_WINDOW_MS (ms) y RATE_LIMIT_MAX (número de requests por ventana)
const limiter = rateLimit({
  windowMs: Number(process.env.RATE_LIMIT_WINDOW_MS) || 1000,
  max: Number(process.env.RATE_LIMIT_MAX) || 10,
  message: process.env.RATE_LIMIT_MESSAGE || 'Demasiadas solicitudes',
});

if (process.env.DISABLE_RATE_LIMIT === '1') {
  console.log('⚠️ Rate limiter desactivado (DISABLE_RATE_LIMIT=1)');
} else {
  app.use(limiter);
}

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
  if (typeof amount !== 'number' || amount <= 0) {
    return res.status(400).json({ error: 'amount debe ser un número positivo' });
  }
  if (!channel) {
    return res.status(503).json({ error: 'Servicio no disponible, intentar de nuevo' });
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
