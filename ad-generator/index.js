const amqp = require('amqplib');

let channel;

const ADS = [
  { subject: '¡Obtén tu Tarjeta de Crédito Platinum!', body: 'Sin cuota de manejo el primer año. Aplica hoy.' },
  { subject: 'Inversiones que generan rendimientos reales', body: 'Tu dinero trabajando 24/7. Conoce LedgerInvest.' },
  { subject: '¡Transfiere gratis este fin de semana!', body: 'Sin comision en transferencias del sabado al domingo.' }
];
let adIndex = 0;

async function connectRabbitMQ() {
  let retries = 10;
  while (retries > 0) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL);
      channel = await conn.createChannel();
      await channel.assertQueue('email_queue', { durable: true });
      console.log('✅ Ad generator conectado a RabbitMQ');
      return;
    } catch (err) {
      console.log(`⏳ Esperando RabbitMQ... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

async function main() {
  await connectRabbitMQ();

  setInterval(() => {
    const ad = ADS[adIndex % ADS.length];
    adIndex++;
    const emailTask = { to: 'all_users@ledger.com', ...ad, type: 'advertisement' };

    channel.sendToQueue('email_queue', Buffer.from(JSON.stringify(emailTask)), { persistent: true });

    console.log(`📢 Anuncio enviado: ${ad.subject}`);
  }, 30000);

  console.log('🎯 Ad generator corriendo, enviando anuncios cada 30 segundos...');
}

main();
