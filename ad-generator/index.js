const amqp = require('amqplib');

let channel;

const anuncios = [
  '¡Obtenga nuestra nueva Tarjeta de Crédito con 0% de interés!',
  '¡Abra una cuenta de ahorros y gane 5% de interés anual!',
  '¡Solicite su préstamo personal con las mejores tasas del mercado!',
  '¡Invierta en fondos mutuos y haga crecer su dinero!'
];

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
    const anuncio = anuncios[Math.floor(Math.random() * anuncios.length)];
    const emailTask = {
      to: 'all_users@banco.com',
      body: anuncio
    };

    channel.sendToQueue(
      'email_queue',
      Buffer.from(JSON.stringify(emailTask)),
      { persistent: true }
    );

    console.log(`📢 Anuncio enviado: ${anuncio}`);
  }, 30000);

  console.log('🎯 Ad generator corriendo, enviando anuncios cada 30 segundos...');
}

main();
