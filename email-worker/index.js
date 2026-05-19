const amqp = require('amqplib');

async function connectRabbitMQ() {
  let retries = 10;
  while (retries > 0) {
    try {
      const conn = await amqp.connect(process.env.RABBITMQ_URL);
      const channel = await conn.createChannel();
      await channel.assertQueue('email_queue', { durable: true });
      channel.prefetch(1);
      console.log('✅ Email worker conectado, esperando emails...');

      channel.consume('email_queue', (msg) => {
        if (!msg) return;
        const tarea = JSON.parse(msg.content.toString());
        console.log(`📨 ENVIANDO EMAIL a [${tarea.to}]: ${tarea.body}`);
        setTimeout(() => {
          channel.ack(msg);
          console.log(`✅ Email enviado a [${tarea.to}]`);
        }, 500);
      });

      return;
    } catch (err) {
      console.log(`⏳ Esperando RabbitMQ... (${retries} intentos restantes)`);
      retries--;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

connectRabbitMQ();
