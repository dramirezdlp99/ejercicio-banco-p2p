# Ledger Platform (AWS Event Ledger)

Repositorio de referencia para la plataforma de transacciones bancarias basada en CQRS/Event Sourcing.

## Requisitos
- Node.js 20 LTS
- Docker y Docker Compose v2
- Acceso a AWS (EC2 con Kafka). RabbitMQ se ejecuta en Docker en el nodo `ec2-services`.

## Variables de entorno
Copiar y completar:

```bash
cp .env.example .env
```

Variables:
- `RABBITMQ_URL`: endpoint AMQP del RabbitMQ (ej: `amqp://admin:pass@10.0.1.X:5672`)
- `RABBITMQ_HTTP_API`: endpoint HTTP del management (ej: `http://10.0.1.X:15672`)
- `RABBITMQ_USER` / `RABBITMQ_PASS`: credenciales del management/API
- `KAFKA_BROKER`: IP privada del nodo Kafka (EC2)

## Levantar servicios (EC2 services node)

```bash
docker compose up -d --build
```

Dashboard:
- HTTP: `http://<SERVICES-EC2-IP>/`
- WebSocket: `ws://<SERVICES-EC2-IP>:8081`

## Pruebas rapidas

### Transaccion normal
```bash
curl -X POST http://<SERVICES-EC2-IP>:3000/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_user": "alice", "to_user": "bob", "amount": 50}'
```

### Transaccion de alto valor
```bash
curl -X POST http://<SERVICES-EC2-IP>:3000/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_user": "carlos", "to_user": "diana", "amount": 25000}'
```

### Fraude por velocidad
```bash
for i in {1..5}; do
  curl -s -X POST http://<SERVICES-EC2-IP>:3000/transfer \
    -H "Content-Type: application/json" \
    -d '{"from_user": "eve", "to_user": "frank", "amount": 100}' \
    > /dev/null
  sleep 5
done
```

### Health check
```bash
curl http://<SERVICES-EC2-IP>:3000/health
```
